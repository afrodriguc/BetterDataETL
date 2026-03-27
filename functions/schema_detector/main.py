"""
Cloud Function: schema_detector

Se activa cuando se sube un archivo CSV a Bronze.
Detecta la estructura del archivo, la compara con el schema registrado
y publica un evento a Pub/Sub si hay cambios.

Trigger: google.cloud.storage.object.v1.finalized
"""

import io
import json
import functions_framework
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
from google.cloud import bigquery

from etl_carriers.config import PROJECT_ID
from etl_carriers.utils import extract_carrier_from_path, extract_metadata_from_filename
from etl_carriers.loaders import GCSLoader


class SchemaDetectorService:
    """Detecta y registra cambios de esquema en archivos de carriers."""

    def __init__(self, project_id: str = PROJECT_ID):
        self.project_id = project_id
        self.gcs = GCSLoader(project_id)
        self.bq  = bigquery.Client(project=project_id)
        self.registry_table = f"{project_id}.metadata.schema_registry"

    # ── Schema extraction ──────────────────────────────────────────────────

    def extract_schema(self, file_path: str) -> Dict[str, Any]:
        """Lee hasta 100 filas del archivo y extrae la estructura de columnas."""
        content = self.gcs.download_as_text(file_path)
        df = pd.read_csv(io.StringIO(content), nrows=100, low_memory=False)

        BQ_TYPE = {
            "int":      "INTEGER",
            "float":    "FLOAT",
            "datetime": "DATETIME",
            "bool":     "BOOLEAN",
        }

        columns = []
        for i, col in enumerate(df.columns):
            dtype_str = str(df[col].dtype)
            data_type = next((v for k, v in BQ_TYPE.items() if k in dtype_str), "STRING")
            columns.append({"column_name": col.strip(), "data_type": data_type, "column_position": i + 1})

        return {"columns": columns, "total_columns": len(columns)}

    # ── BigQuery registry ──────────────────────────────────────────────────

    def get_current_schema(self, carrier: str) -> Optional[Dict[str, Any]]:
        query = f"""
            SELECT schema_version, columns, total_columns
            FROM `{self.registry_table}`
            WHERE carrier = @carrier AND is_current = TRUE
            ORDER BY detected_at DESC LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("carrier", "STRING", carrier)]
        )
        try:
            for row in self.bq.query(query, job_config=job_config).result():
                cols = json.loads(row.columns) if isinstance(row.columns, str) else row.columns
                return {"schema_version": row.schema_version, "columns": cols, "total_columns": row.total_columns}
        except Exception as exc:
            print(f"[INFO] Sin schema previo para {carrier}: {exc}")
        return None

    def compare_schemas(self, old: Optional[Dict], new: Dict) -> List[Dict]:
        if not old:
            return []
        old_cols = {c["column_name"]: c for c in old["columns"]}
        new_cols = {c["column_name"]: c for c in new["columns"]}
        changes = []
        for col in set(new_cols) - set(old_cols):
            changes.append({"change_type": "ADDED",   "column_name": col, "old_value": None, "new_value": new_cols[col]["data_type"]})
        for col in set(old_cols) - set(new_cols):
            changes.append({"change_type": "REMOVED",  "column_name": col, "old_value": old_cols[col]["data_type"], "new_value": None})
        for col in set(old_cols) & set(new_cols):
            if old_cols[col]["data_type"] != new_cols[col]["data_type"]:
                changes.append({"change_type": "TYPE_CHANGED", "column_name": col,
                                 "old_value": old_cols[col]["data_type"], "new_value": new_cols[col]["data_type"]})
        return changes

    def register_schema(self, carrier: str, file_name: str, schema: Dict,
                        changes: List, old_version: Optional[int], aor_id: str) -> int:
        new_version = (old_version or 0) + 1

        if old_version:
            self.bq.query(
                f"UPDATE `{self.registry_table}` SET is_current=FALSE, updated_at=CURRENT_TIMESTAMP() WHERE carrier=@c AND is_current=TRUE",
                job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("c","STRING",carrier)])
            ).result()

        insert = f"""
            INSERT INTO `{self.registry_table}`
            (id,carrier,aor_id,file_name,schema_version,is_current,columns,changes,total_columns,detected_by)
            VALUES (GENERATE_UUID(),@carrier,@aor_id,@file_name,@version,TRUE,
                    PARSE_JSON(@columns),PARSE_JSON(@changes),@total_columns,'schema_detector')
        """
        self.bq.query(insert, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("carrier",      "STRING", carrier),
            bigquery.ScalarQueryParameter("aor_id",       "STRING", aor_id),
            bigquery.ScalarQueryParameter("file_name",    "STRING", file_name),
            bigquery.ScalarQueryParameter("version",      "INT64",  new_version),
            bigquery.ScalarQueryParameter("columns",      "STRING", json.dumps(schema["columns"])),
            bigquery.ScalarQueryParameter("changes",      "STRING", json.dumps(changes)),
            bigquery.ScalarQueryParameter("total_columns","INT64",  schema["total_columns"]),
        ])).result()

        return new_version

    # ── Main detect ────────────────────────────────────────────────────────

    def detect(self, bucket_name: str, file_path: str) -> Dict[str, Any]:
        file_name = file_path.split("/")[-1]
        carrier   = extract_carrier_from_path(file_path)
        meta      = extract_metadata_from_filename(file_name)

        print(f"[INFO] Archivo: {file_name} | Carrier: {carrier} | AOR: {meta['aor_id']}")

        new_schema  = self.extract_schema(file_path)
        current     = self.get_current_schema(carrier)
        old_version = current["schema_version"] if current else None
        changes     = self.compare_schemas(current, new_schema)

        if not current or changes:
            new_version = self.register_schema(carrier, file_name, new_schema, changes, old_version, meta["aor_id"])
            status = "new_schema" if not current else "schema_changed"
            if changes:
                print(f"[ALERT] {len(changes)} cambio(s) detectados en {carrier}")
        else:
            new_version = old_version
            status = "no_changes"
            print(f"[OK] Sin cambios en schema de {carrier}")

        return {
            "status": status, "carrier": carrier,
            "aor_npn": meta["aor_id"], "aor_name": meta["aor_name"],
            "file_name": file_name, "version": new_version,
            "total_columns": new_schema["total_columns"], "changes": changes,
        }


# ── Entry point ────────────────────────────────────────────────────────────

@functions_framework.cloud_event
def detect_schema(cloud_event):
    """Cloud Function activada por nuevo archivo en GCS."""
    data      = cloud_event.data
    bucket    = data["bucket"]
    file_path = data["name"]

    print(f"[EVENT] gs://{bucket}/{file_path}")

    if not file_path.startswith("Data_Lake/Bronze/") or not file_path.endswith(".csv"):
        print("[SKIP] No es CSV en Bronze")
        return {"status": "ignored"}

    result = SchemaDetectorService().detect(bucket, file_path)
    print(f"[RESULT] {json.dumps(result, indent=2)}")
    return result
