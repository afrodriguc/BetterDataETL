"""
Cloud Function: cleanup_on_delete

Se activa cuando se ELIMINA un archivo de la capa Bronze en GCS.
Elimina los registros correspondientes de la tabla Silver en BigQuery.

Trigger: google.cloud.storage.object.v1.deleted
"""

import json
import functions_framework

from etl_carriers.config import PROJECT_ID, get_table_for_carrier
from etl_carriers.utils import extract_carrier_from_path, is_valid_bronze_file
from etl_carriers.loaders import BigQueryLoader


class CleanupService:
    """Elimina datos Silver cuando se borra un archivo Bronze."""

    def __init__(self, project_id: str = PROJECT_ID):
        self.project_id = project_id
        self.bq = BigQueryLoader(project_id)

    def cleanup(self, file_path: str) -> dict:
        file_name = file_path.split("/")[-1]
        carrier   = extract_carrier_from_path(file_path)

        print(f"[INFO] Carrier: {carrier}")

        if carrier == "unknown":
            return {"status": "error", "reason": "unknown_carrier", "file_path": file_path}

        table_id = get_table_for_carrier(carrier, self.project_id)
        print(f"[INFO] Tabla Silver: {table_id}")

        result = self.bq.delete_by_source_file(table_id, file_name)

        if result["status"] == "success":
            deleted = result["deleted_rows"]
            print(f"[OK] Eliminadas {deleted} filas de {table_id}")
            return {
                "status": "success",
                "file_name": file_name,
                "carrier": carrier,
                "table": table_id,
                "rows_deleted": deleted,
            }

        print(f"[ERROR] {result['error']}")
        return {"status": "error", "error": result["error"], "file_name": file_name}


# ── Entry point ────────────────────────────────────────────────────────────

@functions_framework.cloud_event
def cleanup_on_delete(cloud_event):
    """Cloud Function activada por eliminación de archivo en GCS."""
    data      = cloud_event.data
    file_path = data["name"]
    bucket    = data["bucket"]

    print(f"[EVENT] Archivo eliminado: gs://{bucket}/{file_path}")

    if not is_valid_bronze_file(file_path):
        print("[SKIP] Archivo no válido para cleanup")
        return {"status": "ignored", "reason": "invalid_file"}

    result = CleanupService().cleanup(file_path)
    print(f"[RESULT] {json.dumps(result, default=str)}")
    return result
