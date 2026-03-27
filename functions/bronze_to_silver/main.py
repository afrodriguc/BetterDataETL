"""
Cloud Function: bronze_to_silver

Se activa cuando se sube un archivo CSV/XLSX a la capa Bronze en GCS.
Lee el archivo, lo transforma al formato Silver y lo carga en BigQuery.

Trigger: google.cloud.storage.object.v1.finalized
"""

import json
import functions_framework

from etl_carriers.config import PROJECT_ID, BUCKET_NAME, is_special_report, get_special_report_config
from etl_carriers.utils import extract_file_metadata, is_valid_bronze_file
from etl_carriers.loaders import GCSLoader, BigQueryLoader
from etl_carriers.transformers import (
    PoliciesTransformer,
    SpecialReportsTransformer,
    FloridaBlueEnricher,
    CignaEnricher,
)

from .status_monitor import StatusMonitor


class BronzeToSilverService:
    """
    Orquesta el proceso completo Bronze → Silver para un archivo dado.

    Responsabilidades:
    - Detectar el tipo de carrier desde el path del archivo
    - Delegar la transformación al transformer o enricher correspondiente
    - Cargar los resultados en BigQuery
    - Evitar reprocesamiento de archivos ya cargados
    """

    def __init__(self, project_id: str = PROJECT_ID):
        self.project_id = project_id
        self.gcs = GCSLoader(project_id)
        self.bq = BigQueryLoader(project_id)
        self.policies_transformer = PoliciesTransformer()
        self.special_transformer = SpecialReportsTransformer()
        self.fb_enricher = FloridaBlueEnricher(self.gcs.storage_client, BUCKET_NAME)
        self.cigna_enricher = CignaEnricher(self.gcs.storage_client, BUCKET_NAME)
        self.status_monitor = StatusMonitor(project_id)

    def process(self, bucket_name: str, file_path: str) -> dict:
        """
        Procesa un archivo Bronze completo.

        Returns:
            Dict con status, carrier, filas leídas y filas cargadas.
        """
        metadata = extract_file_metadata(file_path)
        carrier = metadata.carrier

        print(f"[INFO] Procesando: {metadata.file_name}")
        print(f"[INFO] Carrier: {carrier} | AOR: {metadata.aor_id} ({metadata.aor_name})")

        if self._is_already_processed(metadata, carrier):
            print(f"[SKIP] Archivo ya procesado: {metadata.file_name}")
            return {"status": "already_processed", "carrier": carrier, "file": metadata.file_name}

        if carrier == "cigna_pending":
            return self._process_cigna_pending(file_path, metadata)

        if carrier == "bluecross_application":
            return self._process_bluecross_application(file_path, metadata)

        if is_special_report(carrier):
            return self._process_special_report(file_path, carrier, metadata)

        return self._process_policies(file_path, carrier, metadata)

    # ── Private helpers ────────────────────────────────────────────────────

    def _is_already_processed(self, metadata, carrier: str) -> bool:
        table = "policies"
        if is_special_report(carrier):
            config = get_special_report_config(carrier)
            table = config["table"].replace("silver.", "")
        elif carrier == "bluecross_application":
            table = "bluecross_applications"

        return not self.bq.validate_file_not_processed(
            metadata.file_name, metadata.extraction_date, carrier, table
        )

    def _process_policies(self, file_path: str, carrier: str, metadata) -> dict:
        df = self.gcs.read_file(file_path, carrier)
        print(f"[INFO] Filas leídas: {len(df)}")

        aligned_data = None
        if carrier == "floridablue":
            aligned_path = self.fb_enricher.get_aligned_file_path(file_path)
            if self.fb_enricher.check_aligned_file_exists(aligned_path):
                aligned_data = self.fb_enricher.load_aligned_data(aligned_path)

        records = self.policies_transformer.transform_dataframe(df, metadata)

        if carrier == "floridablue" and aligned_data is not None:
            records = [self.fb_enricher.enrich_record(r, aligned_data) for r in records]

        self.status_monitor.detect_new_values(
            carrier=carrier, records=records,
            file_name=metadata.file_name,
            aor_id=metadata.aor_id, aor_name=metadata.aor_name,
        )

        loaded = self.bq.load_to_policies(records)
        print(f"[OK] Cargadas a silver.policies: {loaded}")

        return {
            "status": "success", "carrier": carrier,
            "file": metadata.file_name,
            "rows_read": len(df), "rows_loaded": loaded,
        }

    def _process_cigna_pending(self, file_path: str, metadata) -> dict:
        df = self.gcs.read_file(file_path, "cigna_pending")

        active_path = self.cigna_enricher.get_active_file_path(file_path)
        active_ids = set()
        if self.cigna_enricher.check_file_exists(active_path):
            active_ids = self.cigna_enricher.load_active_application_ids(active_path)

        records = self.cigna_enricher.transform_pending_dataframe(df, metadata, active_ids)

        if not records:
            return {"status": "success", "carrier": "cigna_pending", "rows_loaded": 0}

        self.status_monitor.detect_new_values(
            carrier="cigna", records=records,
            file_name=metadata.file_name,
            aor_id=metadata.aor_id, aor_name=metadata.aor_name,
        )

        loaded = self.bq.load_to_policies(records)
        return {"status": "success", "carrier": "cigna_pending", "rows_loaded": loaded}

    def _process_bluecross_application(self, file_path: str, metadata) -> dict:
        df = self.gcs.read_file(file_path, "bluecross_application")

        raw_records = [
            {**dict(row), "carrier": metadata.carrier, "aor_id": metadata.aor_id,
             "aor_name": metadata.aor_name, "extraction_date": str(metadata.extraction_date),
             "source_file": metadata.file_name}
            for _, row in df.iterrows()
        ]
        raw_loaded = self.bq.load_to_special_table(raw_records, "bluecross_applications")

        policy_records = self.policies_transformer.transform_dataframe(df, metadata)
        self.status_monitor.detect_new_values(
            carrier="bluecross", records=policy_records,
            file_name=metadata.file_name,
            aor_id=metadata.aor_id, aor_name=metadata.aor_name,
        )
        policies_loaded = self.bq.load_to_policies(policy_records)

        return {
            "status": "success", "carrier": "bluecross_application",
            "rows_loaded_raw": raw_loaded, "rows_loaded_policies": policies_loaded,
        }

    def _process_special_report(self, file_path: str, carrier: str, metadata) -> dict:
        config = get_special_report_config(carrier)
        table = config["table"].replace("silver.", "")

        df = self.gcs.read_file(file_path, carrier)
        records = self.special_transformer.transform_dataframe(df, carrier, metadata)
        loaded = self.bq.load_to_special_table(records, table)
        print(f"[OK] Cargadas a silver.{table}: {loaded}")

        return {
            "status": "success", "carrier": carrier,
            "target_table": config["table"],
            "rows_read": len(df), "rows_loaded": loaded,
        }


# ── Cloud Function entry point ─────────────────────────────────────────────

@functions_framework.cloud_event
def bronze_to_silver(cloud_event):
    """Entry point de la Cloud Function."""
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_path = data["name"]

    print(f"[EVENT] gs://{bucket_name}/{file_path}")

    if not is_valid_bronze_file(file_path):
        print(f"[SKIP] Ruta no válida para Bronze: {file_path}")
        return

    try:
        service = BronzeToSilverService()
        result = service.process(bucket_name, file_path)
        print(f"[RESULT] {json.dumps(result, default=str)}")
        return result

    except Exception as exc:
        print(f"[ERROR] {file_path}: {exc}")
        raise
