"""
Cloud Function: manual_etl_loader

Carga manual (o programada) de archivos Bronze → Silver.
Útil para reprocesar datos o cargar lotes históricos.

Trigger: HTTP
Entry point: manual_etl_loader

Variables de entorno:
    TRUNCATE      — "true" para truncar tablas antes de cargar (default: false)
    CARRIER       — Filtrar por carrier específico (ej: "floridablue", "cigna_pending")
    TABLE         — Tabla a truncar: "policies", "sherpa", "tld", "all" (default: auto)
    BRONZE_PATH   — Ruta específica dentro de Bronze (default: prefijo completo)
    DRY_RUN       — "true" para mostrar qué se haría sin ejecutar (default: false)
"""

import os
import functions_framework
from datetime import datetime
from typing import Dict, List, Any

from etl_carriers.config import (
    PROJECT_ID,
    BUCKET_NAME,
    BRONZE_PREFIX,
    SILVER_DATASET,
    is_special_report,
    get_special_report_config,
)
from etl_carriers.utils import extract_file_metadata
from etl_carriers.loaders import GCSLoader, BigQueryLoader
from etl_carriers.transformers import (
    PoliciesTransformer,
    SpecialReportsTransformer,
    FloridaBlueEnricher,
    CignaEnricher,
)


# Mapa de tablas Silver y sus carriers
SILVER_TABLES = {
    "policies": {
        "full_name": f"{SILVER_DATASET}.policies",
        "carriers": [
            "ambetter", "amerihealth", "anthem", "aetna", "bluecross",
            "cigna", "community", "floridablue", "molina", "oscar",
            "united", "cigna_pending",
        ],
    },
    "sherpa":                {"full_name": f"{SILVER_DATASET}.sherpa",                 "carriers": ["sherpa"]},
    "tld":                   {"full_name": f"{SILVER_DATASET}.tld",                    "carriers": ["tld"]},
    "bluecross_applications":{"full_name": f"{SILVER_DATASET}.bluecross_applications", "carriers": ["bluecross_application"]},
    "floridablue_aligned":   {"full_name": f"{SILVER_DATASET}.floridablue_aligned",    "carriers": ["florida_blue_aligned"]},
}   


class ManualETLLoader:
    """
    Cargador manual Bronze → Silver.

    Lee todos los archivos del bucket que coincidan con los filtros configurados
    y los transforma/carga en BigQuery. No verifica duplicados — permite
    reprocesar archivos cuando se necesita.
    """

    def __init__(self, project_id: str = PROJECT_ID):
        self.project_id = project_id
        self.gcs = GCSLoader(project_id)
        self.bq = BigQueryLoader(project_id)
        self.policies_transformer = PoliciesTransformer()
        self.special_transformer = SpecialReportsTransformer()
        self.fb_enricher = FloridaBlueEnricher(self.gcs.storage_client, BUCKET_NAME)
        self.cigna_enricher = CignaEnricher(self.gcs.storage_client, BUCKET_NAME)

        # Configuración desde variables de entorno
        self.truncate      = os.environ.get("TRUNCATE",     "false").lower() == "true"
        self.carrier_filter= os.environ.get("CARRIER",      "").lower() or None
        self.table_filter  = os.environ.get("TABLE",        "").lower() or None
        self.path_filter   = os.environ.get("BRONZE_PATH",  "").strip() or None
        self.dry_run       = os.environ.get("DRY_RUN",      "false").lower() == "true"
        self.file_filter   = os.environ.get("FILE_NAME",    "").strip() or None

        self.stats = {
            "files_found": 0, "files_processed": 0,
            "files_skipped": 0, "files_error": 0,
            "rows_loaded": 0, "rows_enriched": 0,
            "rows_filtered": 0, "tables_truncated": [],
        }

    # ── Logging ────────────────────────────────────────────────────────────

    def log(self, msg: str, level: str = "INFO"):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prefix = "[DRY-RUN] " if self.dry_run else ""
        print(f"[{ts}] [{level}] {prefix}{msg}")

    # ── Truncate ───────────────────────────────────────────────────────────

    def _tables_to_truncate(self) -> List[str]:
        if self.table_filter == "all":
            return list(SILVER_TABLES.keys())
        if self.table_filter and self.table_filter in SILVER_TABLES:
            return [self.table_filter]
        if self.carrier_filter:
            return [
                name for name, cfg in SILVER_TABLES.items()
                if self.carrier_filter in cfg["carriers"]
            ]
        return ["policies"]

    def truncate_tables(self):
        for table_name in self._tables_to_truncate():
            table_id = f"{self.project_id}.{SILVER_TABLES[table_name]['full_name']}"
            self.log(f"  Truncando: {table_id}")
            if not self.dry_run:
                if self.bq.truncate_table(table_id):
                    self.stats["tables_truncated"].append(table_name)

    # ── File processing ────────────────────────────────────────────────────

    def list_bronze_files(self) -> List[Dict[str, Any]]:
            prefix = self.path_filter or BRONZE_PREFIX
            self.log(f"  Buscando en: {prefix}")
            files = self.gcs.list_files(prefix=prefix, carrier_filter=self.carrier_filter)

            # Filtrar por nombre de archivo si se especificó FILE_NAME
            if self.file_filter:
                files = [f for f in files if f["name"] == self.file_filter]
                self.log(f"  Filtro FILE_NAME: '{self.file_filter}'")

            self.stats["files_found"] = len(files)
            self.log(f"  Archivos encontrados: {len(files)}")
            return files

    def process_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        file_path = file_info["path"]
        metadata  = extract_file_metadata(file_path)
        carrier   = metadata.carrier

        self.log(f"Procesando: {metadata.file_name} (carrier: {carrier})")

        if self.dry_run:
            return {"status": "dry_run", "file": metadata.file_name}

        try:
            df = self.gcs.read_file(file_path, carrier)
            self.log(f"  Filas leídas: {len(df)}")

            if carrier == "cigna_pending":
                return self._process_cigna_pending(df, file_path, metadata)

            if is_special_report(carrier):
                return self._process_special(df, carrier, metadata)

            return self._process_policies(df, file_path, carrier, metadata)

        except Exception as exc:
            import traceback
            self.log(f"  Error: {str(exc)}", "ERROR")
            self.log(traceback.format_exc(), "ERROR")
            return {"status": "error", "error": str(exc)}

    def _process_policies(self, df, file_path, carrier, metadata) -> Dict[str, Any]:
        aligned_data = None
        if carrier == "floridablue":
            aligned_path = self.fb_enricher.get_aligned_file_path(file_path)
            if self.fb_enricher.check_aligned_file_exists(aligned_path):
                aligned_data = self.fb_enricher.load_aligned_data(aligned_path)

        records = self.policies_transformer.transform_dataframe(df, metadata)

        enriched = 0
        if carrier == "floridablue" and aligned_data is not None:
            records = [self.fb_enricher.enrich_record(r, aligned_data) for r in records]
            enriched = sum(1 for r in records if r.get("_extra_fields", {}).get("enriched_from_aligned"))
            self.stats["rows_enriched"] += enriched

        loaded = self.bq.load_to_policies(records)
        self.log(f"  Cargadas: {loaded} filas")
        return {"status": "success", "rows_read": len(df), "rows_loaded": loaded, "rows_enriched": enriched}

    def _process_cigna_pending(self, df, file_path, metadata) -> Dict[str, Any]:
        active_path = self.cigna_enricher.get_active_file_path(file_path)
        active_ids  = set()
        if self.cigna_enricher.check_file_exists(active_path):
            active_ids = self.cigna_enricher.load_active_application_ids(active_path)

        original_count = len(df)
        records = self.cigna_enricher.transform_pending_dataframe(df, metadata, active_ids)
        filtered = original_count - len(records)
        self.stats["rows_filtered"] += filtered

        if not records:
            return {"status": "skipped", "reason": "all_records_in_active", "rows_loaded": 0}

        loaded = self.bq.load_to_policies(records)
        return {"status": "success", "rows_read": original_count, "rows_filtered": filtered, "rows_loaded": loaded}

    def _process_special(self, df, carrier, metadata) -> Dict[str, Any]:
        config = get_special_report_config(carrier)
        table  = config["table"].replace("silver.", "")
        records = self.special_transformer.transform_dataframe(df, carrier, metadata)
        loaded  = self.bq.load_to_special_table(records, table)
        self.log(f"  Cargadas: {loaded} filas a {config['table']}")
        return {"status": "success", "table": config["table"], "rows_read": len(df), "rows_loaded": loaded}

    # ── Main run ───────────────────────────────────────────────────────────

    def run(self):
        start = datetime.now()
        self.log("=" * 60)
        self.log("Manual ETL Loader")
        self.log(f"  TRUNCATE:     {self.truncate}")
        self.log(f"  CARRIER:      {self.carrier_filter or 'todos'}")
        self.log(f"  TABLE:        {self.table_filter or 'auto'}")
        self.log(f"  BRONZE_PATH:  {self.path_filter or 'Bronze completo'}")
        self.log(f"  DRY_RUN:      {self.dry_run}")
        self.log("=" * 60)

        if self.truncate:
            self.log("PASO 1: Truncando tablas...")
            self.truncate_tables()

        self.log("PASO 2: Listando archivos en Bronze...")
        files = self.list_bronze_files()
        if not files:
            self.log("No se encontraron archivos.")
            return

        self.log(f"PASO 3: Procesando {len(files)} archivos...")
        for i, file_info in enumerate(files, 1):
            self.log(f"\n[{i}/{len(files)}] {file_info['name']}")
            result = self.process_file(file_info)

            if result["status"] in ("success", "dry_run"):
                self.stats["files_processed"] += 1
                self.stats["rows_loaded"] += result.get("rows_loaded", 0)
            elif result["status"] == "skipped":
                self.stats["files_skipped"] += 1
            else:
                self.stats["files_error"] += 1

        duration = (datetime.now() - start).total_seconds()
        self.log("\n" + "=" * 60)
        self.log("RESUMEN FINAL")
        self.log(f"  Duración:           {duration:.1f}s")
        self.log(f"  Archivos encontrados: {self.stats['files_found']}")
        self.log(f"  Archivos procesados:  {self.stats['files_processed']}")
        self.log(f"  Archivos saltados:    {self.stats['files_skipped']}")
        self.log(f"  Archivos con error:   {self.stats['files_error']}")
        self.log(f"  Filas cargadas:       {self.stats['rows_loaded']}")
        self.log(f"  Filas enriquecidas:   {self.stats['rows_enriched']}")
        self.log(f"  Filas filtradas:      {self.stats['rows_filtered']}")
        if self.stats["tables_truncated"]:
            self.log(f"  Tablas truncadas:     {', '.join(self.stats['tables_truncated'])}")
        self.log("=" * 60)


# ── Entry points ───────────────────────────────────────────────────────────

@functions_framework.http
def manual_etl_loader(request):
    """HTTP Cloud Function entry point."""
    try:
        ManualETLLoader().run()
        return {"status": "success"}, 200
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return {"status": "error", "message": str(exc)}, 500


def main():
    """Entry point para ejecución local."""
    ManualETLLoader().run()


if __name__ == "__main__":
    main()
