"""
Configuración del entorno.

Los valores de PROJECT_ID y BUCKET_NAME se leen desde variables de entorno,
lo que permite que el mismo código corra en producción y en el entorno de pruebas
simplemente cambiando las variables — sin modificar código.

Variables de entorno:
    GCP_PROJECT   → ID del proyecto GCP  (default: better-wase-data-2)
    GCS_BUCKET    → Nombre del bucket    (default: better-wase-data-2)
    BRONZE_PREFIX → Prefijo Bronze       (default: Data_Lake/Bronze/)
"""

import os

PROJECT_ID: str = os.environ.get("GCP_PROJECT", "better-wase-data-2")
BUCKET_NAME: str = os.environ.get("GCS_BUCKET", "better-wase-data-2")
BRONZE_PREFIX    = os.environ.get("BRONZE_PREFIX",    "Data_Lake/Bronze/")
SILVER_DATASET   = os.environ.get("SILVER_DATASET",   "silver")