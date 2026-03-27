"""
etl_carriers — Paquete compartido del pipeline ETL de BetterWase.

Expone todos los módulos públicos del core para que cada Cloud Function
los importe sin necesidad de copiar código.

Uso:
    from etl_carriers.config import PROJECT_ID, BUCKET_NAME
    from etl_carriers.loaders import GCSLoader, BigQueryLoader
    from etl_carriers.transformers import PoliciesTransformer
"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("etl-carriers")
except PackageNotFoundError:
    __version__ = "dev"
