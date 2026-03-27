"""
Loaders para I/O con Google Cloud Storage y BigQuery.
"""

from .gcs_loader import GCSLoader
from .bigquery_loader import BigQueryLoader

__all__ = ["GCSLoader", "BigQueryLoader"]
