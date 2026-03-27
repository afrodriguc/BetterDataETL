"""
Módulo de configuración centralizada.

Expone todas las constantes, mapeos y helpers de configuración
que el resto del sistema necesita.
"""

from .settings import PROJECT_ID, BUCKET_NAME, BRONZE_PREFIX, SILVER_DATASET
from .aor_config import AOR_MAP, AORInfo, DEFAULT_AOR, get_aor_info, is_valid_aor_code
from .carrier_config import (
    FOLDER_TO_CARRIER,
    CARRIER_MAPPINGS,
    FLORIDABLUE_FORMATS,
    AETNA_FORMATS,
    AMERIHEALTH_FORMATS,
    UNITED_FORMATS,
    COMMUNITY_FORMATS,
    COMMISSION_RULES,
    ANTHEM_FORMATS,
    get_carrier_from_folder,
    get_carrier_mapping,
    is_special_carrier,
    get_commission_rules,
    calculate_commission_status,
)
from .special_reports import (
    SPECIAL_REPORTS,
    CARRIER_TO_TABLE,
    get_special_report_config,
    is_special_report,
    get_table_for_carrier,
)

__all__ = [
    # Settings
    "PROJECT_ID",
    "BUCKET_NAME",
    "BRONZE_PREFIX",
    "SILVER_DATASET",
    # AOR
    "AOR_MAP",
    "AORInfo",
    "DEFAULT_AOR",
    "get_aor_info",
    "is_valid_aor_code",
    # Carrier config
    "FOLDER_TO_CARRIER",
    "CARRIER_MAPPINGS",
    "FLORIDABLUE_FORMATS",
    "AETNA_FORMATS",
    "AMERIHEALTH_FORMATS",
    "UNITED_FORMATS",
    "ANTHEM_FORMATS",
    "COMMUNITY_FORMATS",
    "COMMISSION_RULES",
    "get_carrier_from_folder",
    "get_carrier_mapping",
    "is_special_carrier",
    "get_commission_rules",
    "calculate_commission_status",
    # Special reports
    "SPECIAL_REPORTS",
    "CARRIER_TO_TABLE",
    "get_special_report_config",
    "is_special_report",
    "get_table_for_carrier",
]
