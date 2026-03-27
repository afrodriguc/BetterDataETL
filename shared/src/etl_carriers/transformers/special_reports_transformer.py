"""
Transformador para reportes especiales (Sherpa, TLD, etc.)
"""

import pandas as pd
from typing import Dict, List, Any

from etl_carriers.config import SPECIAL_REPORTS, get_special_report_config
from etl_carriers.utils import parse_date, clean_phone
from dateutil.parser import parse as parse_datetime

class SpecialReportsTransformer:
    """Transformador para tablas Silver especiales."""
    
    def __init__(self):
        self.special_reports = SPECIAL_REPORTS
    
    def transform_value(self, value, data_type):
        if pd.isna(value) or value is None:
            return None
        if data_type == "STRING":
            return str(value).strip() if value else None
        elif data_type == "STRING_UPPER":
            return str(value).strip().upper() if value else None
        elif data_type == "PHONE":
            return clean_phone(value)
        elif data_type == "DATE":
            return parse_date(value)
        elif data_type == "TIMESTAMP":
            if not value:
                return None
            try:
                return parse_datetime(str(value)).isoformat()  # ← .isoformat()
            except Exception:
                return None
        elif data_type == "FLOAT":
            try:
                clean_val = str(value).replace("$", "").replace(",", "")
                return float(clean_val)
            except (ValueError, TypeError):
                return None
        elif data_type == "INT":
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return None
        return str(value) if value else None
    
    def transform_row(self, row, carrier, metadata):
        config = get_special_report_config(carrier)
        if not config:
            raise ValueError(f"No config for carrier: {carrier}")
        record = {
            "aor_id": metadata.aor_id,
            "aor_name": metadata.aor_name,
            "extraction_date": metadata.extraction_date,
            "source_file": metadata.file_name,
        }
        for silver_col, source_col, data_type in config["columns"]:
            value = row.get(source_col)
            record[silver_col] = self.transform_value(value, data_type)
        return record
    
    def transform_dataframe(self, df, carrier, metadata):
        records = []
        for idx, row in df.iterrows():
            try:
                record = self.transform_row(row, carrier, metadata)
                records.append(record)
            except Exception as e:
                print(f"Error en fila {idx}: {e}")
                continue
        return records

