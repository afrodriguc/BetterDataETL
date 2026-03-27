"""
Tests unitarios para etl_carriers.utils.file_utils

Cubren la extracción de metadata desde nombres de archivo y rutas de GCS.
"""

import pytest
from etl_carriers.utils.file_utils import (
    extract_carrier_from_path,
    extract_metadata_from_filename,
    extract_file_metadata,
    is_valid_bronze_file,
)


class TestExtractCarrierFromPath:
    def test_ambetter(self):
        path = "Data_Lake/Bronze/ambetter/2026/01/ambetter_2026-01-15_mpt.csv"
        assert extract_carrier_from_path(path) == "ambetter"

    def test_floridablue_folder_variants(self):
        path = "Data_Lake/Bronze/florida_blue/2026/01/file.csv"
        assert extract_carrier_from_path(path) == "floridablue"

    def test_cigna_active_termed(self):
        path = "Data_Lake/Bronze/cigna_active_termed/2026/01/file.xlsx"
        assert extract_carrier_from_path(path) == "cigna"

    def test_cigna_pending(self):
        path = "Data_Lake/Bronze/Cigna_Pending/2026/01/file.xlsx"
        assert extract_carrier_from_path(path) == "cigna_pending"

    def test_unknown_folder(self):
        path = "Data_Lake/Bronze/unknown_carrier/2026/01/file.csv"
        assert extract_carrier_from_path(path) == "unknown"

    def test_invalid_path(self):
        assert extract_carrier_from_path("some/other/path/file.csv") == "unknown"


class TestExtractMetadataFromFilename:
    def test_standard_format(self):
        meta = extract_metadata_from_filename("ambetter_2026-01-15_mpt.csv")
        assert meta["extraction_date"] == "2026-01-15"
        assert meta["aor_id"] == "21072733"
        assert meta["aor_name"] == "Manuel Perez-Trujillo"

    def test_npn_as_aor(self):
        meta = extract_metadata_from_filename("floridablue_2026-01-06_20087079.csv")
        assert meta["aor_id"] == "20087079"
        assert meta["aor_name"] == "Manuel Camelo"

    def test_unknown_aor_uses_default(self):
        # Cuando el código al final no es un AOR válido, usa el default
        meta = extract_metadata_from_filename("cigna_2026-01-16_unknown_code.xlsx")
        assert meta["aor_id"] is not None  # default

    def test_extraction_date_detected(self):
        meta = extract_metadata_from_filename("molina_2026-02-28_dgm.csv")
        assert meta["extraction_date"] == "2026-02-28"


class TestExtractFileMetadata:
    def test_complete_extraction(self):
        path = "Data_Lake/Bronze/ambetter/2026/01/ambetter_2026-01-15_mpt.csv"
        meta = extract_file_metadata(path)
        assert meta.carrier == "ambetter"
        assert meta.file_name == "ambetter_2026-01-15_mpt.csv"
        assert meta.aor_id == "21072733"
        assert meta.extraction_date == "2026-01-15"

    def test_metadata_dataclass_fields(self):
        path = "Data_Lake/Bronze/united/2026/01/united_2026-01-28_mpt.csv"
        meta = extract_file_metadata(path)
        assert hasattr(meta, "carrier")
        assert hasattr(meta, "file_name")
        assert hasattr(meta, "aor_id")
        assert hasattr(meta, "aor_name")
        assert hasattr(meta, "extraction_date")
        assert hasattr(meta, "file_path")


class TestIsValidBronzeFile:
    def test_csv_is_valid(self):
        assert is_valid_bronze_file("Data_Lake/Bronze/ambetter/2026/01/file.csv")

    def test_xlsx_is_valid(self):
        assert is_valid_bronze_file("Data_Lake/Bronze/cigna/2026/01/file.xlsx")

    def test_uppercase_extensions_valid(self):
        assert is_valid_bronze_file("Data_Lake/Bronze/anthem/2026/01/file.CSV")
        assert is_valid_bronze_file("Data_Lake/Bronze/anthem/2026/01/file.XLSX")

    def test_folder_is_invalid(self):
        assert not is_valid_bronze_file("Data_Lake/Bronze/ambetter/2026/01/")

    def test_wrong_prefix_is_invalid(self):
        assert not is_valid_bronze_file("Silver/ambetter/file.csv")

    def test_unsupported_extension_invalid(self):
        assert not is_valid_bronze_file("Data_Lake/Bronze/ambetter/file.txt")
