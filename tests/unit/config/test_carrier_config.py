"""
Tests unitarios para etl_carriers.config.carrier_config

Verifican que los mapeos de carriers y la lógica de detección de formatos
sean correctos para todos los carriers soportados.
"""

import pytest
import pandas as pd
from etl_carriers.config import (
    CARRIER_MAPPINGS,
    FLORIDABLUE_FORMATS,
    UNITED_FORMATS,
    COMMUNITY_FORMATS,
    AETNA_FORMATS,
    AMERIHEALTH_FORMATS,
    get_carrier_from_folder,
    is_special_report,
    get_special_report_config,
)
from etl_carriers.transformers import PoliciesTransformer


# ── Carrier mappings ───────────────────────────────────────────────────────

class TestCarrierMappings:
    EXPECTED_CARRIERS = [
        "ambetter", "anthem", "cigna", "floridablue", "molina",
        "united", "community", "amerihealth", "aetna", "oscar",
        "bluecross", "cigna_pending", "bluecross_application",
    ]

    def test_all_carriers_present(self):
        for carrier in self.EXPECTED_CARRIERS:
            assert carrier in CARRIER_MAPPINGS, f"Falta carrier: {carrier}"

    def test_each_carrier_has_policy_id(self):
        for carrier, mapping in CARRIER_MAPPINGS.items():
            assert "policy_id" in mapping, f"{carrier} no tiene policy_id"

    def test_ambetter_status_column(self):
        # Bug histórico: Ambetter se confundía con United en la detección
        assert CARRIER_MAPPINGS["ambetter"]["status"] == "Eligible for Commission"

    def test_united_status_column(self):
        assert CARRIER_MAPPINGS["united"]["status"] == "planStatus"


class TestFolderToCarrier:
    def test_ambetter(self):
        assert get_carrier_from_folder("ambetter") == "ambetter"

    def test_florida_blue_with_space(self):
        assert get_carrier_from_folder("florida_blue") == "floridablue"

    def test_cigna_active_termed(self):
        assert get_carrier_from_folder("cigna_active_termed") == "cigna"

    def test_unknown(self):
        assert get_carrier_from_folder("inexistent") == "unknown"

    def test_case_insensitive(self):
        assert get_carrier_from_folder("AMBETTER") == "ambetter"


class TestSpecialReports:
    def test_tld_is_special(self):
        assert is_special_report("tld")

    def test_sherpa_is_special(self):
        assert is_special_report("sherpa")

    def test_ambetter_not_special(self):
        assert not is_special_report("ambetter")

    def test_tld_config_has_table(self):
        config = get_special_report_config("tld")
        assert config["table"] == "silver.tld"

    def test_sherpa_config_has_columns(self):
        config = get_special_report_config("sherpa")
        assert len(config["columns"]) > 0


# ── Format detection ───────────────────────────────────────────────────────

class TestFormatDetection:
    """Verifica que el transformer detecte correctamente los formatos de cada carrier."""

    def _make_df(self, columns):
        return pd.DataFrame(columns=columns)

    def setup_method(self):
        self.transformer = PoliciesTransformer()

    def test_united_correct_format(self):
        df = self._make_df(["memberFirstName", "memberLastName", "dateOfBirth", "memberNumber"])
        fmt = self.transformer.detect_united_format(df)
        assert fmt == "correct"

    def test_united_normalized_format(self):
        df = self._make_df(["agentId", "agentIdStatus", "agentName", "memberFirstName"])
        fmt = self.transformer.detect_united_format(df)
        assert fmt == "normalized"

    def test_ambetter_not_detected_as_united(self):
        # Bug crítico resuelto: Ambetter compartía columnas con United 'transformed'
        ambetter_cols = [
            "Policy Number", "Exchange Subscriber ID", "Insured First Name",
            "Insured Last Name", "Eligible for Commission", "Member Date Of Birth",
            "Monthly Premium Amount"
        ]
        df = self._make_df(ambetter_cols)
        fmt = self.transformer.detect_united_format(df)
        # Si hay columnas de Ambetter, United NO debe detectar su formato
        assert fmt is None or fmt != "transformed"

    def test_floridablue_legacy_format(self):
        df = self._make_df(["Agency Name", "Agent Name", "Contract ID", "Member FB_UID"])
        fmt = self.transformer.detect_floridablue_format(df)
        assert fmt == "legacy"

    def test_floridablue_basic_format(self):
        df = self._make_df(["MEMBER_FULL_NAME", "AGENCY_AOR", "INSPOLICY_ID", "HCC_ID"])
        fmt = self.transformer.detect_floridablue_format(df)
        assert fmt == "basic"

    def test_community_current_format(self):
        df = self._make_df(["memberFirstName", "memberLastName", "memberDOB", "exchangeMemberID"])
        fmt = self.transformer.detect_community_format(df)
        assert fmt == "current"

    def test_aetna_legacy_format(self):
        df = self._make_df(["First Name", "Last Name", "Policy Status", "Issuer Assigned ID"])
        fmt = self.transformer.detect_aetna_format(df)
        assert fmt == "legacy"

    def test_aetna_current_format(self):
        df = self._make_df(["Member Name", "DOB", "Exchange Assigned ID", "Monthly Premium"])
        fmt = self.transformer.detect_aetna_format(df)
        assert fmt == "current"
