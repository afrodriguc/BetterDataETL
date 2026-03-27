"""
Tests unitarios para etl_carriers.utils.data_parsers

Cubren todas las funciones de parsing y limpieza de datos
sin necesidad de conexión a GCP.
"""

import pytest
from etl_carriers.utils.data_parsers import (
    parse_date,
    clean_phone,
    split_full_name,
    combine_names_and_split,
    normalize_member_type,
    clean_cigna_application_id,
    normalize_floridablue_status,
)


# ── parse_date ─────────────────────────────────────────────────────────────

class TestParseDate:
    def test_iso_format(self):
        assert parse_date("2026-01-15") == "2026-01-15"

    def test_us_slash_format(self):
        assert parse_date("01/15/2026") == "2026-01-15"

    def test_short_year_format(self):
        assert parse_date("01/15/26") == "2026-01-15"

    def test_with_time_component(self):
        assert parse_date("2026-01-15T10:30:00") == "2026-01-15"

    def test_9999_date_returns_none(self):
        assert parse_date("9999-12-31") is None

    def test_none_returns_none(self):
        assert parse_date(None) is None

    def test_empty_string_returns_none(self):
        assert parse_date("") is None

    def test_dash_returns_none(self):
        assert parse_date("-") is None

    def test_nan_string_returns_none(self):
        assert parse_date("nan") is None


# ── clean_phone ────────────────────────────────────────────────────────────

class TestCleanPhone:
    def test_formatted_number(self):
        assert clean_phone("(305) 555-1234") == "3055551234"

    def test_dashes_format(self):
        assert clean_phone("305-555-1234") == "3055551234"

    def test_plain_digits(self):
        assert clean_phone("3055551234") == "3055551234"

    def test_with_country_code(self):
        result = clean_phone("+13055551234")
        assert result == "13055551234"

    def test_scientific_notation(self):
        # Molina envía teléfonos en notación científica
        result = clean_phone("3.05555e+09")
        assert result is not None
        assert result.startswith("305")

    def test_too_short_returns_none(self):
        assert clean_phone("12345") is None

    def test_none_returns_none(self):
        assert clean_phone(None) is None


# ── split_full_name ────────────────────────────────────────────────────────

class TestSplitFullName:
    def test_first_last(self):
        result = split_full_name("Juan Perez")
        assert result["first"] == "Juan"
        assert result["last"] == "Perez"
        assert result["middle"] is None

    def test_first_middle_last(self):
        result = split_full_name("Juan Carlos Perez")
        assert result["first"] == "Juan"
        assert result["middle"] == "Carlos"
        assert result["last"] == "Perez"

    def test_last_comma_first(self):
        result = split_full_name("Perez, Juan")
        assert result["first"] == "Juan"
        assert result["last"] == "Perez"

    def test_last_comma_first_middle(self):
        result = split_full_name("Garcia, Juan Carlos")
        assert result["first"] == "Juan"
        assert result["middle"] == "Carlos"
        assert result["last"] == "Garcia"

    def test_single_name(self):
        result = split_full_name("Madonna")
        assert result["first"] == "Madonna"
        assert result["last"] is None

    def test_none_returns_all_none(self):
        result = split_full_name(None)
        assert result == {"first": None, "middle": None, "last": None}


# ── normalize_member_type ──────────────────────────────────────────────────

class TestNormalizeMemberType:
    def test_self_to_policy_holder(self):
        assert normalize_member_type("Self") == "Policy Holder"

    def test_subscriber_to_policy_holder(self):
        assert normalize_member_type("Subscriber") == "Policy Holder"

    def test_primary_to_policy_holder(self):
        assert normalize_member_type("Primary") == "Policy Holder"

    def test_spouse(self):
        assert normalize_member_type("Spouse") == "Spouse"

    def test_child_to_dependent(self):
        assert normalize_member_type("Child") == "Dependent"

    def test_dependent(self):
        assert normalize_member_type("Dependent") == "Dependent"

    def test_case_insensitive(self):
        assert normalize_member_type("SELF") == "Policy Holder"
        assert normalize_member_type("self") == "Policy Holder"

    def test_none_returns_none(self):
        assert normalize_member_type(None) is None


# ── clean_cigna_application_id ─────────────────────────────────────────────

class TestCleanCignaApplicationId:
    def test_removes_suffix(self):
        assert clean_cigna_application_id("5266873188-3") == "5266873188"

    def test_removes_single_digit_suffix(self):
        assert clean_cigna_application_id("1234567890-1") == "1234567890"

    def test_no_suffix_unchanged(self):
        assert clean_cigna_application_id("HX80961141") == "HX80961141"

    def test_none_returns_none(self):
        assert clean_cigna_application_id(None) is None


# ── normalize_floridablue_status ───────────────────────────────────────────

class TestNormalizeFloridaBlueStatus:
    def test_actv_to_active(self):
        assert normalize_floridablue_status("ACTV") == "Active"

    def test_term_to_terminated(self):
        assert normalize_floridablue_status("TERM") == "Terminated"

    def test_cancel_to_cancelled(self):
        assert normalize_floridablue_status("CANCEL") == "Cancelled"

    def test_none_returns_none(self):
        assert normalize_floridablue_status(None) is None
