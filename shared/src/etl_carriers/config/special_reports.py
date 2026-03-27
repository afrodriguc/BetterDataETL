"""
Configuración de reportes especiales que van a tablas Silver dedicadas.
Cada reporte tiene su propio esquema y tabla destino.
"""

from typing import Dict, List, Tuple, Any


# Columna format: (silver_column, source_column, data_type)
# data_types: STRING, STRING_UPPER, PHONE, DATE, FLOAT, INT, TIMESTAMP

SPECIAL_REPORTS: Dict[str, Dict[str, Any]] = {
    # NOTA: cigna_pending fue movido a silver.policies (ver carrier_config.py)
    
    'bluecross_application': {
        'table': 'silver.bluecross_applications',
        'columns': [
            ('last_name', 'Last Name', 'STRING'),
            ('first_name', 'First Name', 'STRING'),
            ('record_type', 'Record Type', 'STRING'),
            ('e_app_number', 'E-App Number', 'STRING'),
            ('exchange_id', 'Exchange ID', 'STRING'),
            ('client_app_id', 'Client App ID', 'STRING'),
            ('group_number', 'Group Number', 'STRING'),
            ('account_number', 'Account Number', 'STRING'),
            ('status', 'Status', 'STRING'),
            ('product_type', 'Product Type', 'STRING'),
            ('plan_name', 'Plan Name', 'STRING'),
            ('source', 'Source', 'STRING'),
            ('coverage_effective_date', 'Coverage Effective Date', 'DATE'),
            ('producer_name', 'Producer Name', 'STRING'),
            ('nine_digit_producer_number', 'Nine Digit Producer Number', 'STRING'),
            ('client_address_1', 'Client Address 1', 'STRING'),
            ('client_address_2', 'Client Address 2', 'STRING'),
            ('city', 'City', 'STRING'),
            ('state', 'State', 'STRING'),
            ('zip_code', 'Zip Code', 'STRING'),
            ('client_phone', "Client's Primary Phone", 'PHONE'),
            ('email', 'Email', 'STRING'),
        ]
    },
    
    'sherpa': {
        'table': 'silver.sherpa',
        'columns': [
            # Datos personales
            ('first_name', 'first_name', 'STRING_UPPER'),
            ('last_name', 'last_name', 'STRING_UPPER'),
            ('gender', 'gender', 'STRING'),
            ('address', 'address', 'STRING'),
            ('unit', 'unit', 'STRING'),
            ('city', 'city', 'STRING'),
            ('state', 'state', 'STRING'),
            ('zip_code', 'zip_code', 'STRING'),
            ('county', 'county', 'STRING'),
            ('phone', 'phone', 'PHONE'),
            ('email', 'email', 'STRING'),
            # Póliza
            ('effective_date', 'effective_date', 'DATE'),
            ('premium_paid', 'premium_paid', 'STRING'),
            ('followup_docs', 'followup_docs', 'STRING'),
            ('premium', 'premium', 'FLOAT'),
            ('subsidy', 'subsidy', 'FLOAT'),
            ('net_premium', 'net_premium', 'FLOAT'),
            ('applicant_count', 'applicant_count', 'INT'),
            ('policy_status', 'policy_status', 'STRING'),
            ('household_size', 'household_size', 'INT'),
            # Identificadores
            ('ffm_app_id', 'ffm_app_id', 'STRING'),
            ('ffm_subscriber_id', 'ffm_subscriber_id', 'STRING'),
            ('issuer_assigned_policy_id', 'issuer_assigned_policy_id', 'STRING'),
            ('issuer_assigned_subscriber_id', 'issuer_assigned_subscriber_id', 'STRING'),
            ('issuer_assigned_primary_member_id', 'issuer_assigned_primary_member_id', 'STRING'),
            ('healthsherpa_id', 'healthsherpa_id', 'STRING'),
            ('healthsherpa_policy_ids', 'healthsherpa_policy_ids', 'STRING'),
            ('transaction_id', 'transaction_id', 'STRING'),
            ('last_ede_sync', 'last_ede_sync', 'STRING'),
            # Spouse
            ('spouse_first_name', 'spouse_first_name', 'STRING_UPPER'),
            ('spouse_last_name', 'spouse_last_name', 'STRING_UPPER'),
            # Other 1-10
            ('other_1_first_name', 'other_1_first_name', 'STRING_UPPER'),
            ('other_1_last_name', 'other_1_last_name', 'STRING_UPPER'),
            ('other_2_first_name', 'other_2_first_name', 'STRING_UPPER'),
            ('other_2_last_name', 'other_2_last_name', 'STRING_UPPER'),
            ('other_3_first_name', 'other_3_first_name', 'STRING_UPPER'),
            ('other_3_last_name', 'other_3_last_name', 'STRING_UPPER'),
            ('other_4_first_name', 'other_4_first_name', 'STRING_UPPER'),
            ('other_4_last_name', 'other_4_last_name', 'STRING_UPPER'),
            ('other_5_first_name', 'other_5_first_name', 'STRING_UPPER'),
            ('other_5_last_name', 'other_5_last_name', 'STRING_UPPER'),
            ('other_6_first_name', 'other_6_first_name', 'STRING_UPPER'),
            ('other_6_last_name', 'other_6_last_name', 'STRING_UPPER'),
            ('other_7_first_name', 'other_7_first_name', 'STRING_UPPER'),
            ('other_7_last_name', 'other_7_last_name', 'STRING_UPPER'),
            ('other_8_first_name', 'other_8_first_name', 'STRING_UPPER'),
            ('other_8_last_name', 'other_8_last_name', 'STRING_UPPER'),
            ('other_9_first_name', 'other_9_first_name', 'STRING_UPPER'),
            ('other_9_last_name', 'other_9_last_name', 'STRING_UPPER'),
            ('other_10_first_name', 'other_10_first_name', 'STRING_UPPER'),
            ('other_10_last_name', 'other_10_last_name', 'STRING_UPPER'),
            ('application_creation_date', 'application_creation_date', 'TIMESTAMP'),
        ]
    },
    
    'tld': {
        'table': 'silver.tld',
        'columns': [
            ('date_converted', 'date_converted', 'TIMESTAMP'),
            ('date_sold', 'date_sold', 'TIMESTAMP'),
            ('agent_name', 'agent_name', 'STRING'),
            ('lead_id', 'lead_id', 'STRING'),
            ('policy_id', 'policy_id', 'STRING'),
            ('application_number', 'application_number', 'STRING'),
            ('lead_phone', 'lead_phone', 'PHONE'),
            ('lead_phone2', 'lead_phone2', 'PHONE'),
            ('lead_email', 'lead_email', 'STRING'),
            ('lead_first_name', 'lead_first_name', 'STRING_UPPER'),
            ('lead_last_name', 'lead_last_name', 'STRING_UPPER'),
            ('lead_vendor_name', 'lead_vendor_name', 'STRING'),
            ('carrier_name', 'carrier_name', 'STRING'),
            ('lead_state', 'lead_state', 'STRING'),
            ('lead_language_name', 'lead_language_name', 'STRING'),
            ('policy_type', 'policy_type', 'STRING'),
            ('lead_dob', 'lead_dob', 'DATE'),
            ('lead_age', 'lead_age', 'INTEGER'),
            ('lead_zipcode_5', 'lead_zipcode_5', 'STRING'),
        ]
    },
    
    'florida_blue_aligned': {
        'table': 'silver.floridablue_aligned',
        'columns': [
            ('hcc_id', 'HCC_ID', 'STRING'),
            ('member_cip_id', 'MEMBER_CIP_ID', 'STRING'),
            ('member_first_name', 'MEMBER_FIRST_NM', 'STRING_UPPER'),
            ('member_last_name', 'MEMBER_LAST_NM', 'STRING_UPPER'),
            ('member_dob', 'MEMBER_DOB', 'DATE'),
            ('member_phone', 'MEMBER_HOME_PHN', 'PHONE'),
            ('member_email', 'MEMBER_EMAIL_ADDRESS', 'STRING'),
            ('member_county', 'MEMBER_COUNTY_NM', 'STRING'),
            ('exchange_ind', 'EXCHANGE_IND', 'STRING'),
            ('active_member_count', 'ACTIVE_MEMBER_COUNT', 'INT'),
            ('plan_name', 'PLAN_NM', 'STRING'),
            ('plan_type', 'PLAN_TYPE', 'STRING'),
            ('product_name', 'PRODUCT_NAME', 'STRING'),
            ('code_desc', 'CODE_DESC', 'STRING'),
            ('reinstatement_indicator', 'REINSTATEMENT_INDICATOR', 'STRING'),
            ('agein_indicator', 'AGEIN_INDICATOR', 'STRING'),
            ('ageout_indicator', 'AGEOUT_INDICATOR', 'STRING'),
        ]
    },
}


# Mapeo de carriers a sus tablas Silver
CARRIER_TO_TABLE: Dict[str, str] = {
    # NOTA: cigna_pending fue movido a silver.policies
    'bluecross_application': 'bluecross_applications',
    'florida_blue_aligned': 'floridablue_aligned',
    'floridablue_aligned': 'floridablue_aligned',
    'sherpa': 'sherpa',
    'tld': 'tld',
}


def get_special_report_config(carrier: str) -> Dict[str, Any]:
    """Obtiene la configuración de un reporte especial."""
    return SPECIAL_REPORTS.get(carrier)


def is_special_report(carrier: str) -> bool:
    """Verifica si un carrier es un reporte especial."""
    return carrier in SPECIAL_REPORTS


def get_table_for_carrier(carrier: str, project_id: str) -> str:
    """
    Determina la tabla Silver correspondiente al carrier.
    
    Returns:
        ID completo de la tabla (proyecto.schema.tabla)
    """
    if carrier in CARRIER_TO_TABLE:
        table_name = CARRIER_TO_TABLE[carrier]
        return f"{project_id}.silver.{table_name}"
    return f"{project_id}.silver.policies"