"""
Configuracion centralizada de carriers y sus mapeos de columnas.
Define la estructura de datos de cada aseguradora.
"""

from typing import Dict, List, Tuple, Any


# Mapeo de carpetas Bronze a nombres de carrier normalizados
FOLDER_TO_CARRIER: Dict[str, str] = {
    # Carpetas que van a silver.policies
    'ambetter': 'ambetter',
    'amerihealth': 'amerihealth',
    'anthem': 'anthem',
    'aetna': 'aetna',  # AGREGADO
    'bluecross': 'bluecross',
    'bluecross_policy': 'bluecross',
    'cigna': 'cigna',
    'cigna_active_termed': 'cigna',
    'cigna_pending': 'cigna_pending',
    'community': 'community',
    'floridablue': 'floridablue',
    'florida_blue': 'floridablue',
    'molina': 'molina',
    'oscar': 'oscar',
    'united': 'united',
    'united_health_care': 'united',
    
    # Carpetas que van a tablas especiales
    'bluecross_application': 'bluecross_application',
    'florida_blue_aligned': 'florida_blue_aligned',
    'sherpa': 'sherpa',
    'tld': 'tld',
}


# Mapeo de columnas por carrier para tabla silver.policies
# Formato: {silver_column: source_column}
CARRIER_MAPPINGS: Dict[str, Dict[str, Any]] = {
    'ambetter': {
        'policy_id': 'Policy Number',
        'exchange_id': 'Exchange Subscriber ID',
        'member_first_name': 'Insured First Name',
        'member_last_name': 'Insured Last Name',
        'member_dob': 'Member Date Of Birth',
        'member_phone': 'Member Phone Number',
        'member_email': 'Member Email',
        'member_state': 'State',
        'member_county': 'County',
        'effective_date': 'Policy Effective Date',
        'term_date': 'Policy Term Date',
        'paid_through_date': 'Paid Through Date',
        'premium': 'Monthly Premium Amount',
        'premium_responsibility': 'Member Responsibility',
        'on_off_exchange': 'On/Off Exchange',
        'autopay_status': 'Autopay',
        'member_count': 'Number of Members',
        'status': 'Eligible for Commission',
        'broker_effective_date': 'Broker Effective Date',
        'broker_term_date': 'Broker Term Date'
    },
    
    'amerihealth': {
        'policy_id': 'Issuer Assigned ID',
        'exchange_id': 'Exchange Assigned ID',
        'member_full_name': 'Member Name',
        'member_dob': 'DOB',
        'member_phone': 'Primary Contact',
        'member_address': 'Mailing Address',
        'member_state': 'State',
        'member_zip': 'Zip',
        'member_county': 'County',
        'effective_date': 'Plan Effective Date',
        'paid_through_date': 'Paid Through Date',
        'premium': 'Monthly Premium',
        'status': 'Account Status',
        'payment_status': 'Payment Status',
        'autopay_status': 'AutoPay Status',
        'member_count': 'Household Size',
        'member_type': 'Relationship',
    },
    
    'aetna': {
        # Mapeo por defecto para Aetna (formato current)
        'policy_id': 'Issuer Assigned ID',
        'exchange_id': 'Exchange Assigned ID',
        'member_full_name': 'Member Name',
        'member_dob': 'DOB',
        'member_phone': 'Primary Contact',
        'member_address': 'Mailing Address',
        'member_state': 'State',
        'member_zip': 'Zip',
        'member_county': 'County',
        'effective_date': 'Plan Effective Date',
        'paid_through_date': 'Paid Through Date',
        'premium': 'Monthly Premium',
        'status': 'Account Status',
        'payment_status': 'Payment Status',
        'autopay_status': 'AutoPay Status',
        'member_count': 'Household Size',
        'member_type': 'Relationship',
    },
    
    'anthem': {
        'policy_id': 'Client ID',
        'member_full_name': 'Client Name',
        'member_state': 'State',
        'effective_date': 'Effective Date',
        'term_date': 'Cancellation Date',
        'status': 'Status',
        'payment_status': 'Bill Status',
        'on_off_exchange': 'Exchange',
        'member_count': 'Group Size',
    },
    
    'bluecross': {
        'policy_id': 'Account Number',
        'exchange_id': 'Exchange ID',
        'member_first_name': 'First Name',
        'member_last_name': 'Last Name',
        'member_dob': 'Date of Birth',
        'member_phone': "Client's Primary Phone",
        'member_email': 'Email',
        'member_address': 'Client Address 1',
        'member_address_2': 'Client Address 2',
        'member_city': 'City',
        'member_state': 'State',
        'member_zip': 'Zip Code',
        'effective_date': 'Coverage Effective Date',
        'term_date': 'Termed',
        'paid_through_date': 'Paid To Date',
        'status': 'Status',
        'on_off_exchange': 'Product Type',
        'member_count': 'Member Count',
        'member_type': 'Record Type',
    },
    
    'bluecross_application': {
        # Mismo esquema que bluecross policy pero sin DOB, Member Count,
        # Paid To Date, Termed, APTC ni Renewal Indicator (no existen en application)
        'policy_id': 'Account Number',
        'exchange_id': 'Exchange ID',
        'member_first_name': 'First Name',
        'member_last_name': 'Last Name',
        'member_phone': "Client's Primary Phone",
        'member_email': 'Email',
        'member_address': 'Client Address 1',
        'member_address_2': 'Client Address 2',
        'member_city': 'City',
        'member_state': 'State',
        'member_zip': 'Zip Code',
        'effective_date': 'Coverage Effective Date',
        'status': 'Status',
        'on_off_exchange': 'Product Type',
        'member_type': 'Record Type',
        '_e_app_number': 'E-App Number',
        '_plan_name': 'Plan Name',
        '_source': 'Source',
        '_producer_name': 'Producer Name',
        '_producer_number': 'Nine Digit Producer Number',
    },
    
    'cigna': {
        'policy_id': 'Subscriber ID (Detail Case #)',
        'exchange_id': 'Application Id',
        'member_first_name': 'Primary First Name',
        'member_last_name': 'Primary Last Name',
        'member_phone': 'Customer Phone Number',
        'member_email': 'Customer Email Address',
        'member_state': 'State',
        'effective_date': 'Effective Date',
        'term_date': 'Termination Date',
        'paid_through_date': 'Paid Through Date',
        'premium': 'Total Premium',
        'premium_responsibility': 'Premium - Customer Responsibility',
        'status': 'Policy Status',
        'on_off_exchange': 'ON/OFF Exchange',
    },
    
    'cigna_pending': {
        'policy_id': 'Customer Number (Case ID)',
        'exchange_id': 'Application ID',
        'member_full_name': 'Primary Name',
        'member_state': 'State',
        'status': 'Policy Status',
        '_agent_npn': 'Agent NPN',
        '_agent_name': 'Agent Name',
        '_producer_code': 'Producer Code',
        '_received_date': 'Received Date',
    },
    
    'community': {
        'policy_id': 'Issuer Assigned ID',
        'exchange_id': 'Exchange Assigned ID',
        'member_full_name': 'Member Name',
        'member_dob': 'DOB',
        'member_phone': 'Primary Contact',
        'member_address': 'Mailing Address',
        'member_state': 'State',
        'member_zip': 'Zip',
        'member_county': 'County',
        'effective_date': 'Plan Effective Date',
        'paid_through_date': 'Paid Through Date',
        'premium': 'Monthly Premium',
        'status': 'Account Status',
        'payment_status': 'Payment Status',
        'autopay_status': 'AutoPay Status',
        'member_count': 'Household Size',
        'member_type': 'Relationship',
    },
    
    'floridablue': {
        'policy_id': 'HCC_ID',
        'exchange_id': 'MEMBER_CIP_ID',
        'member_first_name': 'MEMBER_FIRST_NM',
        'member_last_name': 'MEMBER_LAST_NM',
        'member_dob': 'MEMBER_DOB',
        'member_phone': 'MEMBER_HOME_PHN',
        'member_email': 'MEMBER_EMAIL_ADDRESS',
        'member_county': 'MEMBER_COUNTY_NM',
        'on_off_exchange': 'EXCHANGE_IND',
        'member_count': 'ACTIVE_MEMBER_COUNT',
    },
    
    'molina': {
        'policy_id': 'Subscriber_ID',
        'exchange_id': 'HIX_ID',
        'member_first_name': 'Member_First_Name',
        'member_last_name': 'Member_Last_Name',
        'member_dob': 'dob',
        'member_phone': 'Member_Bussiness_Phone',
        'member_address': 'Address1',
        'member_address_2': 'Address2',
        'member_city': 'City',
        'member_state': 'State',
        'member_zip': 'Zip',
        'effective_date': 'Effective_date',
        'term_date': 'End_Date',
        'paid_through_date': 'Paid_Through_Date',
        'premium': 'Total_Premium',
        'premium_responsibility': 'Member_Premium',
        'status': 'Status',
        'member_count': 'Member_Count',
        'member_type': 'Relationship',
    },
    
    'oscar': {
        'policy_id': 'Member ID',
        'member_full_name': 'Member name',
        'member_dob': 'Date of birth',
        'member_phone': 'Phone number',
        'member_email': 'Email',
        'member_address': 'Mailing address',
        'member_state': 'State',
        'effective_date': 'Coverage start date',
        'term_date': 'Coverage end date',
        'premium_responsibility': 'Premium amount',
        'status': 'Policy status',
        'on_off_exchange': 'On exchange',
        'autopay_status': 'Autopay',
        'member_count': 'Lives',
    },
    
    'united': {
        'policy_id': 'policyId',
        'exchange_id': 'IFP - FFM APP ID',
        'member_first_name': 'memberFirstName',
        'member_last_name': 'memberLastName',
        'member_dob': 'dateOfBirth',
        'member_phone': 'memberPhone',
        'member_email': 'memberEmail',
        'member_address': 'memberAddress1',
        'member_address_2': 'memberAddress2',
        'member_city': 'memberCity',
        'member_state': 'memberState',
        'member_zip': 'memberZip',
        'member_county': 'memberCounty',
        'effective_date': 'policyEffectiveDate',
        'term_date': 'policyTermDate',
        'status': 'planStatus',
        'skip_rows': 2,
    },
}


# Formatos especiales de FloridaBlue
# Incluye formato legacy (2025) y formatos actuales (2026+)
FLORIDABLUE_FORMATS: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # FORMATO LEGACY (2025 y anteriores)
    # Estructura: Agency Name, Agent Name, Agency ID, Contract ID, etc.
    # Este formato tiene TODOS los datos en un solo archivo (no requiere enriquecimiento)
    # =========================================================================
    'legacy': {
        'detect_columns': ['Agency Name', 'Agent Name', 'Contract ID', 'Member FB_UID'],
        'requires_enrichment': False,  # Este formato ya tiene toda la información
        'mapping': {
            'policy_id': 'Contract ID',
            'exchange_id': 'Member FB_UID',
            'member_full_name': 'Member Full Name',
            'member_county': 'County Name',
            'effective_date': 'Coverage Effective Date',
            'term_date': None,  # No existe en formato legacy
            'premium': 'Premium Amount',
            'status': 'Contract Status',
            'member_count': 'Contract Member Count',
            'on_off_exchange': 'Product Type',
            'member_type': 'Member Relationship',
            # Extra fields específicos del formato legacy
            '_plan_name': 'Plan Name',
            '_metal_level': 'Metal Level',
            '_agent_name': 'Agent Name',
            '_agency_name': 'Agency Name',
            '_agency_id': 'Agency ID',
            '_carrier_name': 'Carrier Name',
            '_aor': 'AOR',
            '_product': 'Product',
            '_agent_contract_start_date': 'Agent Contract Start Date',
            '_mws_registration': 'MWS Registration',
            '_agent_status': 'Agent Status',
            '_member_age_in_indicator': 'Member Age-In Indicator',
            '_product_id': 'Product ID',
            '_member_original_effective_date': 'Member Original Effective Date',
        }
    },
    
    # =========================================================================
    # FORMATO COMPLETE (alternativo legacy con nombres diferentes)
    # Similar a legacy pero con nombres de columnas ligeramente diferentes
    # =========================================================================
    'complete': {
        'detect_columns': ['Agency_Name', 'Premium_Amount'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'Contract_ID',
            'exchange_id': 'Member_FB_UID',
            'member_full_name': 'Member_Full_Name',
            'member_county': 'County_Name',
            'effective_date': 'Coverage_Effective_Date',
            'premium': 'Premium_Amount',
            'status': 'Contract_Status',
            'member_count': 'Contract_Member_Count',
            'on_off_exchange': 'Product_Type',
            '_plan_name': 'Plan_Name',
            '_metal_level': 'Metal_Level',
            '_agent_name': 'Agent_Name',
            '_agency_name': 'Agency_Name',
        }
    },
    
    # =========================================================================
    # FORMATO BASIC (2026+)
    # Estructura actual con MEMBER_FULL_NAME
    # Requiere enriquecimiento con archivo aligned para DOB, email, phone, etc.
    # =========================================================================
    'basic': {
        'detect_columns': ['MEMBER_FULL_NAME', 'AGENCY_AOR', 'INSPOLICY_ID'],
        'requires_enrichment': True,  # Requiere archivo aligned para datos adicionales
        'mapping': {
            'policy_id': 'HCC_ID',
            'exchange_id': 'MEMBER_CIP_ID',
            'member_full_name': 'MEMBER_FULL_NAME',
            'member_county': 'MEMBER_COUNTY_NM',
            'effective_date': 'CONTRACT_EFCV_DT',
            'term_date': 'CONTRACT_EPRN_DT',
            'member_type': 'IS_SUBSCRIBER',
            '_segment': 'SEGMENT',
            '_product_name': 'PRODUCT_NAME',
            '_plan_type': 'PLAN_TYPE',
            '_agent_full_name': 'AGENT_FULL_NM',
            '_agency_aor': 'AGENCY_AOR',
            '_agent_aor': 'AGENT_AOR',
            '_aor_contract_alignment_efcv_dt': 'AOR_CONTRACT_ALIGNMENT_EFCV_DT',
            '_aor_contract_alignment_eprn_dt': 'AOR_CONTRACT_ALIGNMENT_EPRN_DT',
            '_inspolicy_id': 'INSPOLICY_ID',
            '_product_category_type_cd': 'PRODUCT_CATEGORY_TYPE_CD',
            '_source_system_cd': 'SOURCE_SYSTEM_CD',
        }
    },
    
    # =========================================================================
    # FORMATO ALIGNED (2026+)
    # Archivo complementario que contiene DOB, email, phone, member_count
    # Se usa para enriquecer los datos del formato basic
    # =========================================================================
    'aligned': {
        'detect_columns': ['AGENCY_AOR', 'AGENT_AOR', 'HCC_ID'],
        'detect_not_columns': ['MEMBER_FULL_NAME'],  # Si no tiene MEMBER_FULL_NAME es aligned
        'requires_enrichment': False,  # Este ES el archivo de enriquecimiento
        'is_enrichment_source': True,  # Indica que este formato provee datos de enriquecimiento
        'mapping': {
            'policy_id': 'HCC_ID',
            'exchange_id': 'MEMBER_CIP_ID',
            'member_full_name': 'MEMBER_FULL_NAME',
            'member_dob': 'MEMBER_DOB',
            'member_email': 'MEMBER_EMAIL_ADDRESS',
            'member_phone': 'MEMBER_HOME_PHN',
            'member_county': 'MEMBER_COUNTY_NM',
            'effective_date': 'CONTRACT_EFCV_DT',
            'term_date': 'CONTRACT_EPRN_DT',
            'member_type': 'CODE_DESC',
            'member_count': 'ACTIVE_MEMBER_COUNT',
            '_segment': 'SEGMENT',
            '_product_name': 'PRODUCT_NAME',
        }
    },
}


# =============================================================================
# AETNA FORMATS
# Formato legacy (2025): 12 columnas con First Name/Last Name separados
# Formato current (2025-11+): 20 columnas con Member Name completo
# =============================================================================
AETNA_FORMATS: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # FORMATO LEGACY (2025)
    # Estructura: First Name, Last Name separados, sin DOB ni premium
    # =========================================================================
    'legacy': {
        'detect_columns': ['First Name', 'Last Name', 'Policy Status', 'Issuer Assigned ID'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'Issuer Assigned ID',
            'exchange_id': None,
            'member_first_name': 'First Name',
            'member_last_name': 'Last Name',
            'member_dob': None,
            'member_email': None,
            'member_phone': None,
            'member_county': None,
            'member_state': None,
            'effective_date': 'Effective Date',
            'term_date': 'Paid Through Date',
            'status': 'Policy Status',
            'payment_status': 'Financial Status',
            'premium': None,
            'member_type': 'Relationship',
            'on_off_exchange': 'Marketplace',
            '_plan_id': 'Plan ID',
            '_metal_tier': 'Metal Tier',
            '_subscriber_status': 'Subscriber Status',
        }
    },
    
    # =========================================================================
    # FORMATO CURRENT (2025-11+)
    # Estructura: Member Name completo, con DOB, premium, phone, county
    # =========================================================================
    'current': {
        'detect_columns': ['Member Name', 'DOB', 'Exchange Assigned ID', 'Monthly Premium'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'Issuer Assigned ID',
            'exchange_id': 'Exchange Assigned ID',
            'member_full_name': 'Member Name',
            'member_dob': 'DOB',
            'member_email': None,
            'member_phone': 'Primary Contact',
            'member_county': 'County',
            'member_state': 'State',
            'effective_date': 'Plan Effective Date',
            'term_date': 'Paid Through Date',
            'status': 'Account Status',
            'payment_status': 'Payment Status',
            'premium': 'Monthly Premium',
            'member_type': 'Relationship',
            'on_off_exchange': None,
            'autopay_status': 'AutoPay Status',
            'member_count': 'Household Size',
            '_plan_id': 'Member Plan ID',
            '_plan_name': 'Member Plan Name',
            '_metal_tier': 'Metal Tier',
            '_mailing_address': 'Mailing Address',
            '_zip': 'Zip',
        }
    },
}


# =============================================================================
# AMERIHEALTH FORMATS
# Formato legacy (2025): 11 columnas sin DOB ni premium
# Formato current (2026+): 20 columnas con DOB y premium
# =============================================================================
AMERIHEALTH_FORMATS: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # FORMATO LEGACY (2025)
    # Estructura: Member Name completo, sin DOB ni premium
    # =========================================================================
    'legacy': {
        'detect_columns': ['Member Name', 'Relationship', 'Plan ID', 'Marketplace'],
        'detect_not_columns': ['DOB', 'Exchange Assigned ID', 'Monthly Premium'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'Issuer Assigned ID',
            'exchange_id': None,
            'member_full_name': 'Member Name',
            'member_dob': None,
            'member_email': None,
            'member_phone': None,
            'member_county': None,
            'member_state': None,
            'effective_date': 'Effective Date',
            'term_date': 'Paid Through Date',
            'status': 'Subscriber Status',
            'payment_status': 'Financial Status',
            'premium': None,
            'member_type': 'Relationship',
            'on_off_exchange': 'Marketplace',
            '_plan_id': 'Plan ID',
            '_metal_tier': 'Metal Tier',
            '_autopay_status': 'Autopay Status',
        }
    },
    
    # =========================================================================
    # FORMATO CURRENT (2026+)
    # Estructura: Member Name completo, con DOB, premium, phone, county
    # =========================================================================
    'current': {
        'detect_columns': ['Member Name', 'DOB', 'Exchange Assigned ID', 'Monthly Premium'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'Issuer Assigned ID',
            'exchange_id': 'Exchange Assigned ID',
            'member_full_name': 'Member Name',
            'member_dob': 'DOB',
            'member_email': None,
            'member_phone': 'Primary Contact',
            'member_county': 'County',
            'member_state': 'State',
            'effective_date': 'Plan Effective Date',
            'term_date': 'Paid Through Date',
            'status': 'Account Status',
            'payment_status': 'Payment Status',
            'premium': 'Monthly Premium',
            'member_type': 'Relationship',
            'on_off_exchange': None,
            'autopay_status': 'AutoPay Status',
            'member_count': 'Household Size',
            '_plan_id': 'Member Plan ID',
            '_plan_name': 'Member Plan Name',
            '_metal_tier': 'Metal Tier',
            '_mailing_address': 'Mailing Address',
            '_zip': 'Zip',
        }
    },
}

# =============================================================================
# COMMUNITY FORMATS
# Formato current: archivos v6 con headers camelCase como memberFirstName, memberDOB, etc.
# Formato legacy: archivos v1-v5 con headers simples como memberID, firstName, etc.
# =============================================================================
COMMUNITY_FORMATS: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # FORMATO CURRENT (v6 - Enero 2026+)
    # Estructura camelCase con memberFirstName, memberDOB, exchangeMemberID, etc.
    # Este es el formato actual con 30 columnas completas
    # =========================================================================
    'current': {
        'detect_columns': ['memberFirstName', 'memberLastName', 'memberDOB', 'exchangeMemberID'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'issuerMemberID',  # ID único del miembro
            'exchange_id': 'exchangeMemberID',  # ID del exchange
            'member_first_name': 'memberFirstName',
            'member_last_name': 'memberLastName',
            'member_dob': 'memberDOB',
            'member_phone': 'primaryContact',
            'member_address': 'mailingAddress',
            'member_city': 'city',
            'member_state': 'subscriberState',
            'member_zip': 'subscriberZip', 
            'member_county': 'subscriberCounty',
            'member_type': 'memberRelationship',
            'effective_date': 'memberPlanEffectiveDate',
            'term_date': 'memberPlanEndDate',
            'paid_through_date': 'paidThroughDate',
            'premium': 'memberPlanMonthlyPremium',
            'status': 'subscriberSubscriberStatus',  # Active/Inactive/Suspended
            'payment_status': 'subscriberFinanceStatus',  # Paid/Late/Past Due
            'autopay_status': 'autoPay',
            'member_count': 'householdSize',
            'on_off_exchange': 'marketplace',
            # Campos adicionales específicos de Community
            '_plan_id': 'memberPlanID',
            '_plan_name': 'planName',
            '_broker_name': 'brokerName',
            '_broker_policy_status': 'brokerPolicyStatus',
            '_member_folder_creation_date': 'memberFolderCreationDate',
            '_member_creation_date': 'memberCreationDate',
            '_effective_date_alt': 'effectiveDate',  # Campo alternativo de fecha efectiva
            '_exchange_subscriber_id': 'exchangeSubscriberID',
            '_issuer_subscriber_id': 'issuerSubscriberID',
        }
    },
    
    # =========================================================================
    # FORMATO LEGACY (v1-v5 - Hasta Enero 2026)
    # Estructura simple con memberID, firstName, lastName, phoneNumber
    # Para compatibilidad con datos históricos
    # =========================================================================
    'legacy': {
        'detect_columns': ['memberID', 'firstName', 'lastName', 'phoneNumber'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'memberID',
            'member_first_name': 'firstName',
            'member_last_name': 'lastName',
            'member_phone': 'phoneNumber',
            # Campos mínimos para datos históricos
        }
    },
}


# =============================================================================
# UNITED FORMATS
# Formato correct: archivos con headers originales como memberFirstName, memberLastName, etc.
# Formato transformed: archivos procesados con headers como Policy Number, Insured First Name, etc.
# =============================================================================
UNITED_FORMATS: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # FORMATO CORRECT
    # Estructura original con memberFirstName, memberLastName, dateOfBirth, memberNumber
    # =========================================================================
    'correct': {
        'detect_columns': ['memberFirstName', 'memberLastName', 'dateOfBirth', 'memberNumber'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'memberNumber',
            'exchange_id': 'IFP - FFM APP ID',
            'member_first_name': 'memberFirstName',
            'member_last_name': 'memberLastName',
            'member_dob': 'dateOfBirth',
            'member_phone': 'memberPhone',
            'member_email': 'memberEmail',
            'member_address': 'memberAddress1',
            'member_address_2': 'memberAddress2',
            'member_city': 'memberCity',
            'member_state': 'memberState',
            'member_zip': 'memberZip',
            'member_county': 'memberCounty',
            'effective_date': 'policyEffectiveDate',
            'term_date': 'policyTermDate',
            'paid_through_date': 'paidThroughDate',
            'premium': 'monthlyPremiumAmount',
            'premium_responsibility': 'memberResponsibility',
            'status': 'planStatus',
            'on_off_exchange': 'onOffExchange',
            'autopay_status': 'autopayStatus',
            'member_count': 'memberCount',
        }
    },

    

    # =========================================================================
    # FORMATO NORMALIZED
    # Estructura estandarizada/normalizada (generada por BetterClean u otros sistemas)
    # =========================================================================
    'normalized': {
        'detect_columns': ['agentId', 'agentIdStatus', 'agentName', 'memberFirstName'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'agentId',
            'exchange_id': 'IFP___FFM_APP_ID',
            'member_first_name': 'memberFirstName',
            'member_last_name': 'memberLastName',
            'member_dob': 'dateOfBirth',
            'member_phone': 'memberPhone',
            'member_email': 'memberEmail',
            'member_address': 'memberAddress1',
            'member_address_2': 'memberAddress2',
            'member_city': 'memberCity',
            'member_state': 'memberState',
            'member_zip': 'memberZip',
            'member_county': 'memberCounty',
            'effective_date': 'policyEffectiveDate',
            'term_date': 'policyTermDate',
            'paid_through_date': 'paidThroughDate',
            'premium': 'monthlyPremiumAmount',
            'premium_responsibility': 'memberResponsibility',
            'status': 'planStatus',
            'on_off_exchange': 'onOffExchange',
            'autopay_status': 'autopayStatus',
            'member_count': 'memberCount',
        }
    },
    
    # =========================================================================
    # FORMATO TRANSFORMED
    # Estructura procesada con Policy Number, Insured First Name, etc.
    # =========================================================================
    'transformed': {
        'detect_columns': ['Policy Number', 'Exchange Subscriber ID', 'Insured First Name', 'Insured Last Name'],
        'requires_enrichment': False,
        'mapping': {
            'policy_id': 'Policy Number',
            'exchange_id': 'Exchange Subscriber ID',
            'member_first_name': 'Insured First Name',
            'member_last_name': 'Insured Last Name',
            'member_dob': 'Date Of Birth',
            'member_phone': 'Phone Number',
            'member_email': 'Email Address',
            'member_address': 'Address Line 1',
            'member_address_2': 'Address Line 2',
            'member_city': 'City',
            'member_state': 'State',
            'member_zip': 'Zip Code',
            'member_county': 'County',
            'effective_date': 'Effective Date',
            'term_date': 'Termination Date',
            'paid_through_date': 'Paid Through Date',
            'premium': 'Premium Amount',
            'premium_responsibility': 'Member Premium Responsibility',
            'status': 'Policy Status',
            'on_off_exchange': 'On Off Exchange',
            'autopay_status': 'Auto Pay Status',
            'member_count': 'Member Count',
        }
    },
}
# =============================================================================
# ANTHEM FORMATS
# standard: CSV comma-sep, latin-1, skiprows=1 (tiene fila título), 27 columnas
# extended: TSV tab-sep, UTF-16 LE, skiprows=0, 42 columnas (DOB, phone, address)
# =============================================================================
ANTHEM_FORMATS: Dict[str, Dict[str, Any]] = {
    'standard': {
        'detect_columns': ['Client Name', 'Client ID', 'Bill Status', 'Funding Type'],
        'mapping': {
            'policy_id': 'Client ID',
            'member_full_name': 'Client Name',
            'member_state': 'State',
            'effective_date': 'Effective Date',
            'term_date': 'Cancellation Date',
            'status': 'Status',
            'payment_status': 'Bill Status',
            'on_off_exchange': 'Exchange',
            'member_count': 'Group Size',
            '_market': 'Market',
            '_plan_name': 'Plan Name',
            '_new_business': 'New Business',
            '_bill_due_date': 'Bill Due Date',
            '_funding_type': 'Funding Type',
        }
    },
    'extended': {
        'detect_columns': ['Client ID', 'Birth Date', 'Phone #', 'Home Address 1'],
        'mapping': {
            'policy_id': 'Client ID',
            'member_full_name': 'Client Name',
            'member_dob': 'Birth Date',
            'member_phone': 'Phone #',
            'member_address': 'Home Address 1',
            'member_address_2': 'Home Address 2',
            'member_city': 'Home City',
            'member_state': 'Home State Code',
            'member_zip': 'Home Zip Code',
            'effective_date': 'Effective Date',
            'term_date': 'Cancellation Date',
            'status': 'Status',
            'payment_status': 'Bill Status',
            'on_off_exchange': 'Exchange',
            'member_count': 'Group Size',
            '_status_reason': 'Status Reason',
            '_emo_app_id': 'Emo App Id',
            '_mailing_address_1': 'Mailing Address 1',
            '_mailing_city': 'Mailing City',
            '_mailing_zip': 'Mailing Zip Code',
        }
    },
}
# =============================================================================
# LOGICA DE COMISIONABILIDAD
# =============================================================================
# Define como determinar si una poliza es comisionable basandose en status + payment_status

COMMISSION_RULES: Dict[str, Dict[str, Any]] = {
    'amerihealth': {
        # Account Status + Payment Status
        'commissionable': {
            'status': ['Active'],
            'payment_status': ['Paid'],
        },
        'follow_up': {
            'status': ['Active'],
            'payment_status': ['Binder Due', 'Late', 'Past Due'],
        },
        'terminated': {
            'status': ['Inactive', 'Terminated', 'Cancelled'],
        },
    },
    
    'aetna': {
        # Account Status + Payment Status
        'commissionable': {
            'status': ['Active'],
            'payment_status': ['Paid', 'Paid Through'],
        },
        'follow_up': {
            'status': ['Active'],
            'payment_status': ['Binder Due', 'Late', 'Past Due'],
        },
        'terminated': {
            'status': ['Inactive', 'Terminated', 'Cancelled'],
        },
    },
    
    'anthem': {
        # Status + Bill Status
        'commissionable': {
            'status': ['Active'],
            'payment_status': ['Paid'],
        },
        'follow_up': {
            'status': ['Active'],
            'payment_status_not': ['Paid'],  # Active pero bill status != Paid
        },
        'terminated': {
            'status': ['Inactive'],
        },
        'pending': {
            'status': ['Future Active', 'Pending Effectuation'],
        },
    },
    
    'community': {
        # Account Status + Payment Status (misma estructura que AmeriHealth)
        'commissionable': {
            'status': ['Active'],
            'payment_status': ['Paid'],
        },
        'follow_up': {
            'status': ['Active'],
            'payment_status': ['Binder Due', 'Late', 'Past Due'],
        },
        'terminated': {
            'status': ['Inactive', 'Terminated', 'Cancelled'],
        },
    },
}


def get_carrier_from_folder(folder_name: str) -> str:
    """Obtiene el nombre del carrier normalizado desde el nombre de carpeta."""
    return FOLDER_TO_CARRIER.get(folder_name.lower(), 'unknown')


def get_carrier_mapping(carrier: str) -> Dict[str, Any]:
    """Obtiene el mapeo de columnas para un carrier especifico."""
    return CARRIER_MAPPINGS.get(carrier, {})


def is_special_carrier(carrier: str) -> bool:
    """Verifica si el carrier va a una tabla especial (no policies)."""
    special_carriers = {'bluecross_application', 'florida_blue_aligned', 'sherpa', 'tld'}
    return carrier in special_carriers


def get_commission_rules(carrier: str) -> Dict[str, Any]:
    """Obtiene las reglas de comisionabilidad para un carrier."""
    return COMMISSION_RULES.get(carrier, {})


def calculate_commission_status(carrier: str, status: str, payment_status: str) -> str:
    """
    Calcula el estado de comisionabilidad basandose en status y payment_status.
    
    Returns:
        - 'Active - Commissionable': Active + Paid
        - 'Active - Follow Up': Active + otro payment status
        - 'Terminated': Inactive/Terminated/Cancelled
        - 'Pending': Future Active / Pending Effectuation
        - El status original si no hay reglas definidas
    """
    rules = get_commission_rules(carrier)
    if not rules:
        return status  # Sin reglas, retornar status original
    
    status_upper = (status or '').strip().upper()
    payment_upper = (payment_status or '').strip().upper()
    
    # Verificar si es comisionable
    if 'commissionable' in rules:
        rule = rules['commissionable']
        status_match = any(s.upper() in status_upper for s in rule.get('status', []))
        payment_match = any(p.upper() == payment_upper for p in rule.get('payment_status', []))
        if status_match and payment_match:
            return 'Active - Commissionable'
    
    # Verificar si es follow up
    if 'follow_up' in rules:
        rule = rules['follow_up']
        status_match = any(s.upper() in status_upper for s in rule.get('status', []))
        
        if 'payment_status' in rule:
            payment_match = any(p.upper() == payment_upper for p in rule['payment_status'])
        elif 'payment_status_not' in rule:
            payment_match = not any(p.upper() == payment_upper for p in rule['payment_status_not'])
        else:
            payment_match = True
            
        if status_match and payment_match:
            return 'Active - Follow Up'
    
    # Verificar si es terminated
    if 'terminated' in rules:
        rule = rules['terminated']
        if any(s.upper() in status_upper for s in rule.get('status', [])):
            return 'Terminated'
    
    # Verificar si es pending
    if 'pending' in rules:
        rule = rules['pending']
        if any(s.upper() in status_upper for s in rule.get('status', [])):
            return 'Pending'
    
    return status  # Retornar status original si no hay match