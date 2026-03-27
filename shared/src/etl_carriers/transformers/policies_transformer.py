"""
Transformers para convertir datos de Bronze a Silver.

🔧 CHANGELOG:
- 2026-02-27: Implementada solución para problema Ambetter ↔ United
  - Agregada detect_carrier_format_improved() con validación por carrier
  - Mejorada detect_united_format() con validación anti-Ambetter
  - Agregada get_mapping_for_carrier_format() para mapeo correcto
  - Modificada transform_row() con nueva lógica de detección
- 2026-03-09: Fix Anthem — soporte formatos standard y extended
  - Agregada _detect_anthem_format_strict() con limpieza de BOM corrupto
  - Agregado _strip_bom_artifacts() para headers UTF-16 con BOM corrupto
  - Anthem agregado a _MULTI_FORMAT_CARRIERS
  - transform_dataframe() precomputa mapping UNA vez por archivo (fix performance)
  - transform_row() acepta precomputed_mapping para evitar detección por fila
"""

import pandas as pd
from typing import Dict, List, Optional, Any, Set
from abc import ABC, abstractmethod

from etl_carriers.config import (
    CARRIER_MAPPINGS,
    ANTHEM_FORMATS,
    FLORIDABLUE_FORMATS,
    AETNA_FORMATS,
    AMERIHEALTH_FORMATS,
    UNITED_FORMATS,
    COMMUNITY_FORMATS,
    get_carrier_mapping,
)
from etl_carriers.utils import (
    clean_cigna_application_id,
    normalize_member_type,
    parse_date,
    clean_phone,
    split_full_name,
    combine_names_and_split,
    extract_state_from_county,
    extract_county_name,
    normalize_floridablue_status,
    FileMetadata,
)


class BaseTransformer(ABC):
    """Clase base para transformadores Bronze -> Silver."""

    @abstractmethod
    def transform_row(self, row: pd.Series, metadata: FileMetadata) -> Dict[str, Any]:
        """Transforma una fila de Bronze a formato Silver."""
        pass

    def get_value(self, row: pd.Series, mapping: Dict, key: str, default=None) -> Any:
        """Obtiene un valor del row usando el mapeo."""
        if key not in mapping:
            return default

        col_name = mapping[key]
        if col_name in row.index:
            value = row[col_name]
            if pd.isna(value):
                return default
            return value
        return default


class PoliciesTransformer(BaseTransformer):
    """Transformador para la tabla silver.policies."""

    def __init__(self):
        self.carrier_mappings = CARRIER_MAPPINGS
        self.floridablue_formats = FLORIDABLUE_FORMATS
        self.united_formats = UNITED_FORMATS
        self.community_formats = COMMUNITY_FORMATS

    # =========================================================================
    # DETECCIÓN DE FORMATO
    # =========================================================================

    def detect_carrier_format_improved(self, df: pd.DataFrame, carrier: str) -> Optional[str]:
        """
        Detecta el formato del archivo basándose en el carrier real.

        Soluciona el problema donde Ambetter se detectaba como United 'transformed'
        porque comparten las mismas columnas de detección.

        Args:
            df: DataFrame con los datos
            carrier: Carrier detectado desde folder/filename

        Returns:
            str: Formato detectado, o None si no se reconoce
        """
        columns = set(df.columns.tolist())

        # PASO 1: CARRIERS CON MAPEO ÚNICO (direct_mapping)
        # -------------------------------------------------
        if carrier in self.carrier_mappings:
            expected_columns = set(self.carrier_mappings[carrier].values())
            expected_columns = {
                col for col in expected_columns
                if isinstance(col, str) and not col.startswith('_') and col != 'skip_rows'
            }
            matching_columns = expected_columns.intersection(columns)
            column_match_ratio = len(matching_columns) / len(expected_columns) if expected_columns else 0

            # Caso especial Ambetter: validación anti-United
            if carrier == 'ambetter':
                ambetter_unique_cols = {'Eligible for Commission', 'Member Date Of Birth', 'Monthly Premium Amount'}
                united_unique_cols = {'Policy Status', 'Premium Amount', 'Auto Pay Status'}
                has_ambetter_cols = any(col in columns for col in ambetter_unique_cols)
                has_united_cols = any(col in columns for col in united_unique_cols)
                if has_ambetter_cols and not has_united_cols:
                    return 'direct_mapping'
                elif has_united_cols:
                    print(f"🚫 ADVERTENCIA: Archivo tiene columnas de United, no es Ambetter")
                    return None

            # Carriers con múltiples formatos siempre van al Paso 2
            _MULTI_FORMAT_CARRIERS = {'united', 'floridablue', 'community', 'aetna', 'amerihealth', 'anthem'}

            if column_match_ratio > 0.7 and carrier not in _MULTI_FORMAT_CARRIERS:
                return 'direct_mapping'

        # PASO 2: CARRIERS CON MÚLTIPLES FORMATOS
        # ----------------------------------------
        if carrier == 'united':
            return self._detect_united_format_strict(columns)
        elif carrier == 'community':
            return self._detect_community_format_strict(columns)
        elif carrier == 'floridablue':
            return self._detect_floridablue_format_strict(columns)
        elif carrier == 'anthem':
            return self._detect_anthem_format_strict(columns)
        elif carrier == 'aetna':
            return self._detect_aetna_format_strict(columns)
        elif carrier == 'amerihealth':
            return self._detect_amerihealth_format_strict(columns)

        print(f"⚠️ No se pudo detectar formato para carrier '{carrier}'")
        return None

    def _detect_united_format_strict(self, columns: Set[str]) -> Optional[str]:
        """Detección estricta de United con validación anti-Ambetter."""
        if 'correct' in self.united_formats:
            detect_cols = set(self.united_formats['correct']['detect_columns'])
            if detect_cols.issubset(columns):
                return 'correct'

        if 'transformed' in self.united_formats:
            detect_cols = set(self.united_formats['transformed']['detect_columns'])
            if detect_cols.issubset(columns):
                united_specific_cols = {'Premium Amount', 'Member Premium Responsibility', 'Auto Pay Status'}
                ambetter_specific_cols = {'Eligible for Commission', 'Member Date Of Birth', 'Monthly Premium Amount'}
                has_united_cols = any(col in columns for col in united_specific_cols)
                has_ambetter_cols = any(col in columns for col in ambetter_specific_cols)
                if has_united_cols and not has_ambetter_cols:
                    print(f"✅ United 'transformed' confirmado")
                    return 'transformed'
                elif has_ambetter_cols:
                    print(f"🚫 Archivo rechazado: tiene columnas de Ambetter, no es United")
                    return None
                else:
                    print(f"⚠️ United 'transformed' incierto - sin columnas distintivas")
                    return 'transformed'

        if 'normalized' in self.united_formats:
            detect_cols = set(self.united_formats['normalized']['detect_columns'])
            if detect_cols.issubset(columns):
                return 'normalized'

        return None

    def _detect_community_format_strict(self, columns: Set[str]) -> Optional[str]:
        """Detección estricta de formato Community."""
        for format_name in ['current', 'legacy']:
            if format_name in self.community_formats:
                detect_cols = set(self.community_formats[format_name]['detect_columns'])
                if detect_cols.issubset(columns):
                    return format_name
        return None

    def _detect_floridablue_format_strict(self, columns: Set[str]) -> Optional[str]:
        """Detección estricta de formato FloridaBlue."""
        for format_name in ['legacy', 'complete', 'basic', 'aligned']:
            if format_name in self.floridablue_formats:
                detect_cols = set(self.floridablue_formats[format_name]['detect_columns'])
                if detect_cols.issubset(columns):
                    return format_name
        return None

    def _detect_aetna_format_strict(self, columns: Set[str]) -> Optional[str]:
        """Detección estricta de formato Aetna."""
        for format_name in ['current', 'legacy']:
            if format_name in AETNA_FORMATS:
                detect_cols = set(AETNA_FORMATS[format_name]['detect_columns'])
                if detect_cols.issubset(columns):
                    return format_name
        return None

    def _detect_amerihealth_format_strict(self, columns: Set[str]) -> Optional[str]:
        """Detección estricta de formato AmeriHealth."""
        for format_name in ['current', 'legacy']:
            if format_name in AMERIHEALTH_FORMATS:
                detect_cols = set(AMERIHEALTH_FORMATS[format_name]['detect_columns'])
                if detect_cols.issubset(columns):
                    return format_name
        return None

    def _detect_anthem_format_strict(self, columns: Set[str]) -> Optional[str]:
        """
        Detección estricta de formato Anthem.

        Limpia prefijos BOM corruptos antes de comparar — el formato 'extended'
        (UTF-16 sin BOM estándar) puede tener artefactos Unicode en el primer header
        que ya fueron limpiados por gcs_loader, pero se limpia aquí también por
        robustez en caso de llamadas directas.
        """
        clean_columns = {self._strip_bom_artifacts(col) for col in columns}

        # extended: tiene columnas de dirección y DOB (42 cols, TSV, UTF-16)
        extended_detect = {'Client ID', 'Birth Date', 'Phone #', 'Home Address 1'}
        if extended_detect.issubset(clean_columns):
            return 'extended'

        # standard: columnas básicas sin datos personales extendidos (27 cols, CSV, latin-1)
        standard_detect = {'Client Name', 'Client ID', 'Bill Status', 'Funding Type'}
        if standard_detect.issubset(clean_columns):
            return 'standard'

        return None

    @staticmethod
    def _strip_bom_artifacts(col_name: str) -> str:
        """
        Elimina caracteres BOM y artefactos de reemplazo Unicode al inicio de un
        nombre de columna. Defensa en profundidad para archivos UTF-16 con BOM corrupto.
        """
        bom_chars = {'\ufeff', '\ufffd', '\ufffe', '뿯', '붿', '뷯', '뾽'}
        result = col_name
        while result and result[0] in bom_chars:
            result = result[1:]
        return result.strip()

    # =========================================================================
    # MAPEO POR CARRIER Y FORMATO
    # =========================================================================

    def get_mapping_for_carrier_format(self, carrier: str, format_detected: str) -> Dict[str, Any]:
        """
        Obtiene el mapeo correcto basándose en carrier y formato detectado.

        Args:
            carrier: Nombre del carrier
            format_detected: Formato detectado por detect_carrier_format_improved

        Returns:
            Dict con el mapeo de columnas correcto
        """
        if format_detected == 'direct_mapping':
            return self.carrier_mappings.get(carrier, {})

        if carrier == 'united' and format_detected in self.united_formats:
            return self.united_formats[format_detected]['mapping']
        elif carrier == 'anthem' and format_detected in ANTHEM_FORMATS:
            return ANTHEM_FORMATS[format_detected]['mapping']
        elif carrier == 'community' and format_detected in self.community_formats:
            return self.community_formats[format_detected]['mapping']
        elif carrier == 'floridablue' and format_detected in self.floridablue_formats:
            return self.floridablue_formats[format_detected]['mapping']
        elif carrier == 'aetna' and format_detected in AETNA_FORMATS:
            return AETNA_FORMATS[format_detected]['mapping']
        elif carrier == 'amerihealth' and format_detected in AMERIHEALTH_FORMATS:
            return AMERIHEALTH_FORMATS[format_detected]['mapping']

        # Fallback: usar carrier_mappings
        return self.carrier_mappings.get(carrier, {})

    # =========================================================================
    # HELPERS DE DETECCIÓN (interfaz pública legacy — delegan a los _strict)
    # =========================================================================

    def detect_floridablue_format(self, df: pd.DataFrame) -> Optional[str]:
        """Detecta el formato de FloridaBlue. Mantiene compatibilidad con código externo."""
        columns = set(df.columns.tolist())
        columns_lower = set(c.lower() for c in df.columns.tolist())

        if 'Agency Name' in columns and 'Contract ID' in columns and 'Member FB_UID' in columns:
            return 'legacy'
        if 'Agency_Name' in columns and 'Premium_Amount' in columns:
            return 'complete'
        if 'MEMBER_FULL_NAME' in columns and 'AGENCY_AOR' in columns:
            return 'basic'
        if 'AGENCY_AOR' in columns and 'HCC_ID' in columns and 'MEMBER_FULL_NAME' not in columns:
            return 'aligned'
        if 'agency name' in columns_lower and 'contract id' in columns_lower:
            return 'legacy'
        if 'agency_name' in columns_lower and 'premium_amount' in columns_lower:
            return 'complete'
        return None

    def format_requires_enrichment(self, format_type: str) -> bool:
        """Verifica si el formato requiere enriquecimiento con archivo aligned."""
        if format_type and format_type in self.floridablue_formats:
            return self.floridablue_formats[format_type].get('requires_enrichment', False)
        return False

    def get_floridablue_mapping(self, format_type: str) -> Dict[str, str]:
        """Retorna el mapeo correcto para el formato de FloridaBlue detectado."""
        if format_type and format_type in self.floridablue_formats:
            return self.floridablue_formats[format_type]['mapping']
        return self.carrier_mappings.get('floridablue', {})

    def detect_aetna_format(self, df: pd.DataFrame) -> Optional[str]:
        """Detecta el formato de Aetna. Mantiene compatibilidad con código externo."""
        return self._detect_aetna_format_strict(set(df.columns.tolist()))

    def detect_amerihealth_format(self, df: pd.DataFrame) -> Optional[str]:
        """Detecta el formato de AmeriHealth. Mantiene compatibilidad con código externo."""
        return self._detect_amerihealth_format_strict(set(df.columns.tolist()))

    def detect_united_format(self, df: pd.DataFrame) -> Optional[str]:
        """Detecta el formato de United. Mantiene compatibilidad con código externo."""
        return self._detect_united_format_strict(set(df.columns.tolist()))

    def detect_community_format(self, df: pd.DataFrame) -> Optional[str]:
        """Detecta el formato de Community. Mantiene compatibilidad con código externo."""
        return self._detect_community_format_strict(set(df.columns.tolist()))

    def get_united_mapping(self, format_type: str) -> Dict[str, str]:
        """Retorna el mapeo correcto para el formato de United detectado."""
        if format_type and format_type in self.united_formats:
            return self.united_formats[format_type]['mapping']
        return self.carrier_mappings.get('united', {})

    def get_community_mapping(self, format_type: str) -> Dict[str, str]:
        """Retorna el mapeo correcto para el formato de Community detectado."""
        if format_type and format_type in self.community_formats:
            return self.community_formats[format_type]['mapping']
        return self.carrier_mappings.get('community', {})

    def get_aetna_mapping(self, format_type: str) -> Dict[str, str]:
        """Retorna el mapeo correcto para el formato de Aetna detectado."""
        if format_type and format_type in AETNA_FORMATS:
            return AETNA_FORMATS[format_type]['mapping']
        return self.carrier_mappings.get('aetna', {})

    def get_amerihealth_mapping(self, format_type: str) -> Dict[str, str]:
        """Retorna el mapeo correcto para el formato de Amerihealth detectado."""
        if format_type and format_type in AMERIHEALTH_FORMATS:
            return AMERIHEALTH_FORMATS[format_type]['mapping']
        return self.carrier_mappings.get('amerihealth', {})

    # =========================================================================
    # TRANSFORMACIÓN
    # =========================================================================

    def transform_row(
        self,
        row: pd.Series,
        metadata: FileMetadata,
        floridablue_format: Optional[str] = None,
        aetna_format: Optional[str] = None,
        amerihealth_format: Optional[str] = None,
        united_format: Optional[str] = None,
        community_format: Optional[str] = None,
        precomputed_mapping: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Transforma una fila de Bronze a formato Silver para policies.

        Args:
            row: Fila del DataFrame de Bronze
            metadata: Metadatos del archivo (carrier, aor_id, etc.)
            precomputed_mapping: Mapeo pre-calculado por transform_dataframe.
                Si se proporciona, se omite la detección de formato (más eficiente).
                Si es None, se detecta el formato a partir de la fila (para llamadas directas).
        """
        carrier = metadata.carrier

        # Usar mapeo pre-calculado si está disponible (caso normal desde transform_dataframe)
        # o detectar desde la fila individual (caso de llamadas directas / tests)
        if precomputed_mapping is not None:
            mapping = precomputed_mapping
        else:
            format_detected = self.detect_carrier_format_improved(
                pd.DataFrame([row], columns=row.index),
                carrier
            )
            if not format_detected:
                print(f"⚠️ No se pudo detectar formato para {carrier} - usando mapeo base")
                mapping = self.carrier_mappings.get(carrier, {})
            else:
                mapping = self.get_mapping_for_carrier_format(carrier, format_detected)

            # Validaciones de integridad solo en detección por fila (el precomputed ya es correcto)
            if carrier == 'ambetter':
                expected_status_col = 'Eligible for Commission'
                actual_status_col = mapping.get('status')
                if actual_status_col != expected_status_col:
                    print(f"🚨 ERROR CRÍTICO: Ambetter mapea status a '{actual_status_col}' en lugar de '{expected_status_col}'")
                    print(f"🔧 Corrigiendo mapeo para Ambetter...")
                    mapping = self.carrier_mappings['ambetter']

        # Inicializar registro Silver
        silver_record = {
            'carrier': carrier,
            'aor_id': metadata.aor_id,
            'aor_name': metadata.aor_name,
            'extraction_date': metadata.extraction_date,
            'source_file': metadata.file_name,
            'policy_id': None,
            'exchange_id': None,
            'member_first_name': None,
            'member_middle_name': None,
            'member_last_name': None,
            'member_dob': None,
            'member_phone': None,
            'member_email': None,
            'member_address': None,
            'member_city': None,
            'member_state': None,
            'member_zip': None,
            'member_county': None,
            'effective_date': None,
            'term_date': None,
            'paid_through_date': None,
            'broker_effective_date': None,
            'broker_term_date': None,
            'premium': None,
            'premium_responsibility': None,
            'status': None,
            'payment_status': None,
            'on_off_exchange': None,
            'autopay_status': None,
            'member_count': None,
            'member_type': None,
            'is_primary_member': None,
            '_extra_fields': {},
        }

        # --- policy_id ---
        policy_id = self.get_value(row, mapping, 'policy_id', '')
        silver_record['policy_id'] = str(policy_id).strip() if policy_id else None

        # --- exchange_id ---
        exchange_id = self.get_value(row, mapping, 'exchange_id', '')
        if exchange_id:
            if carrier == 'cigna':
                # Cigna envía Application IDs como "5266873188-3", pero Sherpa
                # los tiene como "5266873188". Removemos el sufijo para match.
                silver_record['exchange_id'] = clean_cigna_application_id(exchange_id)
            else:
                silver_record['exchange_id'] = str(exchange_id).strip()
        else:
            silver_record['exchange_id'] = None

        silver_record['member_email'] = self.get_value(row, mapping, 'member_email')
        silver_record['member_city'] = self.get_value(row, mapping, 'member_city')

        # --- Estado y condado ---
        if carrier == 'floridablue':
            county_raw = self.get_value(row, mapping, 'member_county')
            silver_record['member_state'] = extract_state_from_county(county_raw)
            silver_record['member_county'] = extract_county_name(county_raw)
        else:
            silver_record['member_state'] = self.get_value(row, mapping, 'member_state')
            silver_record['member_county'] = self.get_value(row, mapping, 'member_county')

        # --- ZIP ---
        member_zip = self.get_value(row, mapping, 'member_zip', '')
        if member_zip:
            zip_str = str(member_zip).strip()
            if '.' in zip_str:
                zip_str = zip_str.split('.')[0]
            silver_record['member_zip'] = zip_str if zip_str else None

        # --- Status ---
        status_raw = self.get_value(row, mapping, 'status')
        if carrier == 'floridablue':
            silver_record['status'] = normalize_floridablue_status(status_raw)
        elif carrier == 'united' and not status_raw:
            # Archivos ene-sep 2025: planStatus vacío, status real está en memberStatus
            # memberStatus: A = Active, I = Inactive
            member_status = self.get_value(row, {'member_status': 'memberStatus'}, 'member_status')
            silver_record['status'] = member_status
        else:
            silver_record['status'] = status_raw

        silver_record['payment_status'] = self.get_value(row, mapping, 'payment_status')
        silver_record['on_off_exchange'] = self.get_value(row, mapping, 'on_off_exchange')
        silver_record['autopay_status'] = self.get_value(row, mapping, 'autopay_status')

        # --- Member count ---
        member_count = self.get_value(row, mapping, 'member_count')
        if member_count is not None:
            try:
                silver_record['member_count'] = int(float(member_count))
            except (ValueError, TypeError):
                silver_record['member_count'] = None

        # --- Member type ---
        member_type_raw = self.get_value(row, mapping, 'member_type')
        if member_type_raw is not None:
            silver_record['member_type'] = normalize_member_type(member_type_raw)

        # --- is_primary_member ---
        silver_record['is_primary_member'] = self._calculate_is_primary_member(
            carrier, row, mapping, silver_record
        )

        # --- Premium ---
        premium = self.get_value(row, mapping, 'premium')
        if premium is not None:
            try:
                premium_str = str(premium).replace('$', '').replace(',', '')
                silver_record['premium'] = float(premium_str)
            except (ValueError, TypeError):
                silver_record['premium'] = None

        # --- Premium responsibility ---
        premium_resp = self.get_value(row, mapping, 'premium_responsibility')
        if premium_resp is not None:
            try:
                premium_resp_str = str(premium_resp).replace('$', '').replace(',', '')
                silver_record['premium_responsibility'] = float(premium_resp_str)
            except (ValueError, TypeError):
                silver_record['premium_responsibility'] = None

        # --- Teléfono ---
        phone = self.get_value(row, mapping, 'member_phone')
        silver_record['member_phone'] = clean_phone(phone)

        # --- Fechas ---
        silver_record['member_dob'] = parse_date(self.get_value(row, mapping, 'member_dob'))
        silver_record['effective_date'] = parse_date(self.get_value(row, mapping, 'effective_date'))
        silver_record['term_date'] = parse_date(self.get_value(row, mapping, 'term_date'))
        silver_record['paid_through_date'] = parse_date(self.get_value(row, mapping, 'paid_through_date'))
        silver_record['broker_effective_date'] = parse_date(self.get_value(row, mapping, 'broker_effective_date'))
        silver_record['broker_term_date'] = parse_date(self.get_value(row, mapping, 'broker_term_date'))

        # --- Dirección ---
        address = self.get_value(row, mapping, 'member_address', '')
        address_2 = self.get_value(row, mapping, 'member_address_2', '')
        if address or address_2:
            addr_parts = [
                str(a).strip() for a in [address, address_2]
                if a and str(a).strip() and str(a).lower() != 'nan'
            ]
            silver_record['member_address'] = ', '.join(addr_parts) if addr_parts else None

        # --- Nombres ---
        if 'member_full_name' in mapping:
            full_name = self.get_value(row, mapping, 'member_full_name')
            name_parts = split_full_name(full_name)
            silver_record['member_first_name'] = name_parts['first']
            silver_record['member_middle_name'] = name_parts['middle']
            silver_record['member_last_name'] = name_parts['last']
        else:
            first_name = self.get_value(row, mapping, 'member_first_name')
            last_name = self.get_value(row, mapping, 'member_last_name')
            name_parts = combine_names_and_split(first_name, last_name)
            silver_record['member_first_name'] = name_parts['first']
            silver_record['member_middle_name'] = name_parts['middle']
            silver_record['member_last_name'] = name_parts['last']

        # --- Extra fields ---
        mapped_cols = set(v for k, v in mapping.items() if k != 'skip_rows' and not k.startswith('_'))
        extra_fields = {}

        for key, col_name in mapping.items():
            if key.startswith('_') and col_name in row.index:
                value = row[col_name]
                if not pd.isna(value):
                    extra_fields[key[1:]] = str(value)

        for col in row.index:
            if col not in mapped_cols and not pd.isna(row[col]):
                value = row[col]
                if isinstance(value, (int, float)):
                    if not pd.isna(value):
                        extra_fields[col] = value
                else:
                    extra_fields[col] = str(value)

        silver_record['_extra_fields'] = extra_fields if extra_fields else None

        return silver_record

    def _calculate_is_primary_member(
        self,
        carrier: str,
        row: pd.Series,
        mapping: Dict[str, str],
        silver_record: Dict[str, Any]
    ) -> bool:
        """
        Determina si la fila corresponde al titular (account holder) de la póliza.

        Reglas por carrier:
        - Ambetter:  Siempre True (cada fila = 1 póliza)
        - Oscar:     Siempre True (cada fila = 1 póliza)
        - Anthem:    Siempre True (cada fila = 1 póliza)
        - Cigna:     Siempre True (active_termed viene a nivel póliza)
        - United:    True si policy_id termina en '01'
        - AmeriHealth, Aetna, Community: True si Relationship = 'Self'
        - BlueCross: True si Record Type indica primary/subscriber
        - FloridaBlue: True si IS_SUBSCRIBER = 'Y' o CODE_DESC = 'Subscriber'
        - Molina:    True si HIX_ID == Subscriber_ID
        """
        always_primary = {'ambetter', 'oscar', 'anthem', 'cigna'}
        if carrier in always_primary:
            return True

        if carrier == 'united':
            policy_id = silver_record.get('policy_id')
            if policy_id:
                return str(policy_id).strip()[-2:] == '01'
            return True

        if carrier in ('amerihealth', 'aetna', 'community'):
            member_type = silver_record.get('member_type')
            if member_type is None:
                return True
            return member_type == 'Policy Holder'

        if carrier in ('bluecross', 'bluecross_application'):
            member_type = silver_record.get('member_type')
            if member_type is None:
                return True
            if carrier == 'bluecross_application':
                return True
            return member_type == 'Policy Holder'

        if carrier == 'floridablue':
            is_subscriber = self.get_value(row, mapping, 'member_type')
            if is_subscriber is not None:
                is_sub_str = str(is_subscriber).strip().upper()
                if is_sub_str in ('Y', 'YES', '1', 'TRUE'):
                    return True
                if is_sub_str in ('N', 'NO', '0', 'FALSE'):
                    return False
            member_type = silver_record.get('member_type')
            if member_type is not None:
                return member_type == 'Policy Holder'
            return True

        if carrier == 'molina':
            hix_id = self.get_value(row, mapping, 'exchange_id')
            subscriber_id = self.get_value(row, mapping, 'policy_id')
            if hix_id is not None and subscriber_id is not None:
                return str(hix_id).strip() == str(subscriber_id).strip()
            member_type = silver_record.get('member_type')
            if member_type is not None:
                return member_type == 'Policy Holder'
            return True

        return True

    def transform_dataframe(
        self,
        df: pd.DataFrame,
        metadata: FileMetadata,
        floridablue_format: Optional[str] = None,
        aetna_format: Optional[str] = None,
        amerihealth_format: Optional[str] = None,
        united_format: Optional[str] = None,
        community_format: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Transforma un DataFrame completo a registros Silver.

        El formato se detecta UNA sola vez para todo el DataFrame y se pasa
        a cada llamada a transform_row como precomputed_mapping, evitando
        ~N llamadas redundantes de detección (donde N = número de filas).
        """
        records = []
        carrier = metadata.carrier

        # Detectar formato una vez para todo el archivo
        format_detected = self.detect_carrier_format_improved(df, carrier)
        if format_detected:
            precomputed_mapping = self.get_mapping_for_carrier_format(carrier, format_detected)
        else:
            print(f"⚠️ No se pudo detectar formato para {carrier} - usando mapeo base")
            precomputed_mapping = self.carrier_mappings.get(carrier, {})

        for idx, row in df.iterrows():
            try:
                record = self.transform_row(
                    row,
                    metadata,
                    precomputed_mapping=precomputed_mapping,
                )
                records.append(record)
            except Exception as e:
                print(f"Error en fila {idx}: {e}")

        return records