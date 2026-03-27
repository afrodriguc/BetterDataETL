"""
Utilidades para parseo y limpieza de datos.
Funciones de transformación comunes usadas por todos los transformers.
"""

import re
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any, Tuple


# =============================================================================
# LIMPIEZA DE APPLICATION ID (CIGNA)
# =============================================================================

def clean_cigna_application_id(app_id: Any) -> Optional[str]:
    """
    Limpia el Application Id de Cigna removiendo el sufijo -X (ej: -3, -5).
    
    Cigna envía Application IDs en formato "5266873188-3" pero Sherpa
    los tiene como "5266873188". Para hacer match, removemos el sufijo.
    
    Args:
        app_id: Application Id de Cigna (puede venir como "5266873188-3" o "HX80961141")
    
    Returns:
        Application Id limpio sin sufijo, o None si no hay valor
    """
    if pd.isna(app_id) or app_id is None:
        return None
    
    app_id_str = str(app_id).strip()
    
    if not app_id_str or app_id_str.lower() == 'nan':
        return None
    
    # Remover sufijo -X al final (donde X son dígitos)
    cleaned = re.sub(r'-\d+$', '', app_id_str)
    
    return cleaned if cleaned else None



# =============================================================================
# NORMALIZACIÓN DE MEMBER TYPE
# =============================================================================

MEMBER_TYPE_NORMALIZATION: Dict[str, str] = {
    # Policy Holder / Subscriber / Primary
    'self': 'Policy Holder',
    'subscriber': 'Policy Holder',
    'policy': 'Policy Holder',
    'policy holder': 'Policy Holder',
    'policyholder': 'Policy Holder',
    'primary': 'Policy Holder',
    'head': 'Policy Holder',
    'member': 'Policy Holder',
    '1': 'Policy Holder',
    '1.0': 'Policy Holder',
    
    # Spouse
    'spouse': 'Spouse',
    'husband': 'Spouse',
    'wife': 'Spouse',
    'domestic partner': 'Spouse',
    'partner': 'Spouse',
    
    # Dependent
    'dependent': 'Dependent',
    'child': 'Dependent',
    'son': 'Dependent',
    'daughter': 'Dependent',
    'stepchild': 'Dependent',
    'foster child': 'Dependent',
    'adopted child': 'Dependent',
    'other': 'Dependent',
}


def normalize_member_type(raw_value: Any) -> Optional[str]:
    """
    Normaliza el valor de member_type a uno de los 3 valores estándar:
    - Policy Holder
    - Spouse
    - Dependent
    
    Args:
        raw_value: Valor crudo del campo (puede ser string, int, float)
    
    Returns:
        String normalizado o None si no hay valor
    """
    if pd.isna(raw_value) or raw_value is None:
        return None
    
    value_str = str(raw_value).strip().lower()
    
    if not value_str or value_str == 'nan':
        return None
    
    # Buscar en diccionario de normalización
    normalized = MEMBER_TYPE_NORMALIZATION.get(value_str)
    if normalized:
        return normalized
    
    # Intentar match parcial
    for key, norm_value in MEMBER_TYPE_NORMALIZATION.items():
        if key in value_str or value_str in key:
            return norm_value
    
    # Si ya es uno de los valores normalizados
    value_title = raw_value.strip().title() if isinstance(raw_value, str) else str(raw_value)
    if value_title in ['Policy Holder', 'Spouse', 'Dependent']:
        return value_title
    
    # Retornar valor original en Title Case
    return str(raw_value).strip().title()


# =============================================================================
# PARSING DE FECHAS
# =============================================================================

DATE_FORMATS = [
    '%Y-%m-%d',
    '%m/%d/%Y',
    '%m/%d/%y',
    '%d/%m/%Y',
    '%Y/%m/%d',
    '%m-%d-%Y',
    '%d-%m-%Y',
]


def parse_date(date_value: Any) -> Optional[str]:
    """
    Convierte cualquier formato de fecha a YYYY-MM-DD.
    
    Args:
        date_value: Valor de fecha en cualquier formato
    
    Returns:
        Fecha en formato YYYY-MM-DD o None
    """
    if pd.isna(date_value) or date_value is None:
        return None
    
    date_str = str(date_value).strip()
    
    if not date_str or date_str.lower() in ('', '-', 'nan'):
        return None
    
    # Ignorar fechas con 9999
    if '9999' in date_str:
        return '9999-12-31'
    
    # Remover parte de tiempo si existe
    if 'T' in date_str:
        date_str = date_str.split('T')[0]
    
    # Intentar formatos conocidos
    for fmt in DATE_FORMATS:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # Intentar con dateutil como fallback
    try:
        from dateutil import parser
        parsed = parser.parse(date_str)
        return parsed.strftime('%Y-%m-%d')
    except Exception:
        pass
    
    return None


# =============================================================================
# LIMPIEZA DE TELÉFONOS
# =============================================================================

def clean_phone(phone_value: Any) -> Optional[str]:
    """
    Limpia el teléfono dejando solo números.
    
    Args:
        phone_value: Valor de teléfono en cualquier formato
    
    Returns:
        Teléfono limpio (solo dígitos) o None
    """
    if pd.isna(phone_value) or phone_value is None:
        return None
    
    phone_str = str(phone_value).strip()
    
    if not phone_str or phone_str.lower() == 'nan':
        return None
    
    # Manejar notación científica
    if 'e' in phone_str.lower():
        try:
            phone_float = float(phone_str)
            phone_str = str(int(phone_float))
        except (ValueError, OverflowError):
            pass
    
    # Manejar decimales (ej: "1234567890.0")
    if '.' in phone_str:
        try:
            phone_float = float(phone_str)
            if phone_float == int(phone_float):
                phone_str = str(int(phone_float))
            else:
                phone_str = phone_str.split('.')[0]
        except (ValueError, OverflowError):
            phone_str = phone_str.split('.')[0]
    
    # Dejar solo dígitos
    cleaned = re.sub(r'[^\d]', '', phone_str)
    
    # Validar longitud
    if len(cleaned) < 7:
        return None
    
    if len(cleaned) > 15:
        cleaned = cleaned[:15]
    
    return cleaned if cleaned else None


# =============================================================================
# PARSING DE NOMBRES
# =============================================================================

def split_full_name(full_name: Any) -> Dict[str, Optional[str]]:
    """
    Divide un nombre completo en first_name, middle_name, last_name.
    
    Args:
        full_name: Nombre completo (puede contener coma para formato "Apellido, Nombre")
    
    Returns:
        Dict con keys 'first', 'middle', 'last'
    """
    if pd.isna(full_name) or full_name is None:
        return {'first': None, 'middle': None, 'last': None}
    
    name = str(full_name).strip()
    name = re.sub(r'\s+', ' ', name)  # Normalizar espacios
    
    # Formato "Apellido, Nombre"
    if ',' in name:
        parts = name.split(',', 1)
        last_part = parts[0].strip()
        first_part = parts[1].strip() if len(parts) > 1 else ''
        
        first_words = first_part.split() if first_part else []
        
        if len(first_words) >= 2:
            return {
                'first': first_words[0],
                'middle': ' '.join(first_words[1:]),
                'last': last_part
            }
        return {
            'first': first_part if first_part else None,
            'middle': None,
            'last': last_part
        }
    
    # Formato "Nombre Apellido"
    words = name.split()
    
    if len(words) == 1:
        return {'first': words[0], 'middle': None, 'last': None}
    elif len(words) == 2:
        return {'first': words[0], 'middle': None, 'last': words[1]}
    elif len(words) == 3:
        return {'first': words[0], 'middle': words[1], 'last': words[2]}
    else:
        return {
            'first': words[0],
            'middle': words[1],
            'last': ' '.join(words[2:])
        }


def combine_names_and_split(first_name: Any, last_name: Any) -> Dict[str, Optional[str]]:
    """
    Combina first y last name, luego divide en 3 partes.
    Útil cuando el nombre viene en columnas separadas pero puede tener múltiples palabras.
    """
    first = str(first_name).strip() if not pd.isna(first_name) else ''
    last = str(last_name).strip() if not pd.isna(last_name) else ''
    
    first = re.sub(r'\s+', ' ', first)
    last = re.sub(r'\s+', ' ', last)
    
    first_words = first.split() if first else []
    last_words = last.split() if last else []
    
    result = {'first': None, 'middle': None, 'last': None}
    
    if first_words:
        result['first'] = first_words[0]
        if len(first_words) > 1:
            result['middle'] = ' '.join(first_words[1:])
    
    if last_words:
        if len(last_words) == 1:
            result['last'] = last_words[0]
        else:
            if result['middle'] is None:
                result['middle'] = last_words[0]
                result['last'] = ' '.join(last_words[1:])
            else:
                result['last'] = ' '.join(last_words)
    
    return result


# =============================================================================
# PROCESAMIENTO DE VALORES ESPECIALES
# =============================================================================

def process_special_value(value: Any, col_type: str) -> Tuple[Any, str]:
    """
    Procesa un valor según su tipo para reportes especiales.
    
    Args:
        value: Valor a procesar
        col_type: Tipo de dato (STRING, STRING_UPPER, PHONE, DATE, FLOAT, INT, TIMESTAMP)
    
    Returns:
        Tupla (valor_procesado, tipo_bigquery)
    """
    if pd.isna(value) or value is None:
        return None, _get_bq_type(col_type)
    
    if col_type == 'STRING':
        return str(value).strip(), 'STRING'
    elif col_type == 'STRING_UPPER':
        return str(value).strip().upper(), 'STRING'
    elif col_type == 'PHONE':
        return clean_phone(value), 'STRING'
    elif col_type == 'DATE':
        return parse_date(value), 'DATE'
    elif col_type == 'FLOAT':
        try:
            clean_value = str(value).replace('$', '').replace(',', '').strip()
            return float(clean_value), 'FLOAT64'
        except (ValueError, TypeError):
            return None, 'FLOAT64'
    elif col_type == 'INT':
        try:
            return int(float(value)), 'INT64'
        except (ValueError, TypeError):
            return None, 'INT64'
    elif col_type == 'TIMESTAMP':
        try:
            if pd.isna(value):
                return None, 'STRING'
            return str(value), 'STRING'
        except:
            return None, 'STRING'
    else:
        return str(value).strip() if value else None, 'STRING'


def _get_bq_type(col_type: str) -> str:
    """Convierte tipo interno a tipo BigQuery."""
    type_map = {
        'STRING': 'STRING',
        'STRING_UPPER': 'STRING',
        'PHONE': 'STRING',
        'DATE': 'DATE',
        'FLOAT': 'FLOAT64',
        'INT': 'INT64',
        'TIMESTAMP': 'STRING',
    }
    return type_map.get(col_type, 'STRING')


# =============================================================================
# UTILIDADES PARA FLORIDABLUE
# =============================================================================

def extract_state_from_county(county_value: Any) -> Optional[str]:
    """Extrae el estado del campo county de FloridaBlue."""
    if not county_value or pd.isna(county_value):
        return None
    
    county_str = str(county_value).upper().strip()
    
    state_map = {
        'FLORIDA': 'FL',
        'TEXAS': 'TX',
        'GEORGIA': 'GA',
        'CALIFORNIA': 'CA',
        'NEW YORK': 'NY',
    }
    
    for state_name, state_code in state_map.items():
        if county_str.startswith(state_name):
            return state_code
    
    return None


def extract_county_name(county_value: Any) -> Optional[str]:
    """Extrae solo el nombre del condado del campo county de FloridaBlue."""
    if not county_value or pd.isna(county_value):
        return None
    
    county_str = str(county_value).upper().strip()
    
    states_to_remove = ['FLORIDA ', 'TEXAS ', 'GEORGIA ', 'CALIFORNIA ', 'NEW YORK ']
    
    for state in states_to_remove:
        if county_str.startswith(state):
            return county_str[len(state):].strip()
    
    return county_str


def normalize_floridablue_status(status: Any) -> Optional[str]:
    """Normaliza el status de FloridaBlue."""
    if not status or pd.isna(status):
        return None
    
    status_map = {
        'ACTV': 'Active',
        'ACTIVE': 'Active',
        'TERM': 'Terminated',
        'TERMED': 'Terminated',
        'CANCEL': 'Cancelled',
        'CANCELLED': 'Cancelled',
        'PEND': 'Pending',
        'PENDING': 'Pending',
    }
    
    status_upper = str(status).upper().strip()
    return status_map.get(status_upper, status)
