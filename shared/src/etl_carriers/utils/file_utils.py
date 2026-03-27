"""
Utilidades para extracción de metadatos de archivos y rutas.
"""

import re
from typing import Dict, Optional
from dataclasses import dataclass

from etl_carriers.config import get_carrier_from_folder, get_aor_info, AORInfo


@dataclass
class FileMetadata:
    """Metadatos extraídos de un archivo."""
    carrier: str
    file_name: str
    aor_id: Optional[str]
    aor_name: Optional[str]
    extraction_date: Optional[str]
    file_path: str


def extract_carrier_from_path(file_path: str) -> str:
    """
    Extrae el nombre del carrier de la ruta del archivo.
    
    Args:
        file_path: Ruta completa (ej: Data_Lake/Bronze/molina/2026/01/archivo.csv)
    
    Returns:
        Nombre del carrier normalizado o 'unknown'
    """
    parts = file_path.split('/')
    
    # Ruta esperada: Data_Lake/Bronze/{carrier}/...
    if len(parts) >= 3 and parts[0] == 'Data_Lake' and parts[1] == 'Bronze':
        folder_name = parts[2].lower()
        return get_carrier_from_folder(folder_name)
    
    return 'unknown'


def extract_metadata_from_filename(file_name: str) -> Dict[str, Optional[str]]:
    """
    Extrae aor_id, aor_name y extraction_date del nombre del archivo.
    
    Formato esperado: carrier_YYYY-MM-DD_aor.csv
    Ejemplos:
        - ambetter_2026-01-15_mpt.csv
        - floridablue_2026-01-06_20087079.csv
    
    Returns:
        Dict con 'aor_id', 'aor_name', 'extraction_date'
    """
    try:
        # Quitar extensión
        name_without_ext = file_name.replace('.csv', '').replace('.CSV', '')
        name_without_ext = name_without_ext.replace('.xlsx', '').replace('.XLSX', '')
        
        parts = name_without_ext.split('_')
        
        extraction_date = None
        aor_code = None
        
        # Buscar fecha en formato YYYY-MM-DD
        for part in parts:
            if re.match(r'\d{4}-\d{2}-\d{2}', part):
                extraction_date = part
        
        # El AOR suele ser la última parte
        if len(parts) >= 1:
            aor_code = parts[-1].lower()
        
        # Obtener info del AOR
        aor_id = None
        aor_name = None
        
        if aor_code:
            aor_info = get_aor_info(aor_code)
            aor_id = aor_info.npn
            aor_name = aor_info.name
        
        return {
            'aor_id': aor_id,
            'aor_name': aor_name,
            'extraction_date': extraction_date
        }
        
    except Exception as e:
        print(f"Error extrayendo metadata del filename: {e}")
        return {'aor_id': None, 'aor_name': None, 'extraction_date': None}


def extract_file_metadata(file_path: str) -> FileMetadata:
    """
    Extrae todos los metadatos de un archivo dado su path.
    
    Args:
        file_path: Ruta completa del archivo
    
    Returns:
        FileMetadata con toda la información extraída
    """
    file_name = file_path.split('/')[-1]
    carrier = extract_carrier_from_path(file_path)
    metadata = extract_metadata_from_filename(file_name)
    
    return FileMetadata(
        carrier=carrier,
        file_name=file_name,
        aor_id=metadata['aor_id'],
        aor_name=metadata['aor_name'],
        extraction_date=metadata['extraction_date'],
        file_path=file_path
    )


def is_valid_bronze_file(file_path: str) -> bool:
    """
    Verifica si un archivo es válido para procesar desde Bronze.
    
    Args:
        file_path: Ruta del archivo
    
    Returns:
        True si es un archivo válido para procesar
    """
    # Debe estar en Bronze
    if not file_path.startswith('Data_Lake/Bronze/'):
        return False
    
    # No puede ser una carpeta
    if file_path.endswith('/'):
        return False
    
    # Debe tener extensión válida
    valid_extensions = ('.csv', '.CSV', '.xlsx', '.XLSX', '.xls', '.XLS')
    if not file_path.endswith(valid_extensions):
        return False
    
    return True
