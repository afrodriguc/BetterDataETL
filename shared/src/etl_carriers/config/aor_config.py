"""
Configuración centralizada de AORs (Agents of Record).
Mapea códigos de nomenclatura y NPNs a información del agente.
"""

from typing import Dict, Tuple, Optional
from dataclasses import dataclass


@dataclass
class AORInfo:
    """Información de un Agent of Record."""
    npn: str
    name: str
    original_code: Optional[str] = None


# AOR por defecto cuando no se detecta código válido en el nombre del archivo
DEFAULT_AOR: Tuple[str, str] = ('21072733', 'Manuel Perez-Trujillo')


# Mapeo de nomenclaturas AOR a (NPN, Nombre)
AOR_MAP: Dict[str, Tuple[str, str]] = {
    # Nomenclaturas cortas
    'msc': ('20087079', 'Manuel Camelo'),
    'mpt': ('21072733', 'Manuel Perez-Trujillo'),
    'dgm': ('20685325', 'Darian Garcia Mujica'),
    'okm': ('20970860', 'Odra K Morillo'),
    'mm': ('20363649', 'Maria D Montalvo'),
    'ysg': ('20756710', 'Yaditza Del Sol Gonzalez'),
    'jcm': ('20762000', 'Jaysa Carballo'),
    'ler': ('21283608', 'Luis E Romero'),
    'ls': ('20825061', 'Luisa F Tenorio'),
    'abd': ('21392043', 'Angela Blanco'),
    'mpz': ('20880845', 'Madelein Perdomo'),
    
    # NPNs directos
    '20087079': ('20087079', 'Manuel Camelo'),
    '21072733': ('21072733', 'Manuel Perez-Trujillo'),
    '20685325': ('20685325', 'Darian Garcia Mujica'),
    '20970860': ('20970860', 'Odra K Morillo'),
    '20363649': ('20363649', 'Maria D Montalvo'),
    '20756710': ('20756710', 'Yaditza Del Sol Gonzalez'),
    '20762000': ('20762000', 'Jaysa Carballo'),
    '21283608': ('21283608', 'Luis E Romero'),
    '20825061': ('20825061', 'Luisa F Tenorio'),
    '20769809': ('20769809', 'Sandra Munerapatino'),
    '20860018': ('20860018', 'Monika Legisa'),
    '21392043': ('21392043', 'Angela Blanco'),
    '20880845': ('20880845', 'Madelein Perdomo'),
    '20596701': ('20596701', 'David Gomez'),
    '20996814': ('20996814', 'Andrew Bazze'),
    '20036246': ('20036246', 'Kelly Valle'),
    '20452917': ('20452917', 'Daniela Duran'),
    '20474159': ('20474159', 'Llamar Williams'),
    '20973822': ('20973822', 'Didier Ugalde'),
    '20280380': ('20280380', 'Shaka Careen Smith'),
    '21305070': ('21305070', 'Amanda Farias'),
    '19321840': ('19321840', 'Brayan Moreno'),
    '21405129': ('21405129', 'Angel Abad'),
    '19232067': ('19232067', 'Margarette Prosper'),
    '20366448': ('20366448', 'Matthew Woodall'),
}


def is_valid_aor_code(code: str) -> bool:
    """
    Verifica si un código es un AOR válido (nomenclatura o NPN).
    
    Args:
        code: Código a verificar
    
    Returns:
        True si es un código AOR válido, False si parece ser otra cosa (fecha, etc.)
    """
    if not code:
        return False
    
    code_lower = code.lower()
    
    # Si está en el mapeo, es válido
    if code_lower in AOR_MAP:
        return True
    
    # Si parece una fecha (YYYY-MM-DD), NO es válido
    import re
    if re.match(r'^\d{4}-\d{2}-\d{2}$', code):
        return False
    
    # Si es solo números y tiene longitud de NPN (8 dígitos), es válido
    if code.isdigit() and len(code) == 8:
        return True
    
    return False


def get_aor_info(code: str, use_default: bool = True) -> AORInfo:
    """
    Obtiene información del AOR a partir de un código o NPN.
    
    Args:
        code: Código de nomenclatura (ej: 'mpt') o NPN (ej: '21072733')
        use_default: Si True, retorna DEFAULT_AOR cuando el código no es válido
    
    Returns:
        AORInfo con npn, nombre y código original
    """
    code_lower = code.lower() if code else ''
    
    if code_lower in AOR_MAP:
        npn, name = AOR_MAP[code_lower]
        return AORInfo(npn=npn, name=name, original_code=code)
    
    # Si el código no es válido y use_default es True, usar AOR por defecto
    if use_default and not is_valid_aor_code(code):
        npn, name = DEFAULT_AOR
        return AORInfo(npn=npn, name=name, original_code=None)
    
    # Si no está en el mapeo pero parece NPN válido, asumirlo como NPN directo
    return AORInfo(npn=code, name='Unknown', original_code=code)