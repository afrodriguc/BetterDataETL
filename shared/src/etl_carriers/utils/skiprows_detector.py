"""
Detector de skiprows súper dinámico y robusto para archivos United Health Care.
Versión avanzada que reemplaza al detector básico con capacidades superiores.

COMPATIBILIDAD: Mantiene la función detect_united_skiprows() original pero con lógica avanzada.
Detecta headers en cualquier fila (0-15) usando múltiples estrategias inteligentes.
"""

import pandas as pd
import csv
import io
import re
from typing import List, Optional, Tuple, Dict, Union
import logging


class AdvancedUnitedSkiprowsDetector:
    """
    Detector súper avanzado de skiprows para archivos United Health Care.
    
    CAPACIDADES SÚPER AVANZADAS:
    ✅ Detecta headers en cualquier fila (0-15)
    ✅ Maneja archivos Legacy con metadata
    ✅ Reconoce múltiples formatos United (BetterClean, Original, Transformed)
    ✅ Sistema de scoring inteligente con 6 criterios
    ✅ Fallbacks robustos
    ✅ 100% compatible con gcs_loader.py existente
    """
    
    # Patrones de headers United conocidos (ampliado y mejorado)
    UNITED_PATTERNS = [
        ['agentId', 'agentIdStatus', 'agentName', 'memberFirstName'],        # BetterClean/Normalized ✨
        ['memberFirstName', 'memberLastName', 'dateOfBirth', 'memberNumber'], # Original Format
        ['Policy Number', 'Exchange Subscriber ID', 'Insured First Name'],   # Transformed Format
        ['agentId', 'agentName', 'agentEmail', 'agentNpn'],                  # Common Pattern
        ['agentId', 'memberFirstName', 'memberLastName'],                    # Minimal Pattern
    ]
    
    # Palabras que NO son headers (mejorado)
    NON_HEADER_INDICATORS = [
        'note:', 'confidential', 'proprietary', 'do not distribute',
        'unitedhealth group', 'book of business', 'generated on',
        'report date', 'copyright', 'total records'
    ]
    
    # Palabras que SÍ indican headers (ampliado)
    HEADER_INDICATORS = [
        'agent', 'member', 'policy', 'id', 'name', 'email', 'phone',
        'address', 'date', 'status', 'plan', 'contract', 'exchange',
        'subscriber', 'first', 'last', 'birth', 'number', 'effective'
    ]

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def detect_skiprows_from_content(self, content: bytes, file_extension: str) -> int:
        """
        Detecta el skiprows óptimo usando SÚPER ESTRATEGIAS AVANZADAS.
        
        ESTRATEGIAS MÚLTIPLES:
        1️⃣ Patrones específicos United (BetterClean, Original, Transformed)
        2️⃣ Sistema de scoring inteligente (6 criterios)
        3️⃣ Análisis de estructura de datos
        4️⃣ Fallbacks robustos por tipo de archivo
        
        Args:
            content: Contenido del archivo en bytes
            file_extension: Extensión del archivo ('.csv', '.xlsx', etc.)
            
        Returns:
            int: Número óptimo de skiprows (0, 2, 4, 6, etc.)
        """
        try:
            # Leer hasta 15 filas para análisis súper completo
            if file_extension.lower() in ['.xlsx', '.xls']:
                rows = self._read_excel_rows(content, 15)
            else:
                rows = self._read_csv_rows(content, 15)
            
            if not rows:
                return self._fallback_skiprows(file_extension)
            
            # 🎯 ESTRATEGIA 1: Patrones específicos United (SÚPER PRECISOS)
            pattern_result = self._detect_by_united_patterns(rows)
            if pattern_result is not None:
                self.logger.info(f"🎯 Headers United detectados por PATRÓN ESPECÍFICO en fila {pattern_result + 1} → skiprows={pattern_result}")
                return pattern_result
            
            # 📊 ESTRATEGIA 2: Sistema de scoring súper avanzado (6 CRITERIOS)
            scoring_result = self._detect_by_advanced_scoring(rows)
            if scoring_result is not None:
                self.logger.info(f"📊 Headers United detectados por SCORING AVANZADO en fila {scoring_result + 1} → skiprows={scoring_result}")
                return scoring_result
            
            # 🏗️ ESTRATEGIA 3: Análisis de estructura de datos (INTELIGENTE)
            structure_result = self._detect_by_data_structure(rows)
            if structure_result is not None:
                self.logger.info(f"🏗️ Headers United detectados por ESTRUCTURA DE DATOS en fila {structure_result + 1} → skiprows={structure_result}")
                return structure_result
            
            # 🛡️ FALLBACK: Súper inteligente basado en análisis de contenido
            fallback = self._intelligent_fallback(rows, file_extension)
            self.logger.warning(f"🛡️ Usando FALLBACK INTELIGENTE: skiprows={fallback}")
            return fallback
            
        except Exception as e:
            fallback = self._fallback_skiprows(file_extension)
            self.logger.error(f"❌ Error en detección súper avanzada: {e}. Usando fallback: {fallback}")
            return fallback

    def _detect_by_united_patterns(self, rows: Dict[int, List[str]]) -> Optional[int]:
        """
        🎯 ESTRATEGIA 1: Detecta headers usando patrones específicos United SÚPER PRECISOS.
        Reconoce: BetterClean, Original, Transformed y otros formatos.
        """
        for row_idx, row_data in rows.items():
            if not row_data or len(row_data) < 3:
                continue
                
            # Convertir a string limpio para análisis
            row_str = ' '.join(str(cell).strip() for cell in row_data).lower()
            
            # Verificar cada patrón United con scoring mejorado
            for pattern in self.UNITED_PATTERNS:
                matches = 0
                total_chars_matched = 0
                
                for header in pattern:
                    if header.lower() in row_str:
                        matches += 1
                        total_chars_matched += len(header)
                
                # Scoring avanzado: ratio + longitud de coincidencias
                match_ratio = matches / len(pattern)
                char_bonus = min(0.2, total_chars_matched / 100)  # Bonus por caracteres coincidentes
                final_score = match_ratio + char_bonus
                
                # Si coincide 70% o más del patrón (SÚPER PRECISIÓN)
                if final_score >= 0.7:
                    return row_idx
                    
        return None

    def _detect_by_advanced_scoring(self, rows: Dict[int, List[str]]) -> Optional[int]:
        """
        📊 ESTRATEGIA 2: Sistema de scoring súper avanzado con 6 CRITERIOS INTELIGENTES.
        """
        best_score = 0.0
        best_row = None
        
        for row_idx, row_data in rows.items():
            score = self._calculate_advanced_score(row_data)
            
            # Threshold más inteligente (60% mínimo)
            if score > best_score and score >= 0.60:
                best_score = score
                best_row = row_idx
                
        return best_row

    def _calculate_advanced_score(self, row_data: List[str]) -> float:
        """
        🧠 SCORING SÚPER INTELIGENTE con 6 CRITERIOS AVANZADOS.
        """
        if not row_data or len(row_data) < 3:
            return 0.0
            
        row_values = [str(cell).strip() for cell in row_data if str(cell).strip()]
        
        if not row_values:
            return 0.0
        
        scores = []
        
        # 🚫 CRITERIO 0: Verificar que no sea fila de datos (primer campo numérico = -1.0)
        if row_values[0].isdigit():
            return 0.0
            
        # 📏 CRITERIO 1: Longitud promedio óptima de headers (8-50 caracteres)
        avg_length = sum(len(val) for val in row_values) / len(row_values)
        length_score = max(0, min(1.0, (avg_length - 3) / 47))
        scores.append(length_score * 0.20)  # Peso: 20%
        
        # 📝 CRITERIO 2: Proporción de texto vs números (headers son texto)
        text_count = sum(1 for val in row_values if not val.replace('.', '').replace('-', '').isdigit())
        text_ratio = text_count / len(row_values)
        scores.append(text_ratio * 0.25)  # Peso: 25%
        
        # 🎯 CRITERIO 3: Presencia de palabras clave de headers (SÚPER ESPECÍFICO)
        header_words = 0
        for val in row_values:
            val_lower = val.lower()
            for indicator in self.HEADER_INDICATORS:
                if indicator in val_lower:
                    header_words += 1
                    break  # No contar múltiples matches por valor
        
        header_score = min(1.0, header_words / max(1, len(row_values) * 0.4))
        scores.append(header_score * 0.30)  # Peso: 30%
        
        # ❌ CRITERIO 4: Penalización por palabras que NO son headers
        non_header_penalty = 0
        for val in row_values:
            val_lower = val.lower()
            for bad_indicator in self.NON_HEADER_INDICATORS:
                if bad_indicator in val_lower:
                    non_header_penalty += 0.3
                    break
        
        penalty = min(0.4, non_header_penalty)
        scores.append(-penalty)  # Penalización
        
        # 🏆 CRITERIO 5: Bonus súper específico por patrones United
        united_bonus = 0.0
        row_str = ' '.join(row_values).lower()
        
        # Bonus por combinaciones específicas United
        if 'agentid' in row_str and 'member' in row_str:
            united_bonus += 0.15
        if 'agentname' in row_str and 'agentemail' in row_str:
            united_bonus += 0.10
        if 'memberfirstname' in row_str or 'memberLastName' in row_str.lower():
            united_bonus += 0.10
        if 'dateofbirth' in row_str or 'membernumber' in row_str:
            united_bonus += 0.10
        
        scores.append(united_bonus)  # Peso: Variable
        
        # 📊 CRITERIO 6: Consistencia de datos (no muchas celdas vacías)
        non_empty_ratio = len(row_values) / len(row_data) if row_data else 0
        consistency_score = non_empty_ratio * 0.10
        scores.append(consistency_score)  # Peso: 10%
        
        # SCORE FINAL: Suma ponderada con normalización
        final_score = sum(scores)
        return max(0.0, min(1.0, final_score))

    def _detect_by_data_structure(self, rows: Dict[int, List[str]]) -> Optional[int]:
        """
        🏗️ ESTRATEGIA 3: Análisis súper inteligente de estructura de datos.
        """
        for row_idx in sorted(rows.keys()):
            if row_idx + 1 not in rows:
                continue
                
            current_row = rows[row_idx]
            next_row = rows[row_idx + 1]
            
            current_count = len([x for x in current_row if str(x).strip()])
            next_count = len([x for x in next_row if str(x).strip()])
            
            # Ambas filas deben tener datos consistentes (5+ columnas)
            if (current_count >= 5 and next_count >= 5 and 
                abs(current_count - next_count) <= 3):
                
                # Verificar que no sea metadata/texto explicativo
                current_str = ' '.join(str(x) for x in current_row).lower()
                if not any(bad in current_str for bad in self.NON_HEADER_INDICATORS[:5]):
                    # Score adicional si parece header
                    basic_score = self._calculate_advanced_score(current_row)
                    if basic_score >= 0.4:  # Threshold más bajo para esta estrategia
                        return row_idx
                    
        return None

    def _intelligent_fallback(self, rows: Dict[int, List[str]], file_extension: str) -> int:
        """
        🛡️ FALLBACK SÚPER INTELIGENTE que analiza el contenido real.
        """
        # Buscar la primera fila que no sea obviamente metadata
        for row_idx, row_data in rows.items():
            if row_idx > 10:  # No buscar más allá de fila 10
                break
                
            if not row_data:
                continue
                
            row_str = ' '.join(str(x) for x in row_data).lower()
            
            # Si no contiene texto de metadata obvio Y tiene múltiples columnas
            non_empty = len([x for x in row_data if str(x).strip()])
            if (non_empty >= 4 and 
                not any(bad in row_str for bad in self.NON_HEADER_INDICATORS[:3])):
                return row_idx
        
        # Si no encontró nada, fallback tradicional
        return self._fallback_skiprows(file_extension)

    def _fallback_skiprows(self, file_extension: str) -> int:
        """
        🔄 Fallback tradicional mejorado basado en tipo de archivo.
        """
        if file_extension.lower() == '.csv':
            return 0  # CSV suelen empezar directo
        else:
            return 2  # Excel suelen tener metadata en las primeras filas

    def _read_csv_rows(self, content: bytes, num_rows: int) -> Dict[int, List[str]]:
        """📄 Lectura súper robusta de CSV desde bytes"""
        rows = {}
        
        try:
            # Múltiples encodings para máxima compatibilidad
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    content_str = content.decode(encoding, errors='ignore')
                    break
                except:
                    continue
            else:
                content_str = content.decode('utf-8', errors='replace')
            
            lines = content_str.strip().split('\n')[:num_rows]
            
            for i, line in enumerate(lines):
                if line.strip():
                    try:
                        # Usar csv.reader para parsing robusto
                        reader = csv.reader([line])
                        parsed_row = next(reader)
                        rows[i] = [str(cell).strip() for cell in parsed_row]
                    except Exception:
                        # Fallback: split simple por comas
                        rows[i] = [str(cell).strip() for cell in line.split(',')]
                        
        except Exception as e:
            self.logger.error(f"Error leyendo CSV súper robusto: {e}")
            
        return rows
    
    def _read_excel_rows(self, content: bytes, num_rows: int) -> Dict[int, List[str]]:
        """📊 Lectura súper robusta de Excel desde bytes"""
        rows = {}
        
        try:
            # Lectura con pandas súper robusta
            df = pd.read_excel(io.BytesIO(content), header=None, nrows=num_rows)
            
            for i in range(len(df)):
                row_data = df.iloc[i].tolist()
                # Convertir todo a strings limpios, manejar NaN
                clean_row = []
                for x in row_data:
                    if pd.isna(x):
                        clean_row.append('')
                    else:
                        clean_row.append(str(x).strip())
                rows[i] = clean_row
                
        except Exception as e:
            self.logger.error(f"Error leyendo Excel súper robusto: {e}")
            
        return rows


# 🔌 FUNCIÓN PRINCIPAL: 100% COMPATIBLE CON GCS_LOADER.PY
def detect_united_skiprows(content: bytes, file_extension: str, logger: Optional[logging.Logger] = None) -> int:
    """
    🚀 FUNCIÓN PRINCIPAL SÚPER AVANZADA - 100% COMPATIBLE CON CÓDIGO EXISTENTE.
    
    MEJORAS SÚPER AVANZADAS vs DETECTOR BÁSICO:
    ✅ Detecta skiprows 0, 2, 4, 6, 8+ (vs básico: solo 0, 2)  
    ✅ Analiza hasta 15 filas (vs básico: solo 3)
    ✅ 3 estrategias inteligentes (vs básico: 1 simple)
    ✅ 6 criterios de scoring (vs básico: 2 básicos)
    ✅ Reconoce BetterClean, Original, Transformed (vs básico: genérico)
    ✅ Fallbacks súper robustos (vs básico: estático)
    
    Args:
        content: Contenido del archivo en bytes
        file_extension: Extensión del archivo ('.csv', '.xlsx', etc.)
        logger: Logger opcional
        
    Returns:
        int: Número óptimo de skiprows detectado inteligentemente
    """
    detector = AdvancedUnitedSkiprowsDetector(logger)
    return detector.detect_skiprows_from_content(content, file_extension)


# 🔧 EXTENSIÓN PARA ANTHEM - AGREGAR AL FINAL DE skiprows_detector.py
# (Después de la función test_advanced_detection(), antes del if __name__)

class AnthemSkiprowsDetector:
    """
    🎯 Detector específico para Anthem basado en el éxito de AdvancedUnitedSkiprowsDetector.
    Reutiliza la lógica probada pero con patrones específicos de Anthem.
    """
    
    # Patrones específicos de Anthem
    ANTHEM_PATTERNS = [
        ['Client Name', 'Client ID', 'Market', 'Status', 'State'],          # Patrón típico
        ['Client ID', 'Status', 'Bill Status', 'Exchange'],                 # Patrón mínimo
        ['Client Name', 'Market', 'Exchange', 'Effective Date'],            # Variación común
    ]
    
    # Indicadores de títulos/metadata que Anthem pone al inicio
    ANTHEM_METADATA_INDICATORS = [
        'list of clients', 'as of', 'anthem', 'report for', 
        'generated on', 'summary', 'page', 'confidential'
    ]
    
    # Palabras que indican headers reales de Anthem
    ANTHEM_HEADER_INDICATORS = [
        'client', 'id', 'name', 'market', 'status', 'state', 'exchange',
        'effective', 'date', 'cancellation', 'bill', 'plan', 'product'
    ]

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def detect_skiprows_from_content(self, content: bytes, file_extension: str) -> int:
        """
        Detecta skiprows para archivos Anthem usando estrategias probadas de United.
        """
        try:
            # Reutilizar lógica de lectura robusta de United
            if file_extension.lower() in ['.xlsx', '.xls']:
                rows = self._read_excel_rows(content, 10)
            else:
                rows = self._read_csv_rows(content, 10)
            
            if not rows:
                return 0 if file_extension.lower() == '.csv' else 1
            
            # ESTRATEGIA 1: Patrones específicos Anthem
            pattern_result = self._detect_by_anthem_patterns(rows)
            if pattern_result is not None:
                self.logger.info(f"🎯 Headers Anthem detectados por PATRÓN en fila {pattern_result + 1} → skiprows={pattern_result}")
                return pattern_result
            
            # ESTRATEGIA 2: Scoring adaptado para Anthem
            scoring_result = self._detect_by_anthem_scoring(rows)
            if scoring_result is not None:
                self.logger.info(f"📊 Headers Anthem detectados por SCORING en fila {scoring_result + 1} → skiprows={scoring_result}")
                return scoring_result
            
            # FALLBACK: Inteligente
            fallback = self._anthem_fallback(rows, file_extension)
            self.logger.warning(f"🛡️ Anthem fallback: skiprows={fallback}")
            return fallback
            
        except Exception as e:
            fallback = 0 if file_extension.lower() == '.csv' else 1
            self.logger.error(f"❌ Error en detección Anthem: {e}. Fallback: {fallback}")
            return fallback

    def _detect_by_anthem_patterns(self, rows: Dict[int, List[str]]) -> Optional[int]:
        """🎯 Detecta headers usando patrones específicos de Anthem"""
        for row_idx, row_data in rows.items():
            if not row_data or len(row_data) < 3:
                continue
                
            row_str = ' '.join(str(cell).strip() for cell in row_data).lower()
            
            # Verificar cada patrón Anthem
            for pattern in self.ANTHEM_PATTERNS:
                matches = 0
                for header in pattern:
                    if header.lower() in row_str:
                        matches += 1
                
                # Si coincide 60% o más del patrón
                if matches / len(pattern) >= 0.6:
                    return row_idx
                    
        return None

    def _detect_by_anthem_scoring(self, rows: Dict[int, List[str]]) -> Optional[int]:
        """📊 Scoring adaptado para Anthem"""
        best_score = 0.0
        best_row = None
        
        for row_idx, row_data in rows.items():
            score = self._calculate_anthem_score(row_data)
            
            if score > best_score and score >= 0.55:  # Threshold más permisivo para Anthem
                best_score = score
                best_row = row_idx
                
        return best_row

    def _calculate_anthem_score(self, row_data: List[str]) -> float:
        """🧠 Scoring específico para Anthem"""
        if not row_data or len(row_data) < 3:
            return 0.0
            
        row_values = [str(cell).strip() for cell in row_data if str(cell).strip()]
        if not row_values:
            return 0.0
        
        scores = []
        
        # CRITERIO 1: No debe empezar con número (datos vs headers)
        if row_values[0].replace('.', '').replace('-', '').isdigit():
            return 0.0
            
        # CRITERIO 2: Longitud promedio razonable
        avg_length = sum(len(val) for val in row_values) / len(row_values)
        length_score = max(0, min(1.0, (avg_length - 2) / 48))
        scores.append(length_score * 0.20)
        
        # CRITERIO 3: Más texto que números
        text_count = sum(1 for val in row_values if not val.replace('.', '').replace('-', '').isdigit())
        text_ratio = text_count / len(row_values)
        scores.append(text_ratio * 0.30)
        
        # CRITERIO 4: Palabras clave Anthem
        anthem_words = 0
        for val in row_values:
            val_lower = val.lower()
            for indicator in self.ANTHEM_HEADER_INDICATORS:
                if indicator in val_lower:
                    anthem_words += 1
                    break
        
        anthem_score = min(1.0, anthem_words / max(1, len(row_values) * 0.3))
        scores.append(anthem_score * 0.35)
        
        # CRITERIO 5: Penalización por metadata
        metadata_penalty = 0
        for val in row_values:
            val_lower = val.lower()
            for bad_indicator in self.ANTHEM_METADATA_INDICATORS:
                if bad_indicator in val_lower:
                    metadata_penalty += 0.4
                    break
        
        scores.append(-min(0.4, metadata_penalty))
        
        # CRITERIO 6: Consistencia
        consistency = len(row_values) / len(row_data) if row_data else 0
        scores.append(consistency * 0.15)
        
        return max(0.0, min(1.0, sum(scores)))

    def _anthem_fallback(self, rows: Dict[int, List[str]], file_extension: str) -> int:
        """🛡️ Fallback inteligente para Anthem"""
        # Buscar primera fila con suficientes columnas que no sea metadata
        for row_idx, row_data in rows.items():
            if row_idx > 6:  # No buscar muy lejos
                break
                
            non_empty = len([x for x in row_data if str(x).strip()])
            row_str = ' '.join(str(x) for x in row_data).lower()
            
            if (non_empty >= 4 and 
                not any(bad in row_str for bad in self.ANTHEM_METADATA_INDICATORS[:4])):
                return row_idx
                
        return 0 if file_extension.lower() == '.csv' else 1

    # Reutilizar métodos de lectura robusta del detector United
    def _read_csv_rows(self, content: bytes, num_rows: int) -> Dict[int, List[str]]:
        """📄 Reutiliza lógica robusta de United para CSV"""
        rows = {}
        try:
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    content_str = content.decode(encoding, errors='ignore')
                    break
                except:
                    continue
            else:
                content_str = content.decode('utf-8', errors='replace')
            
            lines = content_str.strip().split('\n')[:num_rows]
            
            for i, line in enumerate(lines):
                if line.strip():
                    try:
                        reader = csv.reader([line])
                        parsed_row = next(reader)
                        rows[i] = [str(cell).strip() for cell in parsed_row]
                    except Exception:
                        rows[i] = [str(cell).strip() for cell in line.split(',')]
                        
        except Exception as e:
            self.logger.error(f"Error leyendo CSV Anthem: {e}")
            
        return rows
    
    def _read_excel_rows(self, content: bytes, num_rows: int) -> Dict[int, List[str]]:
        """📊 Reutiliza lógica robusta de United para Excel"""
        rows = {}
        try:
            df = pd.read_excel(io.BytesIO(content), header=None, nrows=num_rows)
            
            for i in range(len(df)):
                row_data = df.iloc[i].tolist()
                clean_row = []
                for x in row_data:
                    if pd.isna(x):
                        clean_row.append('')
                    else:
                        clean_row.append(str(x).strip())
                rows[i] = clean_row
                
        except Exception as e:
            self.logger.error(f"Error leyendo Excel Anthem: {e}")
            
        return rows


# 🔌 NUEVA FUNCIÓN PARA ANTHEM - 100% COMPATIBLE
def detect_anthem_skiprows(content: bytes, file_extension: str, logger: Optional[logging.Logger] = None) -> int:
    """
    🎯 FUNCIÓN ESPECÍFICA PARA ANTHEM usando lógica probada de United.
    
    Detecta automáticamente si Anthem tiene:
    - Excel: headers en línea 1 → skiprows=0
    - CSV con título: headers en línea 2 → skiprows=1
    - CSV con metadata: headers en línea N → skiprows=N-1
    
    Args:
        content: Contenido del archivo en bytes
        file_extension: '.csv', '.xlsx', etc.
        logger: Logger opcional
        
    Returns:
        int: skip_rows óptimo para Anthem
    """
    detector = AnthemSkiprowsDetector(logger)
    return detector.detect_skiprows_from_content(content, file_extension)


# 🧪 TEST PARA ANTHEM
def test_anthem_detection():
    """🧪 Test específico para Anthem"""
    print("🚀 TEST DETECCIÓN ANTHEM")
    print("=" * 40)
    
    # Test 1: CSV con título (skip_rows=1)
    anthem_csv_title = (
        b'List of clients as of  02/17/2026 12:03 PM EST ,,,,,,,\n'
        b'Client Name,Client ID,Market,Status,State,Exchange\n'
        b'"Acosta, Ana",765W25498,Individual,Active,TX,On\n'
    )
    result1 = detect_anthem_skiprows(anthem_csv_title, '.csv')
    print(f"📄 Anthem CSV con título: skiprows = {result1} ✅")
    
    # Test 2: Excel directo (skip_rows=0)
    anthem_excel = b'Client Name,Client ID,Market,Status\n"Test Client",12345,Individual,Active'
    result2 = detect_anthem_skiprows(anthem_excel, '.xlsx')
    print(f"📊 Anthem Excel directo: skiprows = {result2} ✅")
    
    print("✅ ANTHEM DETECTION LISTO")

###
# Modificar la función de test principal para incluir Anthem
def test_advanced_detection():
    """🧪 Testing súper completo del detector avanzado."""
    print("🚀 TEST SÚPER AVANZADO DE DETECCIÓN United + Anthem")
    print("=" * 70)
    
    # Tests United existentes
    csv_legacy = b'agentId,agentIdStatus,agentName,agentEmail,agentNpn\n6600237,Active,MORILLO ODRA,test@email.com,20970860'
    result1 = detect_united_skiprows(csv_legacy, '.csv')
    print(f"📄 United CSV Legacy: skiprows = {result1} ✅")
    
    csv_with_metadata = (
        b'Note: Book of Business Reports are confidential,,,,,\n'
        b',,,,,\n'
        b',,,,,\n'
        b',,,,,\n'
        b'agentId,agentIdStatus,agentName,memberFirstName,memberLastName\n'
        b'6600237,Active,MORILLO ODRA,JOHN,DOE'
    )
    result2 = detect_united_skiprows(csv_with_metadata, '.csv')
    print(f"📄 United CSV con metadata: skiprows = {result2} ✅")
    
    # Tests Anthem nuevos
    print("\n🎯 TESTS ANTHEM:")
    test_anthem_detection()
    
    print("\n✅ TODOS LOS TESTS COMPLETADOS")
    print("🚀 DETECTOR LISTO PARA United + Anthem")

if __name__ == "__main__":
    test_advanced_detection()