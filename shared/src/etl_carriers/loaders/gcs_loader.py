"""
Loader para leer archivos desde Google Cloud Storage.
"""

import io
import pandas as pd
from typing import Optional, List, Dict, Any
from google.cloud import storage
from io import StringIO

from etl_carriers.config import CARRIER_MAPPINGS, BUCKET_NAME, BRONZE_PREFIX
from etl_carriers.utils.skiprows_detector import detect_united_skiprows, detect_anthem_skiprows
from etl_carriers.config import get_carrier_from_folder


class GCSLoader:
    """Cargador de archivos desde Google Cloud Storage."""
    
    def __init__(self, project_id: str, bucket_name: str = BUCKET_NAME):
        self.storage_client = storage.Client(project=project_id)
        self.bucket_name = bucket_name
        self.bucket = self.storage_client.bucket(bucket_name)
    
    def read_file(self, file_path: str, carrier: str = None) -> pd.DataFrame:
        """
        Lee un archivo de GCS (CSV o Excel).
        
        Args:
            file_path: Ruta del archivo en el bucket
            carrier: Nombre del carrier (para determinar skip_rows)
        
        Returns:
            DataFrame con los datos
        """
        blob = self.bucket.blob(file_path)
        content = blob.download_as_bytes()
        
        # Determinar skip_rows dinámicamente para United y Anthem, desde configuración para otros
        skip_rows = 0
        if carrier == 'united':
            # Para United, detectar dinámicamente basándose en contenido
            file_extension = '.xlsx' if (file_path.endswith('.xlsx') or file_path.endswith('.XLSX')) else '.csv'
            skip_rows = detect_united_skiprows(content, file_extension, logger=getattr(self, 'logger', None))
        elif carrier == 'anthem':
            # Para Anthem, detectar dinámicamente (Excel vs CSV con título)
            file_extension = '.xlsx' if (file_path.endswith('.xlsx') or file_path.endswith('.XLSX')) else '.csv'
            skip_rows = detect_anthem_skiprows(content, file_extension, logger=getattr(self, 'logger', None))
        elif carrier and carrier in CARRIER_MAPPINGS:
            # Para otros carriers, usar configuración estática
            skip_rows = CARRIER_MAPPINGS[carrier].get('skip_rows', 0)

        # Leer según extensión
        if file_path.endswith('.xlsx') or file_path.endswith('.XLSX'):
            # Intentar múltiples engines con fallback automático
            df = None
            errors = []
            for engine in ['openpyxl', 'xlrd', 'calamine']:
                try:
                    df = pd.read_excel(io.BytesIO(content), skiprows=skip_rows, engine=engine)
                    break
                except Exception as e:
                    errors.append(f"{engine}: {str(e)[:80]}")
                    continue

            if df is None:
                try:
                    import lxml
                    tables = pd.read_html(io.BytesIO(content), encoding='utf-8')
                    if tables:
                        df = tables[0]
                except Exception as e:
                    errors.append(f"html: {str(e)[:80]}")

            if df is None:
                raise ValueError(f"No se pudo leer el archivo Excel. Intentos: {'; '.join(errors)}")

        else:
            # Para CSV: SIEMPRE usar manejo robusto de encoding (resuelve problemas como Molina MPT)
            df = self._read_csv_robust(content, skip_rows)
            # Limpiar artefactos BOM de nombres de columnas
            # (afecta archivos UTF-16 con BOM corrupto, ej: Anthem extended)
            df.columns = [col.lstrip('\ufeff\ufffd\ufffe').strip() for col in df.columns]
            
            # Eliminar filas completamente vacías
            df = df.dropna(how='all')

        return df
    
    def _read_csv_robust(self, content: bytes, skip_rows: int = 0) -> pd.DataFrame:
        """
        Lee un CSV probando múltiples encodings y separadores automáticamente.
        
        Maneja:
        - UTF-16 con BOM estándar (FF FE / FE FF)
        - UTF-16-LE sin BOM estándar (patrón de null bytes en posición impar)
        incluyendo archivos con BOM corrupto (ej: Anthem extended)
        - UTF-8, UTF-8-SIG, Latin-1, CP1252
        """
        # 1. UTF-16 con BOM estándar
        if content[:2] in (b'\xff\xfe', b'\xfe\xff'):
            try:
                content_str = content.decode('utf-16')
                sep = '\t' if '\t' in content_str[:1000] else ','
                return pd.read_csv(StringIO(content_str), skiprows=skip_rows, sep=sep, low_memory=False)  # FIX: low_memory=False
            except Exception:
                pass

        # 2. Detectar UTF-16-LE sin BOM estándar por patrón de null bytes
        #    En UTF-16-LE los caracteres ASCII tienen \x00 en posición impar.
        #    Anthem 'extended' tiene BOM corrupto (6 bytes EF BF BD EF BF BD)
        #    seguido de contenido UTF-16-LE puro.
        sample = content[:500]
        if len(sample) >= 10:
            odd_nulls = sum(1 for i in range(1, len(sample), 2) if sample[i] == 0)
            null_ratio = odd_nulls / (len(sample) // 2)
            if null_ratio > 0.5:
                for bom_offset in [0, 2, 4, 6]:
                    try:
                        content_str = content[bom_offset:].decode('utf-16-le')
                        # Validar que el contenido empieza con ASCII legible.
                        # Si no, el offset no saltó el BOM corrupto correctamente.
                        first_char = content_str[0] if content_str else ''
                        if not (first_char.isascii() and first_char.isprintable()):
                            continue
                        sep = '\t' if '\t' in content_str[:1000] else ','
                        return pd.read_csv(
                            StringIO(content_str), skiprows=skip_rows, sep=sep, low_memory=False  # FIX: low_memory=False
                        )
                    except Exception:
                        continue

        # 3. Encodings clásicos (CSV comma-sep)
        encodings = ['utf-8', 'utf-8-sig', 'utf-16-le', 'utf-16-be', 'latin-1', 'cp1252']
        last_error = None
        for encoding in encodings:
            try:
                content_str = content.decode(encoding)
                return pd.read_csv(StringIO(content_str), skiprows=skip_rows, low_memory=False)  # FIX: low_memory=False
            except Exception as e:
                last_error = e
                continue

        raise ValueError(f"No se pudo leer el CSV. Último error: {last_error}")


    def file_exists(self, file_path: str) -> bool:
        """Verifica si un archivo existe en GCS."""
        try:
            blob = self.bucket.blob(file_path)
            return blob.exists()
        except Exception:
            return False
    
    def list_files(
        self, 
        prefix: str = BRONZE_PREFIX,
        carrier_filter: str = None,
        extensions: tuple = ('.csv', '.CSV', '.xlsx', '.XLSX')
    ) -> List[Dict[str, Any]]:
        """
        Lista archivos en un path de GCS.
        
        Args:
            prefix: Prefijo de búsqueda
            carrier_filter: Filtrar por carrier específico
            extensions: Extensiones válidas
        
        Returns:
            Lista de dicts con información de archivos
        """
        files = []
        
        blobs = self.bucket.list_blobs(prefix=prefix)
        
        for blob in blobs:
            # Ignorar carpetas
            if blob.name.endswith('/'):
                continue
            
            # Verificar extensión
            if not blob.name.endswith(extensions):
                continue
            
            # Extraer información
            parts = blob.name.split('/')
            if len(parts) < 4:
                continue
            
            folder = parts[2] if len(parts) > 2 else 'unknown'
            
            # Filtrar por carrier si es necesario
            if carrier_filter:
                from etl_carriers.config import get_carrier_from_folder
                carrier = get_carrier_from_folder(folder)
                if carrier != carrier_filter:
                    continue
            
            files.append({
                'path': blob.name,
                'name': parts[-1],
                'folder': folder,
                'size': blob.size,
                'updated': blob.updated
            })
        
        return files
    
    def download_as_text(self, file_path: str) -> str:
        """Descarga un archivo como texto."""
        blob = self.bucket.blob(file_path)
        return blob.download_as_text()
    
    def download_as_bytes(self, file_path: str) -> bytes:
        """Descarga un archivo como bytes."""
        blob = self.bucket.blob(file_path)
        return blob.download_as_bytes()