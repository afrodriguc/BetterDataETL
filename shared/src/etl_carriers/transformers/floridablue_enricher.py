"""
Servicio de enriquecimiento para datos de FloridaBlue.
Combina datos del reporte principal con datos del archivo aligned.
"""

import io
import pandas as pd
from typing import Dict, Optional, Any
from google.cloud import storage

from etl_carriers.utils import parse_date, clean_phone, normalize_member_type


class FloridaBlueEnricher:
    """Enriquece registros de FloridaBlue con datos de archivos aligned."""
    
    # Columnas a extraer del archivo aligned para enriquecimiento
    ENRICHMENT_COLUMNS = [
        'HCC_ID', 
        'MEMBER_DOB', 
        'MEMBER_EMAIL_ADDRESS', 
        'MEMBER_HOME_PHN', 
        'CODE_DESC', 
        'ACTIVE_MEMBER_COUNT'
    ]
    
    def __init__(self, storage_client: storage.Client, bucket_name: str):
        self.storage_client = storage_client
        self.bucket_name = bucket_name
        self.bucket = storage_client.bucket(bucket_name)
        self._cache: Dict[str, pd.DataFrame] = {}
    
    def get_aligned_file_path(self, florida_blue_path: str) -> str:
        """
        Construye el path del archivo aligned correspondiente.
        
        Ejemplo:
            Input:  Data_Lake/Bronze/Florida_blue/2026/01/file.csv
            Output: Data_Lake/Bronze/Florida_blue_aligned/2026/01/file.csv
        """
        aligned_path = florida_blue_path.replace('/Florida_blue/', '/Florida_blue_aligned/')
        aligned_path = aligned_path.replace('/florida_blue/', '/Florida_blue_aligned/')
        return aligned_path
    
    def check_aligned_file_exists(self, aligned_path: str) -> bool:
        """Verifica si existe el archivo aligned."""
        try:
            blob = self.bucket.blob(aligned_path)
            return blob.exists()
        except Exception as e:
            print(f"Error verificando archivo aligned: {e}")
            return False
    
    def load_aligned_data(self, aligned_path: str) -> Optional[pd.DataFrame]:
        """
        Carga el archivo aligned y retorna un DataFrame indexado por HCC_ID.
        Usa cache para evitar cargar el mismo archivo múltiples veces.
        """
        # Verificar cache
        if aligned_path in self._cache:
            return self._cache[aligned_path]
        
        try:
            blob = self.bucket.blob(aligned_path)
            content = blob.download_as_bytes()
            
            if aligned_path.endswith('.xlsx') or aligned_path.endswith('.XLSX'):
                df = pd.read_excel(io.BytesIO(content))
            else:
                df = pd.read_csv(io.BytesIO(content))
            
            df = df.dropna(how='all')
            
            if 'HCC_ID' not in df.columns:
                print(f"⚠️ Archivo aligned no tiene columna HCC_ID: {aligned_path}")
                return None
            
            # Seleccionar columnas de enriquecimiento
            available_columns = [col for col in self.ENRICHMENT_COLUMNS if col in df.columns]
            df_enrichment = df[available_columns].copy()
            
            # Limpiar HCC_ID
            df_enrichment['HCC_ID'] = df_enrichment['HCC_ID'].astype(str).str.strip()
            df_enrichment = df_enrichment.drop_duplicates(subset=['HCC_ID'], keep='first')
            df_enrichment = df_enrichment.set_index('HCC_ID')
            
            # Guardar en cache
            self._cache[aligned_path] = df_enrichment
            
            print(f"📊 Datos de enriquecimiento cargados: {len(df_enrichment)} registros únicos")
            
            return df_enrichment
            
        except Exception as e:
            print(f"Error cargando archivo aligned: {e}")
            return None
    
    def enrich_record(
        self, 
        record: Dict[str, Any], 
        aligned_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Enriquece un registro de FloridaBlue con datos del archivo aligned.
        
        Args:
            record: Registro Silver a enriquecer
            aligned_data: DataFrame con datos de enriquecimiento indexado por HCC_ID
        
        Returns:
            Registro enriquecido
        """
        if aligned_data is None:
            return record
        
        hcc_id = record.get('policy_id')
        if hcc_id is None:
            return record
        
        hcc_id_clean = str(hcc_id).strip()
        
        if hcc_id_clean not in aligned_data.index:
            return record
        
        try:
            aligned_row = aligned_data.loc[hcc_id_clean]
            
            # Enriquecer DOB
            if record.get('member_dob') is None and 'MEMBER_DOB' in aligned_row.index:
                dob_value = aligned_row['MEMBER_DOB']
                if not pd.isna(dob_value):
                    record['member_dob'] = parse_date(dob_value)
            
            # Enriquecer Email
            if record.get('member_email') is None and 'MEMBER_EMAIL_ADDRESS' in aligned_row.index:
                email_value = aligned_row['MEMBER_EMAIL_ADDRESS']
                if not pd.isna(email_value):
                    record['member_email'] = str(email_value).strip()
            
            # Enriquecer Phone
            if record.get('member_phone') is None and 'MEMBER_HOME_PHN' in aligned_row.index:
                phone_value = aligned_row['MEMBER_HOME_PHN']
                if not pd.isna(phone_value):
                    record['member_phone'] = clean_phone(phone_value)
            
            # Enriquecer member_type desde CODE_DESC
            if record.get('member_type') is None and 'CODE_DESC' in aligned_row.index:
                code_desc = aligned_row['CODE_DESC']
                if not pd.isna(code_desc):
                    record['member_type'] = normalize_member_type(code_desc)
            
            # Enriquecer member_count
            if record.get('member_count') is None and 'ACTIVE_MEMBER_COUNT' in aligned_row.index:
                member_count_value = aligned_row['ACTIVE_MEMBER_COUNT']
                if not pd.isna(member_count_value):
                    try:
                        record['member_count'] = int(float(member_count_value))
                    except (ValueError, TypeError):
                        pass
            
            # Marcar que fue enriquecido
            if record.get('_extra_fields') is None:
                record['_extra_fields'] = {}
            record['_extra_fields']['enriched_from_aligned'] = True
            
            # Agregar CODE_DESC a extra_fields
            if 'CODE_DESC' in aligned_row.index:
                code_desc = aligned_row['CODE_DESC']
                if not pd.isna(code_desc):
                    record['_extra_fields']['code_desc'] = str(code_desc).strip()
            
        except Exception as e:
            print(f"Error enriqueciendo registro {hcc_id}: {e}")
        
        return record
    
    def clear_cache(self):
        """Limpia el cache de archivos aligned."""
        self._cache.clear()
