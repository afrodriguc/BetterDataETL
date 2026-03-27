"""
Servicio de enriquecimiento para datos de Cigna.
Combina datos de Cigna_Pending con Cigna_active_termed para evitar duplicados
y asegurar que siempre se tenga la información más completa.

Lógica:
- Cigna_active_termed: Fuente principal (datos completos) → va directo a silver.policies
- Cigna_Pending: Solo se insertan pólizas que NO existan ya en active_termed

La combinación se hace por AOR y fecha de extracción:
- Cigna_Pending/2026/01/cigna_2026-01-16_21072733.xlsx
- Cigna_active_termed/2026/01/cigna_2026-01-16_21072733.xlsx
"""

import io
import pandas as pd
from typing import Dict, Optional, Any, List, Set
from google.cloud import storage

from etl_carriers.utils import parse_date, clean_phone, split_full_name


class CignaEnricher:
    """
    Enriquece y combina datos de Cigna de múltiples fuentes.
    
    Cuando se procesa un archivo de Cigna_Pending, verifica si existe
    el archivo correspondiente en Cigna_active_termed y filtra los
    registros que ya existen para evitar duplicados.
    """
    
    # Mapeo de columnas de Cigna_Pending (archivo con pocas columnas)
    PENDING_MAPPING = {
        'customer_number': 'Customer Number (Case ID)',
        'application_id': 'Application ID',
        'primary_name': 'Primary Name',
        'agent_npn': 'Agent NPN',
        'producer_code': 'Producer Code',
        'agent_name': 'Agent Name',
        'policy_status': 'Policy Status',
        'received_date': 'Received Date',
        'state': 'State',
    }
    
    # Mapeo de columnas de Cigna_active_termed (archivo completo)
    ACTIVE_MAPPING = {
        'subscriber_id': 'Subscriber ID (Detail Case #)',
        'customer_number': 'Customer Number (Case ID)',
        'application_id': 'Application Id',
        'primary_first_name': 'Primary First Name',
        'primary_last_name': 'Primary Last Name',
        'agent_npn': 'Writing Agent NPN',
        'agent_name': 'Writing Agent',
        'product_type': 'Product Type',
        'on_off_exchange': 'ON/OFF Exchange',
        'subsidy': 'Subsidy',
        'total_premium': 'Total Premium',
        'aptc': 'APTC',
        'premium_responsibility': 'Premium - Customer Responsibility',
        'plan_name': 'Plan Name',
        'policy_status': 'Policy Status',
        'effective_date': 'Effective Date',
        'received_date': 'Date Application Received',
        'renewal_month': 'Renewal Month',
        'paid_through_date': 'Paid Through Date',
        'term_date': 'Termination Date',
        'state': 'State',
        'email': 'Customer Email Address',
        'phone': 'Customer Phone Number',
        'coverage_status': 'Coverage Status',
        'agent_start_date': 'Agent Start Date',
        'agent_end_date': 'Agent End Date',
    }
    
    def __init__(self, storage_client: storage.Client, bucket_name: str):
        self.storage_client = storage_client
        self.bucket_name = bucket_name
        self.bucket = storage_client.bucket(bucket_name)
        self._cache: Dict[str, Set[str]] = {}
    
    def _clean_application_id(self, app_id: Any) -> str:
        """
        Limpia el Application ID removiendo el sufijo -X.
        Cigna envía IDs como "5266873188-3", normalizamos a "5266873188".
        """
        if app_id is None or pd.isna(app_id):
            return ''
        
        app_str = str(app_id).strip()
        if '-' in app_str:
            parts = app_str.rsplit('-', 1)
            if len(parts) == 2 and parts[1].isdigit():
                return parts[0]
        return app_str
    
    def get_active_file_path(self, pending_path: str) -> str:
        """
        Construye el path del archivo active_termed correspondiente.
        
        Ejemplo:
            Input:  Data_Lake/Bronze/Cigna_Pending/2026/01/cigna_2026-01-16_21072733.xlsx
            Output: Data_Lake/Bronze/Cigna_active_termed/2026/01/cigna_2026-01-16_21072733.xlsx
        """
        active_path = pending_path.replace('/Cigna_Pending/', '/Cigna_active_termed/')
        active_path = active_path.replace('/cigna_pending/', '/Cigna_active_termed/')
        return active_path
    
    def get_pending_file_path(self, active_path: str) -> str:
        """
        Construye el path del archivo pending correspondiente.
        """
        pending_path = active_path.replace('/Cigna_active_termed/', '/Cigna_Pending/')
        pending_path = pending_path.replace('/cigna_active_termed/', '/Cigna_Pending/')
        return pending_path
    
    def check_file_exists(self, file_path: str) -> bool:
        """Verifica si existe un archivo en GCS."""
        try:
            blob = self.bucket.blob(file_path)
            return blob.exists()
        except Exception as e:
            print(f"Error verificando archivo: {e}")
            return False
    
    def load_active_application_ids(self, active_path: str) -> Set[str]:
        """
        Carga los Application IDs del archivo active_termed.
        Usa cache para evitar cargar el mismo archivo múltiples veces.
        """
        if active_path in self._cache:
            return self._cache[active_path]
        
        try:
            blob = self.bucket.blob(active_path)
            content = blob.download_as_bytes()
            
            if active_path.endswith('.xlsx') or active_path.endswith('.XLSX'):
                df = pd.read_excel(io.BytesIO(content))
            else:
                df = pd.read_csv(io.BytesIO(content))
            
            df = df.dropna(how='all')
            
            app_id_col = self.ACTIVE_MAPPING.get('application_id', 'Application Id')
            
            if app_id_col not in df.columns:
                print(f"[WARN] Archivo active no tiene columna {app_id_col}")
                return set()
            
            app_ids = set()
            for app_id in df[app_id_col].dropna():
                clean_id = self._clean_application_id(app_id)
                if clean_id:
                    app_ids.add(clean_id)
            
            self._cache[active_path] = app_ids
            print(f"[INFO] Application IDs cargados de active_termed: {len(app_ids)}")
            
            return app_ids
            
        except Exception as e:
            print(f"Error cargando archivo active_termed: {e}")
            return set()
    
    def filter_pending_records(
        self, 
        pending_df: pd.DataFrame,
        active_app_ids: Set[str]
    ) -> pd.DataFrame:
        """
        Filtra registros de Cigna_Pending que ya existen en active_termed.
        """
        if not active_app_ids:
            return pending_df
        
        app_id_col = self.PENDING_MAPPING.get('application_id', 'Application ID')
        
        if app_id_col not in pending_df.columns:
            print(f"[WARN] DataFrame pending no tiene columna {app_id_col}")
            return pending_df
        
        pending_df['_clean_app_id'] = pending_df[app_id_col].apply(self._clean_application_id)
        
        original_count = len(pending_df)
        filtered_df = pending_df[~pending_df['_clean_app_id'].isin(active_app_ids)].copy()
        filtered_df = filtered_df.drop(columns=['_clean_app_id'])
        
        filtered_count = len(filtered_df)
        skipped = original_count - filtered_count
        
        print(f"[INFO] Filtrado Cigna Pending: {original_count} → {filtered_count} (omitidos: {skipped} ya en active)")
        
        return filtered_df
    
    def transform_pending_to_policies(
        self, 
        row: pd.Series,
        metadata: Any
    ) -> Dict[str, Any]:
        """
        Transforma un registro de Cigna_Pending al formato de silver.policies.
        """
        full_name = row.get(self.PENDING_MAPPING['primary_name'], '')
        name_parts = split_full_name(full_name)
        
        app_id_raw = row.get(self.PENDING_MAPPING['application_id'], '')
        app_id_clean = self._clean_application_id(app_id_raw)
        
        silver_record = {
            'carrier': 'cigna',
            'aor_id': metadata.aor_id,
            'aor_name': metadata.aor_name,
            'extraction_date': metadata.extraction_date,
            'source_file': metadata.file_name,
            'policy_id': row.get(self.PENDING_MAPPING['customer_number']),
            'exchange_id': app_id_clean if app_id_clean else None,
            'member_first_name': name_parts['first'],
            'member_middle_name': name_parts['middle'],
            'member_last_name': name_parts['last'],
            'member_dob': None,
            'member_phone': None,
            'member_email': None,
            'member_address': None,
            'member_city': None,
            'member_state': row.get(self.PENDING_MAPPING['state']),
            'member_zip': None,
            'member_county': None,
            'effective_date': None,
            'term_date': None,
            'paid_through_date': None,
            'premium': None,
            'premium_responsibility': None,
            'status': row.get(self.PENDING_MAPPING['policy_status']),
            'payment_status': None,
            'on_off_exchange': None,
            'autopay_status': None,
            'member_count': None,
            'member_type': None,
            'is_primary_member': True,  # Cigna pending: cada fila es un titular
            '_extra_fields': {
                'source_type': 'cigna_pending',
                'agent_npn': str(row.get(self.PENDING_MAPPING['agent_npn'], '')),
                'agent_name': str(row.get(self.PENDING_MAPPING['agent_name'], '')),
                'producer_code': str(row.get(self.PENDING_MAPPING['producer_code'], '')),
                'received_date': str(row.get(self.PENDING_MAPPING['received_date'], '')),
            }
        }
        
        received_date = row.get(self.PENDING_MAPPING['received_date'])
        if received_date:
            parsed_date = parse_date(received_date)
            if parsed_date:
                silver_record['_extra_fields']['received_date'] = parsed_date
        
        return silver_record
    
    def transform_pending_dataframe(
        self,
        df: pd.DataFrame,
        metadata: Any,
        active_app_ids: Set[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Transforma un DataFrame de Cigna_Pending a registros Silver.
        """
        if active_app_ids:
            df = self.filter_pending_records(df, active_app_ids)
        
        records = []
        for idx, row in df.iterrows():
            try:
                record = self.transform_pending_to_policies(row, metadata)
                records.append(record)
            except Exception as e:
                print(f"Error en fila {idx}: {e}")
                continue
        
        return records
    
    def clear_cache(self):
        """Limpia el cache de Application IDs."""
        self._cache.clear()