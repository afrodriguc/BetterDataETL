"""
Loaders para cargar datos transformados a BigQuery.
"""

import json
from typing import Dict, List, Any, Optional
from google.cloud import bigquery

from etl_carriers.config import PROJECT_ID, SILVER_DATASET


class BigQueryLoader:
    """Cargador de datos a BigQuery."""
    
    def __init__(self, project_id: str = PROJECT_ID):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
    
    def load_to_policies(
        self, 
        records: List[Dict[str, Any]], 
        table_id: Optional[str] = None,
        batch_size: int = 500
    ) -> int:
        """
        Carga registros a la tabla silver.policies usando batch inserts.
        
        Args:
            records: Lista de registros transformados
            table_id: ID de la tabla (default: {project_id}.silver.policies)
            batch_size: Tamaño del batch para inserción (default: 500)
        
        Returns:
            Número de filas cargadas exitosamente
        """
        if not records:
            print("No hay registros para cargar")
            return 0
        
        if table_id is None:
            table_id = f"{self.project_id}.{SILVER_DATASET}.policies"
        
        # Preparar registros para streaming insert
        rows_to_insert = []
        for record in records:
            # Convertir _extra_fields a JSON string para BigQuery
            extra_fields = record.get('_extra_fields')
            if extra_fields and isinstance(extra_fields, dict):
                extra_fields_json = json.dumps(extra_fields)
            else:
                extra_fields_json = None
            
            row = {
                'carrier': record.get('carrier'),
                'aor_id': record.get('aor_id'),
                'aor_name': record.get('aor_name'),
                'extraction_date': str(record.get('extraction_date')) if record.get('extraction_date') else None,
                'source_file': record.get('source_file'),
                'policy_id': record.get('policy_id'),
                'exchange_id': record.get('exchange_id'),
                'member_first_name': record.get('member_first_name'),
                'member_middle_name': record.get('member_middle_name'),
                'member_last_name': record.get('member_last_name'),
                'member_dob': str(record.get('member_dob')) if record.get('member_dob') else None,
                'member_phone': record.get('member_phone'),
                'member_email': record.get('member_email'),
                'member_address': record.get('member_address'),
                'member_city': record.get('member_city'),
                'member_state': record.get('member_state'),
                'member_zip': record.get('member_zip'),
                'member_county': record.get('member_county'),
                'effective_date': str(record.get('effective_date')) if record.get('effective_date') else None,
                'term_date': str(record.get('term_date')) if record.get('term_date') else None,
                'paid_through_date': str(record.get('paid_through_date')) if record.get('paid_through_date') else None,
                'broker_effective_date': str(record.get('broker_effective_date')) if record.get('broker_effective_date') else None,
                'broker_term_date': str(record.get('broker_term_date')) if record.get('broker_term_date') else None,
    
                'premium': float(record.get('premium')) if record.get('premium') is not None else None,
                'premium_responsibility': float(record.get('premium_responsibility')) if record.get('premium_responsibility') is not None else None,
                'status': record.get('status'),
                'payment_status': record.get('payment_status'),
                'on_off_exchange': record.get('on_off_exchange'),
                'autopay_status': record.get('autopay_status'),
                'member_count': int(record.get('member_count')) if record.get('member_count') is not None else None,
                'member_type': record.get('member_type'),
                'is_primary_member': record.get('is_primary_member'),
                '_extra_fields': extra_fields_json,

            }
            rows_to_insert.append(row)
        
        # Obtener referencia a la tabla
        table_ref = self.client.get_table(table_id)
        
        loaded = 0
        errors_total = 0
        
        # Insertar en batches
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            
            try:
                errors = self.client.insert_rows_json(table_ref, batch)
                if errors:
                    errors_total += len(errors)
                    if errors_total <= 5:
                        print(f"Errores en batch {i//batch_size + 1}: {errors[:2]}")
                else:
                    loaded += len(batch)
                
            except Exception as e:
                errors_total += len(batch)
                print(f"Error en batch {i//batch_size + 1}: {str(e)[:200]}")
                continue
        
        if errors_total > 0:
            print(f"Total errores de inserción: {errors_total}")
        
        return loaded
    
    def load_to_special_table(
        self, 
        records: List[Dict[str, Any]], 
        table_name: str,
        batch_size: int = 500
    ) -> int:
        """
        Carga registros a una tabla especial usando streaming insert.
        
        Args:
            records: Lista de registros transformados
            table_name: Nombre de la tabla (sin 'silver.')
            batch_size: Tamaño del batch para inserción
        
        Returns:
            Número de filas cargadas exitosamente
        """
        if not records:
            print("No hay registros para cargar")
            return 0
        
        table_ref = self.client.dataset(SILVER_DATASET).table(table_name)
        
        loaded = 0
        errors_insert = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            try:
                errors = self.client.insert_rows_json(table_ref, batch)
                if errors:
                    errors_insert += len(errors)
                    print(f"PRIMER ERROR DETALLADO: {errors[0]}")
                    if errors_insert <= 5:
                        print(f"Errores en batch {i//batch_size}: {errors[:2]}")
                else:
                    loaded += len(batch)
                
                if (i + batch_size) % 5000 == 0:
                    print(f"Progreso: {i + batch_size}/{len(records)} filas procesadas")
                    
            except Exception as e:
                errors_insert += len(batch)
                print(f"Error en batch {i//batch_size}: {str(e)[:200]}")
                continue
        
        return loaded
    
    def delete_by_source_file(self, table_id: str, source_file: str) -> Dict[str, Any]:
        """
        Elimina registros de una tabla por nombre de archivo fuente.
        
        Args:
            table_id: ID completo de la tabla
            source_file: Nombre del archivo fuente
        
        Returns:
            Dict con resultado de la operación
        """
        delete_query = f"""
            DELETE FROM `{table_id}`
            WHERE source_file = @source_file
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source_file", "STRING", source_file)
            ]
        )
        
        try:
            print(f"🗑️  Ejecutando DELETE en {table_id}...")
            print(f"   WHERE source_file = '{source_file}'")
            
            query_job = self.client.query(delete_query, job_config=job_config)
            query_job.result()
            
            deleted_rows = 0
            if hasattr(query_job, 'num_dml_affected_rows'):
                deleted_rows = query_job.num_dml_affected_rows
            
            return {
                'status': 'success',
                'deleted_rows': deleted_rows
            }
        
        except Exception as e:
            print(f"❌ Error ejecutando DELETE: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def truncate_table(self, table_id: str) -> bool:
        """
        Trunca una tabla (elimina todos los datos).
        
        Args:
            table_id: ID completo de la tabla
        
        Returns:
            True si fue exitoso
        """
        try:
            query = f"TRUNCATE TABLE `{table_id}`"
            self.client.query(query).result()
            print(f"✅ Tabla truncada: {table_id}")
            return True
        except Exception as e:
            print(f"❌ Error truncando {table_id}: {e}")
            return False
        
    def validate_file_not_processed(self, file_name: str, extraction_date: str, 
                                carrier: str, table_name: str = 'policies') -> bool:
        """
        Verifica si un archivo ya fue procesado para evitar duplicados.
        
        Args:
            file_name: Nombre del archivo fuente
            extraction_date: Fecha de extracción del archivo
            carrier: Nombre del carrier
            table_name: Nombre de la tabla (sin 'silver.')
        
        Returns:
            bool: True si NO fue procesado (es seguro procesar), False si es duplicado
        """
        try:
            # Query optimizada para verificar existencia
            carrier_filter = "AND carrier = @carrier" if table_name == 'policies' else ""
            query = f"""
            SELECT 1 as exists_flag
            FROM `{self.project_id}.{SILVER_DATASET}.{table_name}`
            WHERE source_file = @file_name
            AND DATE(extraction_date) = DATE(@extraction_date)
            {carrier_filter}
            LIMIT 1
            """
            
            params = [
                bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
                bigquery.ScalarQueryParameter("extraction_date", "STRING", str(extraction_date)),
            ]
            if table_name == 'policies':
                params.append(bigquery.ScalarQueryParameter("carrier", "STRING", carrier))

            job_config = bigquery.QueryJobConfig(query_parameters=params)
                
            result = self.client.query(query, job_config=job_config)
            rows = list(result)
            
            is_duplicate = len(rows) > 0
            
            if is_duplicate:
                print(f"🔍 DUPLICATE FOUND: {file_name} ya existe en silver.{table_name}")
            else:
                print(f"✅ FILE VALIDATION: {file_name} es nuevo, procesando...")
            
            return not is_duplicate  # True = seguro procesar, False = duplicado
            
        except Exception as e:
            print(f"⚠️ Error verificando duplicados: {e}")
            # FALLBACK: En caso de error, permitir procesamiento para no bloquear el sistema
            return True