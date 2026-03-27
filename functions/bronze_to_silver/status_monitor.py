"""
Status Monitor - Detecta valores nuevos de status/payment_status.
Registra valores desconocidos y envía alertas via Pub/Sub.
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Set, Optional
from google.cloud import bigquery, pubsub_v1

from etl_carriers.config import PROJECT_ID


class StatusMonitor:
    """Monitor de valores de status y payment_status."""
    
    MONITORED_FIELDS = ['status', 'payment_status']
    
    def __init__(self, project_id: str = PROJECT_ID):
        self.project_id = project_id
        self.bq_client = bigquery.Client(project=project_id)
        self.registry_table = f"{project_id}.metadata.status_values_registry"
        self._known_values_cache: Dict[str, Dict[str, Set[str]]] = {}
        
        # Configuración Pub/Sub
        self.pubsub_enabled = os.environ.get("ENABLE_STATUS_ALERTS", "true").lower() == "true"
        self.pubsub_topic = os.environ.get("STATUS_ALERT_TOPIC", "status-changes")
        self.publisher = None
        self.topic_path = None
        
        if self.pubsub_enabled:
            try:
                self.publisher = pubsub_v1.PublisherClient()
                self.topic_path = self.publisher.topic_path(project_id, self.pubsub_topic)
                print(f"StatusMonitor: Pub/Sub habilitado - {self.topic_path}")
            except Exception as e:
                print(f"StatusMonitor: Error Pub/Sub: {e}")
                self.pubsub_enabled = False
    
    def load_known_values(self, carrier: str) -> Dict[str, Set[str]]:
        """Carga valores conocidos de status/payment_status para un carrier."""
        if carrier in self._known_values_cache:
            return self._known_values_cache[carrier]
        
        known_values = {field: set() for field in self.MONITORED_FIELDS}
        
        try:
            query = f"""
                SELECT field_name, field_value
                FROM `{self.registry_table}`
                WHERE carrier = @carrier AND is_active = TRUE
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("carrier", "STRING", carrier)
                ]
            )
            
            results = self.bq_client.query(query, job_config=job_config).result()
            
            for row in results:
                if row.field_name in known_values:
                    known_values[row.field_name].add(row.field_value)
            
            self._known_values_cache[carrier] = known_values
            total = sum(len(v) for v in known_values.values())
            if total > 0:
                print(f"StatusMonitor: {total} valores conocidos para {carrier}")
            
        except Exception as e:
            print(f"StatusMonitor: Error cargando valores: {e}")
        
        return known_values
    
    def detect_new_values(
        self, 
        carrier: str, 
        records: List[Dict], 
        file_name: str,
        aor_id: str = None, 
        aor_name: str = None
    ) -> List[Dict]:
        """Detecta valores nuevos de status/payment_status."""
        known_values = self.load_known_values(carrier)
        
        if not any(known_values.values()):
            print(f"StatusMonitor: No hay valores registrados para {carrier}, saltando detección")
            return []
        
        new_values_found: Dict[str, Dict] = {}
        
        for record in records:
            policy_id = record.get('policy_id', 'unknown')
            
            for field_name in self.MONITORED_FIELDS:
                field_value = record.get(field_name)
                if not field_value or str(field_value).strip() == '':
                    continue
                
                field_value_str = str(field_value).strip()
                
                if field_value_str not in known_values.get(field_name, set()):
                    key = f"{field_name}|{field_value_str}"
                    
                    if key not in new_values_found:
                        new_values_found[key] = {
                            'field_name': field_name,
                            'field_value': field_value_str,
                            'occurrence_count': 0,
                            'sample_policy_ids': []
                        }
                    
                    new_values_found[key]['occurrence_count'] += 1
                    if len(new_values_found[key]['sample_policy_ids']) < 5 and policy_id != 'unknown':
                        new_values_found[key]['sample_policy_ids'].append(str(policy_id))
        
        new_values_list = list(new_values_found.values())
        
        if new_values_list:
            print(f"StatusMonitor: {len(new_values_list)} valor(es) nuevo(s) para {carrier}")
            for nv in new_values_list:
                print(f"  - {nv['field_name']}: '{nv['field_value']}' ({nv['occurrence_count']}x)")
            
            self._log_to_registry(carrier, new_values_list, file_name)
            self._publish_alert(carrier, new_values_list, file_name, aor_id, aor_name)
        
        return new_values_list
    
    def _log_to_registry(self, carrier: str, new_values: List[Dict], file_name: str):
        """Registra valores nuevos en BigQuery."""
        for nv in new_values:
            try:
                query = f"""
                    INSERT INTO `{self.registry_table}` 
                    (id, carrier, field_name, field_value, is_active, first_seen_file, occurrence_count, notes)
                    VALUES (GENERATE_UUID(), @carrier, @field_name, @field_value, FALSE, @file_name, @count, 'Detectado automaticamente')
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("carrier", "STRING", carrier),
                        bigquery.ScalarQueryParameter("field_name", "STRING", nv['field_name']),
                        bigquery.ScalarQueryParameter("field_value", "STRING", nv['field_value']),
                        bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
                        bigquery.ScalarQueryParameter("count", "INT64", nv['occurrence_count']),
                    ]
                )
                self.bq_client.query(query, job_config=job_config).result()
                print(f"StatusMonitor: Registrado {nv['field_name']}='{nv['field_value']}' en registry")
            except Exception as e:
                print(f"StatusMonitor: Error registrando valor: {e}")
    
    def _publish_alert(
        self, 
        carrier: str, 
        new_values: List[Dict], 
        file_name: str, 
        aor_id: str, 
        aor_name: str
    ):
        """Publica alerta a Pub/Sub."""
        if not self.pubsub_enabled or not self.publisher:
            print("StatusMonitor: Pub/Sub no habilitado, saltando alerta")
            return
        
        try:
            alert_data = {
                'carrier': carrier,
                'file_name': file_name,
                'aor_id': aor_id,
                'aor_name': aor_name,
                'detected_at': datetime.utcnow().isoformat(),
                'new_values': new_values
            }
            
            message_bytes = json.dumps(alert_data, default=str).encode('utf-8')
            future = self.publisher.publish(self.topic_path, message_bytes, carrier=carrier)
            message_id = future.result(timeout=30)
            print(f"StatusMonitor: Alerta publicada a Pub/Sub: {message_id}")
        except Exception as e:
            print(f"StatusMonitor: Error publicando alerta: {e}")
