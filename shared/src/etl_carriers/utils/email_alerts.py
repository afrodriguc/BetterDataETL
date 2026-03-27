"""
Servicio de envío de alertas por email usando SendGrid.
Usado por status-alert y schema-alert.
"""

import os
import json
import urllib.request
import urllib.error
from typing import Dict, List, Any, Optional
from datetime import datetime


# Configuración desde variables de entorno
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
ALERT_EMAIL_FROM = os.environ.get("ALERT_EMAIL_FROM", "datawase@betterwase.com")
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "").replace(";", ",").split(",")
PROJECT_ID = os.environ.get("GCP_PROJECT", "better-wase-data-2")


class EmailSender:
    """Envía emails usando SendGrid API."""
    
    def __init__(self):
        self.api_key = SENDGRID_API_KEY
        self.from_email = ALERT_EMAIL_FROM
        self.to_emails = [e.strip() for e in ALERT_EMAIL_TO if e.strip()]
    
    def send(self, subject: str, html_content: str) -> bool:
        """
        Envía un email via SendGrid.
        
        Args:
            subject: Asunto del email
            html_content: Contenido HTML del email
        
        Returns:
            True si se envió correctamente
        """
        if not self.api_key:
            print("SENDGRID_API_KEY no configurado")
            return False
        
        if not self.to_emails:
            print("ALERT_EMAIL_TO no configurado")
            return False
        
        try:
            to_list = [{"email": e} for e in self.to_emails]
            
            data = {
                "personalizations": [{"to": to_list}],
                "from": {"email": self.from_email, "name": "Data Pipeline Alerts"},
                "subject": subject,
                "content": [{"type": "text/html", "value": html_content}]
            }
            
            req = urllib.request.Request(
                "https://api.sendgrid.com/v3/mail/send",
                data=json.dumps(data).encode('utf-8'),
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                method="POST"
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                print(f"Email enviado. Status: {response.status}")
                return True
                
        except urllib.error.HTTPError as e:
            print(f"SendGrid Error: {e.code} - {e.read().decode()}")
            return False
        except Exception as e:
            print(f"Error enviando email: {e}")
            return False


class StatusAlertFormatter:
    """Formatea alertas de status para email."""
    
    @staticmethod
    def format_html(alert_data: Dict[str, Any]) -> str:
        """Genera HTML para alerta de status nuevo."""
        carrier = alert_data.get('carrier', 'Unknown').upper()
        file_name = alert_data.get('file_name', 'Unknown')
        detected_at = alert_data.get('detected_at', datetime.now().isoformat())
        aor_id = alert_data.get('aor_id', 'N/A')
        aor_name = alert_data.get('aor_name', 'N/A')
        new_values = alert_data.get('new_values', [])
        
        # Tabla de valores nuevos
        values_html = ""
        if new_values:
            values_html = '''
            <table style="width:100%;border-collapse:collapse;margin:15px 0;">
                <thead>
                    <tr style="background:#f1f1f1;">
                        <th style="padding:10px;border:1px solid #ddd;text-align:left;">Campo</th>
                        <th style="padding:10px;border:1px solid #ddd;text-align:left;">Valor Nuevo</th>
                        <th style="padding:10px;border:1px solid #ddd;text-align:center;">Ocurrencias</th>
                        <th style="padding:10px;border:1px solid #ddd;text-align:left;">Ejemplo Policy ID</th>
                    </tr>
                </thead>
                <tbody>
            '''
            
            for val in new_values:
                field_name = val.get('field_name', 'status')
                field_value = val.get('field_value', 'Unknown')
                count = val.get('occurrence_count', 0)
                sample_ids = val.get('sample_policy_ids', [])
                sample_str = ', '.join(sample_ids[:3]) if sample_ids else 'N/A'
                badge_color = '#dc3545' if field_name == 'status' else '#fd7e14'
                
                values_html += f'''
                <tr>
                    <td style="padding:10px;border:1px solid #ddd;">
                        <span style="background:{badge_color};color:white;padding:3px 8px;border-radius:3px;font-size:12px;">{field_name}</span>
                    </td>
                    <td style="padding:10px;border:1px solid #ddd;">
                        <code style="background:#f8f9fa;padding:2px 6px;border-radius:3px;">{field_value}</code>
                    </td>
                    <td style="padding:10px;border:1px solid #ddd;text-align:center;"><strong>{count}</strong></td>
                    <td style="padding:10px;border:1px solid #ddd;font-size:12px;">{sample_str}</td>
                </tr>
                '''
            values_html += '</tbody></table>'
        
        html = f'''
        <html>
        <body style="font-family:Arial,sans-serif;line-height:1.6;">
            <div style="max-width:650px;margin:0 auto;">
                <div style="background:linear-gradient(135deg,#fd7e14,#dc3545);color:white;padding:25px;text-align:center;border-radius:8px 8px 0 0;">
                    <h1 style="margin:0;font-size:24px;">⚠️ Nuevo Valor de Status Detectado</h1>
                    <p style="margin:10px 0 0 0;opacity:0.9;">Valores no registrados previamente</p>
                </div>
                
                <div style="background:#f8f9fa;padding:20px;border:1px solid #ddd;border-top:none;">
                    <table style="width:100%;">
                        <tr><td style="padding:5px 0;"><strong>Carrier:</strong></td>
                            <td><span style="background:#007bff;color:white;padding:3px 10px;border-radius:3px;">{carrier}</span></td></tr>
                        <tr><td style="padding:5px 0;"><strong>Archivo:</strong></td>
                            <td><code>{file_name}</code></td></tr>
                        <tr><td style="padding:5px 0;"><strong>AOR:</strong></td>
                            <td>{aor_name} ({aor_id})</td></tr>
                        <tr><td style="padding:5px 0;"><strong>Fecha:</strong></td>
                            <td>{detected_at}</td></tr>
                    </table>
                </div>
                
                <div style="background:white;padding:20px;border:1px solid #ddd;border-top:none;">
                    <h2 style="color:#333;margin-top:0;border-bottom:2px solid #dc3545;padding-bottom:10px;">
                        Valores Nuevos ({len(new_values)})
                    </h2>
                    {values_html}
                </div>
                
                <div style="background:#fff3cd;padding:15px;border:1px solid #ddd;border-top:none;">
                    <h3 style="color:#856404;margin:0 0 10px 0;">📋 Acciones Requeridas:</h3>
                    <ol style="color:#856404;margin:0;padding-left:20px;">
                        <li>Verificar si el valor es válido para el carrier</li>
                        <li>Si es válido, actualizar <code>status_values_registry</code> con <code>is_active = TRUE</code></li>
                        <li>Actualizar mapeo en bronze-to-silver si es necesario</li>
                    </ol>
                </div>
                
                <div style="background:#f8f9fa;padding:15px;border:1px solid #ddd;border-top:none;text-align:center;">
                    <a href="https://console.cloud.google.com/bigquery?project={PROJECT_ID}" 
                       style="background:#007bff;color:white;padding:10px 20px;text-decoration:none;border-radius:5px;display:inline-block;">
                        Ver Status Registry en BigQuery
                    </a>
                </div>
                
                <div style="background:#343a40;color:white;padding:12px;text-align:center;font-size:12px;border-radius:0 0 8px 8px;">
                    Status Monitor - Medallion Pipeline
                </div>
            </div>
        </body>
        </html>
        '''
        return html
    
    @staticmethod
    def get_subject(alert_data: Dict[str, Any]) -> str:
        """Genera el asunto del email."""
        carrier = alert_data.get('carrier', 'Unknown').upper()
        count = len(alert_data.get('new_values', []))
        return f"⚠️ Status Alert: {carrier} - {count} valor(es) nuevo(s)"


class SchemaAlertFormatter:
    """Formatea alertas de cambio de schema para email."""
    
    @staticmethod
    def format_html(alert_data: Dict[str, Any]) -> str:
        """Genera HTML para alerta de cambio de schema."""
        carrier = alert_data.get('carrier', 'Unknown').upper()
        file_name = alert_data.get('file_name', 'Unknown')
        detected_at = alert_data.get('detected_at', datetime.now().isoformat())
        version = alert_data.get('version', 'N/A')
        total_columns = alert_data.get('total_columns', 0)
        changes = alert_data.get('changes', [])
        status = alert_data.get('status', 'unknown')
        
        # Determinar color según tipo
        if status == 'new_schema':
            header_color = '#28a745'
            header_icon = '🆕'
            header_text = 'Nuevo Schema Registrado'
        else:
            header_color = '#dc3545'
            header_icon = '⚠️'
            header_text = 'Cambio de Schema Detectado'
        
        # Tabla de cambios
        changes_html = ""
        if changes:
            changes_html = '''
            <table style="width:100%;border-collapse:collapse;margin:15px 0;">
                <thead>
                    <tr style="background:#f1f1f1;">
                        <th style="padding:10px;border:1px solid #ddd;text-align:left;">Tipo</th>
                        <th style="padding:10px;border:1px solid #ddd;text-align:left;">Columna</th>
                        <th style="padding:10px;border:1px solid #ddd;text-align:left;">Valor Anterior</th>
                        <th style="padding:10px;border:1px solid #ddd;text-align:left;">Valor Nuevo</th>
                    </tr>
                </thead>
                <tbody>
            '''
            
            for change in changes:
                change_type = change.get('change_type', 'UNKNOWN')
                column_name = change.get('column_name', 'Unknown')
                old_value = change.get('old_value') or '-'
                new_value = change.get('new_value') or '-'
                
                # Color según tipo de cambio
                if change_type == 'ADDED':
                    badge_color = '#28a745'
                    badge_text = '➕ ADDED'
                elif change_type == 'REMOVED':
                    badge_color = '#dc3545'
                    badge_text = '➖ REMOVED'
                else:
                    badge_color = '#fd7e14'
                    badge_text = '🔄 CHANGED'
                
                changes_html += f'''
                <tr>
                    <td style="padding:10px;border:1px solid #ddd;">
                        <span style="background:{badge_color};color:white;padding:3px 8px;border-radius:3px;font-size:12px;">{badge_text}</span>
                    </td>
                    <td style="padding:10px;border:1px solid #ddd;"><code>{column_name}</code></td>
                    <td style="padding:10px;border:1px solid #ddd;">{old_value}</td>
                    <td style="padding:10px;border:1px solid #ddd;">{new_value}</td>
                </tr>
                '''
            changes_html += '</tbody></table>'
        else:
            changes_html = '<p style="color:#666;font-style:italic;">Primer schema registrado - sin cambios previos</p>'
        
        html = f'''
        <html>
        <body style="font-family:Arial,sans-serif;line-height:1.6;">
            <div style="max-width:650px;margin:0 auto;">
                <div style="background:{header_color};color:white;padding:25px;text-align:center;border-radius:8px 8px 0 0;">
                    <h1 style="margin:0;font-size:24px;">{header_icon} {header_text}</h1>
                    <p style="margin:10px 0 0 0;opacity:0.9;">Estructura de archivo modificada</p>
                </div>
                
                <div style="background:#f8f9fa;padding:20px;border:1px solid #ddd;border-top:none;">
                    <table style="width:100%;">
                        <tr><td style="padding:5px 0;"><strong>Carrier:</strong></td>
                            <td><span style="background:#007bff;color:white;padding:3px 10px;border-radius:3px;">{carrier}</span></td></tr>
                        <tr><td style="padding:5px 0;"><strong>Archivo:</strong></td>
                            <td><code>{file_name}</code></td></tr>
                        <tr><td style="padding:5px 0;"><strong>Versión:</strong></td>
                            <td><strong>v{version}</strong></td></tr>
                        <tr><td style="padding:5px 0;"><strong>Total Columnas:</strong></td>
                            <td>{total_columns}</td></tr>
                        <tr><td style="padding:5px 0;"><strong>Fecha:</strong></td>
                            <td>{detected_at}</td></tr>
                    </table>
                </div>
                
                <div style="background:white;padding:20px;border:1px solid #ddd;border-top:none;">
                    <h2 style="color:#333;margin-top:0;border-bottom:2px solid {header_color};padding-bottom:10px;">
                        Cambios Detectados ({len(changes)})
                    </h2>
                    {changes_html}
                </div>
                
                <div style="background:#fff3cd;padding:15px;border:1px solid #ddd;border-top:none;">
                    <h3 style="color:#856404;margin:0 0 10px 0;">📋 Acciones Requeridas:</h3>
                    <ol style="color:#856404;margin:0;padding-left:20px;">
                        <li>Revisar si los cambios son esperados</li>
                        <li>Actualizar mapeos en <code>carrier_config.py</code> si es necesario</li>
                        <li>Verificar que bronze-to-silver procesa correctamente</li>
                    </ol>
                </div>
                
                <div style="background:#f8f9fa;padding:15px;border:1px solid #ddd;border-top:none;text-align:center;">
                    <a href="https://console.cloud.google.com/bigquery?project={PROJECT_ID}" 
                       style="background:#007bff;color:white;padding:10px 20px;text-decoration:none;border-radius:5px;display:inline-block;">
                        Ver Schema Registry en BigQuery
                    </a>
                </div>
                
                <div style="background:#343a40;color:white;padding:12px;text-align:center;font-size:12px;border-radius:0 0 8px 8px;">
                    Schema Detector - Medallion Pipeline
                </div>
            </div>
        </body>
        </html>
        '''
        return html
    
    @staticmethod
    def get_subject(alert_data: Dict[str, Any]) -> str:
        """Genera el asunto del email."""
        carrier = alert_data.get('carrier', 'Unknown').upper()
        status = alert_data.get('status', 'unknown')
        changes_count = len(alert_data.get('changes', []))
        
        if status == 'new_schema':
            return f"🆕 Schema Alert: {carrier} - Nuevo schema registrado"
        else:
            return f"⚠️ Schema Alert: {carrier} - {changes_count} cambio(s) detectado(s)"
