"""
Cloud Function: schema_alert

Se activa via Pub/Sub cuando schema_detector detecta cambios en la estructura
de un archivo. Envía una alerta por email usando SendGrid.

Trigger: Pub/Sub topic "schema-changes"
"""

import json
import base64
import functions_framework

from etl_carriers.utils import EmailSender, SchemaAlertFormatter


@functions_framework.cloud_event
def schema_alert_pubsub(cloud_event):
    """Cloud Function activada por mensaje Pub/Sub de cambio de schema."""
    print("Schema Alert — evento recibido")

    try:
        data = cloud_event.data
        raw  = data.get("message", data)
        alert_data = json.loads(base64.b64decode(raw["data"]).decode("utf-8"))

        carrier = alert_data.get("carrier", "?")
        status  = alert_data.get("status", "")
        changes = alert_data.get("changes", [])

        print(f"[INFO] Carrier: {carrier} | Status: {status} | Cambios: {len(changes)}")

        if status not in ("new_schema", "schema_changed"):
            print(f"[SKIP] Status '{status}' no requiere alerta")
            return {"status": "ignored"}

        subject = SchemaAlertFormatter.get_subject(alert_data)
        html    = SchemaAlertFormatter.format_html(alert_data)
        sent    = EmailSender().send(subject, html)

        if sent:
            print("[OK] Email enviado")
            return {"status": "success", "email_sent": True}

        print("[ERROR] No se pudo enviar el email")
        return {"status": "error", "email_sent": False}

    except Exception as exc:
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(exc)}
