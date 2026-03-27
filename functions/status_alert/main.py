"""
Cloud Function: status_alert

Se activa via Pub/Sub cuando bronze_to_silver detecta un valor nuevo
de status o payment_status en un carrier. Envía alerta por email.

Trigger: Pub/Sub topic "status-changes"
"""

import json
import base64
import functions_framework

from etl_carriers.utils import EmailSender, StatusAlertFormatter


@functions_framework.cloud_event
def status_alert_pubsub(cloud_event):
    """Cloud Function activada por mensaje Pub/Sub de nuevo valor de status."""
    print("Status Alert — evento recibido")

    try:
        data = cloud_event.data
        raw  = data.get("message", data)
        alert_data = json.loads(base64.b64decode(raw["data"]).decode("utf-8"))

        carrier    = alert_data.get("carrier", "?")
        new_values = alert_data.get("new_values", [])

        print(f"[INFO] Carrier: {carrier} | Valores nuevos: {len(new_values)}")

        if not new_values:
            print("[SKIP] Sin valores nuevos")
            return {"status": "ignored"}

        subject = StatusAlertFormatter.get_subject(alert_data)
        html    = StatusAlertFormatter.format_html(alert_data)
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
