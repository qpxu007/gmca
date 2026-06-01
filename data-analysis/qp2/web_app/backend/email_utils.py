"""Email sending utilities for scheduled report jobs."""
import os
import socket
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

logger = logging.getLogger(__name__)

ADMIN_EMAIL = os.environ.get("QP2_ADMIN_EMAIL", "")
SMTP_HOST = "127.0.0.1"


def send_mail(subject: str, body: str, to: list[str],
              attachment_path: str | None = None,
              sender: str | None = None) -> None:
    """Send email via localhost SMTP. No-ops silently if `to` is empty."""
    if not to:
        logger.warning("send_mail called with empty recipient list — skipping")
        return

    user = os.environ.get("USER", "qp2")
    host = socket.gethostname()
    from_addr = sender or f"{user}@{host}"

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to)
    msg.attach(MIMEText(body))

    if attachment_path and os.path.exists(attachment_path):
        part = MIMEBase("application", "octet-stream")
        with open(attachment_path, "rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{os.path.basename(attachment_path)}"',
        )
        msg.attach(part)

    smtp = smtplib.SMTP(SMTP_HOST)
    smtp.sendmail(from_addr, to, msg.as_string())
    smtp.close()
    logger.info(f"Email sent to {to}: {subject!r}")


def send_admin_alert(subject: str, body: str) -> None:
    """Send a failure alert to the fixed admin address."""
    if not ADMIN_EMAIL:
        logger.warning("QP2_ADMIN_EMAIL not set — skipping admin alert")
        return
    try:
        send_mail(subject=subject, body=body, to=[ADMIN_EMAIL])
    except Exception as e:
        logger.error(f"Failed to send admin alert: {e}")
