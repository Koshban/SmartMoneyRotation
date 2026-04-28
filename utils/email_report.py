"""utils/email_report.py — Send HTML report via SMTP."""
from __future__ import annotations

import logging
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

logger = logging.getLogger(__name__)


def _load_credentials() -> dict | None:
    try:
        from common.credentials import (
            SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD,
            EMAIL_FROM, EMAIL_TO,
        )
        if not SMTP_USER or not SMTP_PASSWORD:
            logger.warning(
                "Email credentials empty — edit common/credential.py "
                "with your SMTP / App-Password settings."
            )
            return None
        return {
            "host": SMTP_HOST,
            "port": SMTP_PORT,
            "user": SMTP_USER,
            "password": SMTP_PASSWORD,
            "from": EMAIL_FROM or SMTP_USER,
            "to": EMAIL_TO or [],
        }
    except ImportError:
        logger.warning(
            "common/credential.py not found — create it to enable email."
        )
        return None
    except Exception as exc:
        logger.warning("Failed to load email credentials: %s", exc)
        return None


def send_report_email(
    html_content: str,
    subject: str,
    to: list[str] | None = None,
    html_path: Path | None = None,
) -> bool:
    """Send *html_content* as the email body; optionally attach the file at *html_path*."""
    creds = _load_credentials()
    if creds is None:
        return False

    recipients = to or creds["to"]
    if not recipients:
        logger.warning("No email recipients configured.")
        return False

    try:
        msg = MIMEMultipart("mixed")
        msg["From"] = creds["from"]
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject

        # inline HTML body
        msg.attach(MIMEText(html_content, "html", "utf-8"))

        # attach .html file so recipient can open locally
        if html_path and html_path.exists():
            with open(html_path, "rb") as fh:
                part = MIMEBase("text", "html")
                part.set_payload(fh.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={html_path.name}",
                )
                msg.attach(part)

        with smtplib.SMTP(creds["host"], creds["port"], timeout=30) as srv:
            srv.ehlo()
            srv.starttls()
            srv.ehlo()
            srv.login(creds["user"], creds["password"])
            srv.sendmail(creds["from"], recipients, msg.as_string())

        logger.info("Report emailed → %s", ", ".join(recipients))
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error(
            "SMTP auth failed — check common/credential.py. "
            "For Gmail use an App Password, not your regular password."
        )
        return False
    except Exception as exc:
        logger.error("Email send failed: %s", exc)
        return False