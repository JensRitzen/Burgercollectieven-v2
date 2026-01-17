import os
import glob
import ssl
import smtplib
from email.message import EmailMessage

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.sendgrid.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "apikey")          
SMTP_PASS = os.getenv("SMTP_PASS")                    
MAIL_FROM = os.getenv("SMTP_FROM")                    
MAIL_TO = os.getenv("DEMO_EMAIL_TO")                  

CHARTS_DIR = os.getenv("CHARTS_DIR", "/app/data/charts")


def main():
    if not (SMTP_PASS and MAIL_FROM and MAIL_TO):
        raise SystemExit("Missing env vars: SMTP_PASS, SMTP_FROM, DEMO_EMAIL_TO")

    pdf_paths = sorted(glob.glob(f"{CHARTS_DIR}/*.pdf"))
    if not pdf_paths:
        print(f"No PDFs found in {CHARTS_DIR}")
        return

    msg = EmailMessage()
    msg["Subject"] = "Demo – PDF charts"
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg.set_content("Bijgevoegd: gegenereerde PDF charts (demo).")

    for p in pdf_paths:
        with open(p, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="application",
                subtype="pdf",
                filename=os.path.basename(p),
            )

    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls(context=ctx)
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

    print(f"Sent 1 email to {MAIL_TO} with {len(pdf_paths)} PDFs from {CHARTS_DIR}")


if __name__ == "__main__":
    main()

