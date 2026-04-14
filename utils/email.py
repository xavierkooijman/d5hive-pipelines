import smtplib
from email.mime.text import MIMEText
from utils.common import resolve_secret


def send_email(env, email_config, html_content):

    email_config = dict(email_config)

    if "password" in email_config:
        email_config["password"] = resolve_secret(email_config["password"])

    if "api_key" in email_config:
        email_config["api_key"] = resolve_secret(email_config["api_key"])

    subject = email_config.get("subject", "Pipeline Report")
    recipients = email_config.get("recipients", [])

    if env == "render":
        import resend

        resend.api_key = email_config.get("api_key")

        try:
            result = resend.Emails.send({
                "from": email_config.get("from"),
                "to": "xavierkooijman@gmail.com",
                "subject": subject,
                "html": html_content,
            })

            print("Email sent successfully!")
            print(f"Email ID: {result['id']}")

        except Exception as e:
            print(f"Error sending email: {e}")
            raise

    else:
        SMTP_SERVER = "smtp.gmail.com"
        SMTP_PORT = 587

        try:
            msg = MIMEText(html_content, "html")
            msg["Subject"] = subject
            msg["From"] = email_config.get("from")
            msg["To"] = ", ".join(recipients)

            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(
                    email_config.get("from"),
                    email_config.get("password")
                )
                server.sendmail(
                    email_config.get("from"),
                    recipients,
                    msg.as_string()
                )

            print("Email sent!")

        except Exception as e:
            print(f"Error sending email: {e}")
            raise
