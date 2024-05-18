from __future__ import annotations

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from string import Template


class EmailSender:
    def __init__(self):
        self.email_username = os.getenv("EMAIL_USERNAME")
        self.email_password = os.getenv("EMAIL_PASSWORD")

    def send_plain_email(
        self,
        subject: str,
        message: str,
        recipient_emails=["calvindunotifications@gmail.com"],
    ) -> bool:
        # Create a MIME message object
        for recipient_email in recipient_emails:
            msg = MIMEMultipart()
            msg["From"] = self.email_username
            msg["To"] = recipient_email
            msg["Subject"] = subject

            # Attach the message body
            msg.attach(MIMEText(message, "plain"))

            # Send the email
            self.smtp_email(msg=msg, recipient_email=recipient_email)
            return True

    # Sends a DQ Notificaiton Email with a template
    def send_dq_notification_email(
        self,
        parameters: dict,
        recipient_emails=["calvindunotifications@gmail.com"],
    ):
        # Read the HTML file
        html_template = os.path.join(
            os.path.dirname(__file__),
            "templates",
            "ge_dq_notification.html",
        )
        html_content = self.render_html_template(
            html_file_path=html_template,
            params=parameters,
        )

        # Create a MIME message object
        for recipient_email in recipient_emails:
            msg = MIMEMultipart()
            msg["From"] = self.email_username
            msg["To"] = recipient_email
            msg["Subject"] = "DQ Check Notification"

            # Attach the message body
            html_part = MIMEText(html_content, "html")
            msg.attach(html_part)

            # Send the email
            self.smtp_email(msg=msg, recipient_email=recipient_email)

    # Sends an email with smtp email
    def smtp_email(self, msg, recipient_email: str):
        # Create a secure connection to the SMTP server
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
            smtp_server.login(self.email_username, self.email_password)
            smtp_server.sendmail(self.email_username, recipient_email, msg.as_string())
            print("Email sent!")

    # Renders an HTML template to be used in an email
    def render_html_template(self, html_file_path: str, params: dict) -> str:
        with open(html_file_path) as file:
            html_template = file.read()

        # Verify that html parameters exist
        valid_parameters = ["asset_name", "error_exceptions", "data_docs_site"]
        for key in valid_parameters:
            if key not in params:
                raise Exception(f"Parameters must include {key}")

        html_template = Template(html_template)
        html_content = html_template.substitute(params)
        return html_content
