import base64
import csv
import os
from datetime import datetime, timezone

import boto3
import paubox
from paubox_email_footers import providerrelations_footer


class PauboxMailer:
    def __init__(
        self,
        api_key: str | None = None,
        host: str | None = None,
        s3_bucket: str = 'acp-data',
    ):
        self.api_key = api_key or os.getenv("PAUBOX_API_KEY")
        self.host = host or os.getenv("PAUBOX_HOST")
        self.paubox_client = paubox.PauboxApiClient(self.api_key, self.host)
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')

    def get_logo(self, logo_name: str) -> bytes:
        key = f'paubox/{logo_name}.png'
        response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
        return response['Body'].read()

    def send_mail(
        self,
        recipients: str | list[str],
        subject: str,
        attachments: str | list[str],
        message: str,
        footer: str | None = providerrelations_footer,
        bcc: str | list[str] | None = "",
        cc: str | list[str] | None = "",
        delivery_email_address: str | None = "providerrelations@somoscommunitycare.org",
        logo: str | None = "somos_logo",
    ) -> str:
        if logo is not None:
            logo_bytes = self.get_logo(logo)
            logo_content = base64.b64encode(logo_bytes).decode("utf-8")
        else:
            logo_content = ""
        # Normalize recipients, bcc, cc to lists
        recipients = self._normalize_recipients(recipients)
        bcc = self._normalize_recipients(bcc)
        cc = self._normalize_recipients(cc)

        # Prepare full html message
        html_message = self._build_html_message(message, footer)

        mail = {
            "data": {
                "message": {
                    "recipients": recipients,
                    "headers": {
                        "subject": subject,
                        "from": delivery_email_address,
                    },
                    "bcc": bcc,
                    "cc": cc,
                    "content": {"text/html": html_message},
                    "attachments": [
                        {
                            "fileName": f"{logo}.png",
                            "contentType": "image/png",
                            "contentId": "123",
                            "disposition": "inline",
                            "content": logo_content,
                        },
                    ],
                }
            }
        }

        attachments_list = self._normalize_attachments(attachments)

        for attachment in attachments_list:
            file_name = os.path.basename(attachment)
            try:
                with open(attachment, "rb") as f:
                    file_data = f.read()
                encoded_file_data = base64.b64encode(file_data).decode("utf-8")
            except Exception as e:
                print(f"Skipping attachment '{attachment}': {e}")
                continue

            mail["data"]["message"]["attachments"].append({
                "fileName": file_name,
                "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "content": encoded_file_data,
            })

        response = self.paubox_client.send(mail)
        return response.to_dict['sourceTrackingId']  # type: ignore

    def check_delivery_status(self, source_tracking_id: str) -> tuple[list[dict], str]:
        disposition_response = self.paubox_client.get(source_tracking_id)
        message_deliveries = [recipient for recipient in disposition_response.to_dict['data']  # type: ignore
                              ['message']['message_deliveries']]   # type: ignore
        print(f"Delivery Status: {message_deliveries}")
        print(type(message_deliveries))
        return message_deliveries, source_tracking_id

    @staticmethod
    def convert_paubox_timestamps(time_str: str) -> str:
        dt = datetime.strptime(time_str, "%a, %d %b %Y %H:%M:%S %z")
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def update_delivery_status_table(
        recipients_data: tuple[list[dict], str],
        email_type: str,
        tins: list,
        practice_names: list,
        for_month: str,
        filenames: list,
    ) -> str:
        recipients, tracking_id = recipients_data
        output_file = f'/home/etl/etl_home/downloads/{email_type}_emails.csv'
        with open(output_file, mode='a+', newline="") as file:
            writer = csv.writer(file)
            for recipient in recipients:
                address = recipient['recipient']
                status = recipient['status']['deliveryStatus']
                try:
                    delivery_time = recipient['status']['deliveryTime']
                    if delivery_time:
                        delivery_time = PauboxMailer.convert_paubox_timestamps(
                            delivery_time)
                    else:
                        delivery_time = None
                    opened_status = recipient['status'].get('openedStatus', '')
                    opened_time_raw = recipient['status'].get('openedTime')
                    if opened_time_raw:
                        opened_time = PauboxMailer.convert_paubox_timestamps(
                            opened_time_raw)
                    else:
                        opened_time = None
                except Exception as e:
                    delivery_time = ''
                    opened_status = ''
                    opened_time = ''
                    print(
                        f'Unable to retrieve delivery or opened times from Paubox: {e}')
                writer.writerow([
                    ', '.join(tins),
                    ', '.join(practice_names),
                    address,
                    status,
                    delivery_time,
                    opened_status,
                    opened_time,
                    email_type,
                    for_month,
                    tracking_id,
                    ', '.join(filenames)
                ])
        return output_file

    @staticmethod
    def _normalize_recipients(recipients: str | list[str] | None) -> list[str]:
        if not recipients:
            return []
        if isinstance(recipients, str):
            return [r.strip() for r in recipients.split(",") if r.strip()]
        return recipients

    @staticmethod
    def _normalize_attachments(attachments: str | list[str]) -> list[str]:
        if isinstance(attachments, str):
            if "," in attachments:
                return [a.strip() for a in attachments.split(",") if a.strip()]
            else:
                return [attachments.strip()]
        return attachments

    @staticmethod
    def _build_html_message(message: str, footer: str | None) -> str:
        footer = footer or ""
        if '<' not in message:
            message = f"<p>{message}</p>"
        return f"""<html>
            <head><style><!-- (your CSS here) --></style></head>
            <body>{message}{footer}</body>
        </html>"""


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Send a secure email with optional attachments and recipients.")
    parser.add_argument(
        "recipients", help="Comma-separated list of recipients")
    parser.add_argument("subject", help="Subject of the email")
    parser.add_argument("message", help="Email message body")
    parser.add_argument(
        "attachments", help="Comma-separated list of file paths")
    parser.add_argument(
        "--footer", default=providerrelations_footer, help="Optional footer")
    parser.add_argument("--bcc", default="", help="Comma-separated BCC list")
    parser.add_argument("--cc", default="", help="Comma-separated CC list")
    parser.add_argument("--delivery-email-address",
                        default="providerrelations@somoscommunitycare.org", help="Delivery email address")
    parser.add_argument("--logo", default="somos_logo", help="Logo name")

    args = parser.parse_args()

    mailer = PauboxMailer()
    source_tracking_id = mailer.send_mail(
        recipients=args.recipients,
        subject=args.subject,
        attachments=args.attachments,
        message=args.message,
        footer=args.footer,
        bcc=args.bcc,
        cc=args.cc,
        delivery_email_address=args.delivery_email_address,
        logo=args.logo,
    )
    print(f"Sent mail with tracking ID: {source_tracking_id}")
