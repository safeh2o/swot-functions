from __future__ import annotations

import base64
import logging
import os
from urllib.parse import quote_plus

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Attachment, Disposition, FileName, FileType, Mail

from utils.standardize import Datapoint, UploadedFileSummary


def create_error_attachments(uploaded_file_summaries: list[UploadedFileSummary]):
    attachments: list[Attachment] = []
    for file_summary in uploaded_file_summaries:
        base_filename = ".".join(file_summary.filename.split(".")[:-1])
        filename = base_filename + ".csv"
        attachment = Attachment(
            file_name=FileName(filename),
            file_type=FileType("text/csv"),
            disposition=Disposition("attachment"),
        )
        header_line = f"row_number, {Datapoint.header_line()}"
        lines = [header_line]
        if not file_summary.errors:
            continue
        for error in file_summary.errors:
            lines.append(error.to_csv_line())
        all_lines = "\n".join(lines)
        encoded_file = base64.b64encode(all_lines.encode()).decode()
        attachment.file_content = encoded_file
        attachments.append(attachment)

    return attachments


def send_mail(email, uploaded_file_summaries, country_name, area_name, fieldsite_name):
    WEBURL = os.environ.get("WEBURL")

    analyze_url = f"{WEBURL}/analyze#country={quote_plus(country_name)}&area={quote_plus(area_name)}&fieldsite={quote_plus(fieldsite_name)}"
    message = Mail(from_email="no-reply@safeh2o.app", to_emails=email)
    message.template_id = os.environ.get("SENDGRID_UPLOAD_SUMMARY_TEMPLATE_ID")
    message.dynamic_template_data = {
        "errors": [],
        "analyzeUrl": analyze_url,
        "fieldsiteName": fieldsite_name,
    }
    attachments = create_error_attachments(uploaded_file_summaries)
    message.attachment = attachments

    try:
        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        sg.send(message)
        logging.info("sent upload confirmation email to %s", email)
    except Exception as err:
        logging.error(err)
