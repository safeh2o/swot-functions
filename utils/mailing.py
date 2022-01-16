import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def send_mail(email, errors):
    WEBURL = os.environ.get("WEBURL")
    analyze_url = f"{WEBURL}/analyze"
    message = Mail(from_email="no-reply@safeh2o.app", to_emails=email)
    message.template_id = os.environ.get("SENDGRID_UPLOAD_SUMMARY_TEMPLATE_ID")
    message.dynamic_template_data = {"errors": errors, "analyzeUrl": analyze_url}
    try:
        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e)
