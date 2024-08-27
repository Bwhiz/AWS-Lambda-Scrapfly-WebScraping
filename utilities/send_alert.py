import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os

# Email configuration
SMTP_USERNAME = os.getenv('SMTP_USERNAME')  
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')  
SENDER_EMAIL = os.getenv('SENDER_EMAIL')  
RECIPIENT_EMAIL = os.getenv('RECIPIENT_EMAIL') 

def send_email_notification(message, subject='Automated Error Notification - AWS Lambda'):
    try:
        
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECIPIENT_EMAIL
        msg['Subject'] = subject

        # attaching the error message to the body of the mail
        body = MIMEText(message, 'plain')
        msg.attach(body)

        # Connect to the SMTP server and send the email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()  
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())

    except Exception as e:
        print(f"Failed to send email: {str(e)}")
