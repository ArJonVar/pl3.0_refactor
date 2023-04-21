import smtplib
from email.mime.text import MIMEText
from logger import ghetto_logger

smtp_server = 'smtp-mail.outlook.com'
smtp_port = 587
smtp_user = 'arielv@dowbuilt.com'
smtp_password = ''
from_address = 'arielv@dowbuilt.com'
to_address = 'ariel_vardy@yahoo.com'
logr = ghetto_logger('server_email.py')

message = 'test'

msg=MIMEText(message)
msg['Subject'] = 'TESTY 3.13.23'
msg['From'] = from_address
msg['To']=to_address

try:
    with smtplib.SMTP(smtp_server, smtp_port) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)
except Exception as e:
    logr.log(e)