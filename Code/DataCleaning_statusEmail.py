import pandas as pd
import csv
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import DataCleaning_queries as DQ
import DataCleaning_EmailPass as creds
from datetime import datetime

#BATCH_FILE_queries.GetSwitchStatusSummary was for other project. It will be changed and replaced if mails are required for cleansing

def toMail(datesent):
   toMail = BATCH_FILE_queries.GetSwitchStatusSummary(datesent)
   toMail = toMail['EmailTo']
   toMail = str(toMail[0])
   return toMail

def ccMail(datesent):
   ccMail = BATCH_FILE_queries.GetSwitchStatusSummary(datesent)
   ccMail = ccMail['EmailCC']
   ccMail = str(ccMail[0])
   return ccMail   

def bccMail(datesent):
   bccMail = BATCH_FILE_queries.GetSwitchStatusSummary(datesent)
   bccMail = bccMail['EmailBCC']
   bccMail = str(bccMail[0])
   return bccMail 

def sendStatusEmail(datesent):
    MY_ADDRESS = BATCH_FILE_EmailPass.emailID()
    MY_PASSWORD = BATCH_FILE_EmailPass.emailPass()
    server = smtplib.SMTP(host='smtp-mail.outlook.com',port=587)
    TOMail = toMail(datesent)
    CCMail = ccMail(datesent)
    BCCMail = bccMail(datesent)
    
    text = """
Hi,

Summary of Batch Files as of today:
    
{table}

Regards,

Company-name"""

    html = """
<html>
<head>
<style> 
 table, th, td {{ border: 1px solid black; border-collapse: collapse; }}
  th, td {{ padding: 5px; }}
</style>
</head>
<body><p>Hi,</p>
<p>Summary of Batch Files as of today:</p>
{table}
<p>Regards,</p>
<p>company-name</p>
</body></html>
"""

    df = BATCH_FILE_queries.GetSwitchStatusSummary(datesent)   #procedure to get summary, to be changed
    df = df[['SwitchDate','SupplierId','SupplierName','SwitchCount','IsError','ErrorMessage']]
    col_list = list(df.columns.values)
    df.index +=1
    data=df

    text = text.format(table=tabulate(data, headers=col_list, tablefmt="grid"))
    html = html.format(table=tabulate(data, headers=col_list, tablefmt="html"))

    message = MIMEMultipart("alternative", None, [MIMEText(text), MIMEText(html,'html')])

    message['Subject'] = "Summary of Batch File " + datetime.strftime(datetime.now(), '%d-%m-%Y')

    message['From'] = MY_ADDRESS
    message['To'] = TOMail
    message['Cc'] = CCMail
    message['Bcc'] = BCCMail
    server = smtplib.SMTP(host='smtp-mail.outlook.com',port=587)
    server.starttls()
    server.login(MY_ADDRESS, MY_PASSWORD)
    server.send_message(message)
    
def sendAlert(e):
    emailContent = e
    datesent = datetime.strftime(datetime.now(), '%Y-%m-%d')
    TOMail = toMail(datesent)  #to be changed
    CCMail = ccMail(datesent)  #to be changed
    BCCMail = bccMail(datesent)  #to be changed
    
    MY_ADDRESS = creds.emailID()
    MY_PASSWORD = creds.emailPass()
    s = smtplib.SMTP(host='smtp-mail.outlook.com',port=587)
    s.starttls()
    s.login(MY_ADDRESS,MY_PASSWORD)

    msg = MIMEMultipart()

    msg['From'] = MY_ADDRESS
    msg['To'] = TOMail
    msg['Cc'] = CCMail
    msg['Bcc'] = BCCMail
    msg['Subject'] = "Data Cleaning Process Failed on " + datetime.strftime(datetime.now(), '%d-%m-%Y')
    message  = MIMEText(emailContent, "plain")
    msg.attach(message)

    s.send_message(msg)
