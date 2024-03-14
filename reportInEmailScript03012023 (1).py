# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 12:55:41 2023

@author: sheld
"""

# import mysql.connector
from sqlalchemy import create_engine
import pymysql
import pandas as pd
from pandas import DataFrame as df
# import pymongo
from pymongo import MongoClient
from datetime import datetime
import numpy as np



from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pretty_html_table import build_table
from datetime import date
from datetime import timedelta
# import ssl





def getOla():
    cluster_link = ""
    cluster = MongoClient(cluster_link)
    db = cluster['olainsure']
    df = pd.DataFrame.from_records(db.b2cLeads.find({'status': "Policy Issued"}))
    df['policyIssueDate'] = pd.to_datetime(df['policyIssueDate'],format = "%d/%m/%Y")
    df.rename({'totalPremium':'Price','policyIssueDate':'Sales_Date'}, axis = 1,inplace = True)
    df['Client'] = 'Ola'
    df= df[['Sales_Date','Price','Client']]
    return(df)

def getUnison():
    cluster_link = ""
    cluster = MongoClient(cluster_link)
    db = cluster['unison']
    df = pd.DataFrame.from_records(db.b2cLeads.find({'status': "Policy Issued"}))
    df['policyIssueDate'] = pd.to_datetime(df['policyIssueDate'],format = "%d/%m/%Y")
    df.rename({'totalPremium':'Price','policyIssueDate':'Sales_Date'}, axis = 1,inplace = True)
    df['Client'] = 'Unison'
    df= df[['Sales_Date','Price','Client']]
    return(df)


def getJmd():
    cluster_link = ""
    cluster = MongoClient(cluster_link)
    db = cluster['jmd']
    df = pd.DataFrame.from_records(db.b2cLeads.find({'status': "Policy Issued"}))
    df['policyIssueDate'] = pd.to_datetime(df['policyIssueDate'],format = "%d/%m/%Y")
    df.rename({'totalPremium':'Price','policyIssueDate':'Sales_Date'}, axis = 1,inplace = True)
    df['Client'] = 'JMD'
    df= df[['Sales_Date','Price','Client']]
    return(df)

def getTechnopolis():
    cluster_link = ""
    cluster = MongoClient(cluster_link)
    db = cluster['technopolis']
    df = pd.DataFrame.from_records(db.b2cLeads.find({'status': "Policy Issued"}))
    df['policyIssueDate'] = pd.to_datetime(df['policyIssueDate'],format = "%d/%m/%Y")
    df.rename({'totalPremium':'Price','policyIssueDate':'Sales_Date'}, axis = 1,inplace = True)
    df['Client'] = 'Technopolis'
    df= df[['Sales_Date','Price','Client']]
    return(df)

def getBookMyInsurance():
    cluster_link = ""
    cluster = MongoClient(cluster_link)
    db = cluster['bminsurance']
    df = pd.DataFrame.from_records(db.b2cLeads.find({'status': "Policy Issued"}))
    df['policyIssueDate'] = pd.to_datetime(df['policyIssueDate'],format = "%d/%m/%Y")
    df.rename({'totalPremium':'Price','policyIssueDate':'Sales_Date'}, axis = 1,inplace = True)
    df['Client'] = 'BMI'
    df= df[['Sales_Date','Price','Client']]
    return(df)


def getHudson():
    cluster_link = ""
    cluster = MongoClient(cluster_link)
    db = cluster['hudson']
    df = pd.DataFrame.from_records(db.b2cLeads.find({'status': "Policy Issued"}))
    df['policyIssueDate'] = pd.to_datetime(df['policyIssueDate'],format = "%d/%m/%Y")
    df.rename({'totalPremium':'Price','policyIssueDate':'Sales_Date'}, axis = 1,inplace = True)
    df['Client'] = 'Hudson'
    df= df[['Sales_Date','Price','Client']]
    return(df)

def getVibrant():
    cluster_link = ""
    cluster = MongoClient(cluster_link)
    db = cluster['vibrant']
    df = pd.DataFrame.from_records(db.b2cLeads.find({'status': "Policy Issued"}))
    df['policyIssueDate'] = pd.to_datetime(df['policyIssueDate'],format = "%d/%m/%Y")
    df.rename({'totalPremium':'Price','policyIssueDate':'Sales_Date'}, axis = 1,inplace = True)
    df['Client'] = 'Vibrant'
    df= df[['Sales_Date','Price','Client']]
    return(df)

def getBharatre():
    cluster_link = ""
    cluster = MongoClient(cluster_link)
    db = cluster['bharatre']
    df = pd.DataFrame.from_records(db.b2cLeads.find({'status': "Policy Issued"}))
    df['policyIssueDate'] = pd.to_datetime(df['policyIssueDate'],format = "%d/%m/%Y")
    df.rename({'totalPremium':'Price','policyIssueDate':'Sales_Date'}, axis = 1,inplace = True)
    df['Client'] = 'Bharatre'
    df= df[['Sales_Date','Price','Client']]
    return(df)

def getVizza():
    cluster_link = ""
    cluster = MongoClient(cluster_link)
    db = cluster['vizza']
    df = pd.DataFrame.from_records(db.b2cLeads.find({'status': "Policy Issued"}))
    df['policyIssueDate'] = pd.to_datetime(df['policyIssueDate'],format = "%d/%m/%Y")
    df.rename({'totalPremium':'Price','policyIssueDate':'Sales_Date'}, axis = 1,inplace = True)
    df['Client'] = 'Vizza'
    df= df[['Sales_Date','Price','Client']]
    return(df)

def getOneCode():
    db_connection_str = ''
    db_connection = create_engine(db_connection_str)
    df = pd.read_sql('SELECT * FROM t_ledger', con=db_connection)
    df['modified_on'] = pd.to_datetime(df['modified_on'])
    df['Sales_Date'] = df['modified_on'].dt.date
    df['Client'] = 'OneCode'
    df.rename({'amount':'Price'}, axis = 1,inplace = True)
    df = df[['Sales_Date','Price','Client']]
    return(df)

def getUnilightSales():
    db_connection_str = ''
    db_connection = create_engine(db_connection_str)
    df = pd.read_sql('SELECT * FROM t_ledger WHERE process_id IS NOT NULL AND is_published = 1', con=db_connection)
    df['modified_on'] = df['modified_on'].dt.date
    df1 = df[['modified_on','amount']]
    df1.rename(columns = {'modified_on': 'Sales_Date', 'amount': 'Price'},inplace = True)
    df1['Client'] = 'Unilight'
    return(df1)



def getTotalSales():
    df1 = getUnilightSales()
    df3 = getOneCode()
    df2 = getPorter()
    df4 = getHudson()
    df5 = getVibrant()
    df6 = getBharatre()
    df7 = getOla()
    df8 = getBookMyInsurance()
    df10 = getTechnopolis()
    df9 = getUnison()
    df11 = getVizza()
    frames = [df3,df4,df5,df6,df7,df8,df9,df10,df11]
    df = pd.concat(frames)
    return(df)

def getPorter():
    db_connection_str = ''
    db_connection = create_engine(db_connection_str)
    df = pd.read_sql('SELECT * FROM t_test', con=db_connection)
    df[df['client'] == 'Porter']
    df.rename({'price':'Price','sales_date':'Sales_Date','client':'Client'}, axis = 1,inplace = True)
    df = df[['Sales_Date','Price','Client']]
    return(df)



df = getTotalSales()

from datetime import date
from datetime import timedelta

today = date.today()
year = datetime.today().year
print(df)
df['Sales_Date'] = pd.to_datetime(df['Sales_Date'])
df['Quarter'] = df['Sales_Date'].dt.to_period('Q')
dfQ = df.groupby(['Client','Quarter'])['Price'].agg(['sum','count']).reset_index()
dfq1 = dfQ[dfQ['Quarter'] == str(year) + "Q" + str(((datetime.today().month-1)//3 + 1))]
dfq1.rename({'sum':'Total Premium Q2D', 'count': 'Total Policies Q2D'}, axis = 1,inplace = True)


dfm2d = df[(df['Sales_Date'].dt.month == today.month) & (df['Sales_Date'].dt.year == today.year) ]
dfm2d['Price'] = dfm2d['Price'].astype(float)
dfm2d1 = dfm2d.groupby('Client')['Price'].agg(['sum','count']).reset_index()
dfm2d1.rename({'sum':'Total Premium M2D', 'count': 'Total Policies M2D'}, axis = 1,inplace = True)

dftoday = df[df['Sales_Date'] == pd.to_datetime(today)]
dftoday[pd.to_datetime(dftoday['Sales_Date']) == pd.to_datetime(today)]
dftoday['Price'] = dftoday['Price'].astype(float)

dfsend1 = dftoday.groupby('Client')['Price'].agg(['sum','count'])
dfsend1.rename({'sum':'Total Premium', 'count': 'Total Policies'}, axis = 1,inplace = True)
dfsend1.reset_index(inplace = True)
dfsend1['Date'] = pd.to_datetime(today)


dfsendTot = dfm2d1.merge(dfsend1,how = 'left', on = 'Client')
dfsendTot1 = dfq1.merge(dfsendTot, how = 'left', on = 'Client')
dfsendTot1['Date'] = today
dfsendTot2 = dfsendTot1.replace(np.NaN,0)
dfsendTot2['Total Policies'] = dfsendTot2['Total Policies'].astype(int)
dfsendTot2['Total Policies M2D'] = dfsendTot2['Total Policies M2D'].astype(int)
dfsendTot2 = dfsendTot2[['Date','Client','Total Policies','Total Premium','Total Policies M2D','Total Premium M2D','Total Policies Q2D','Total Premium Q2D']]


total = {"Date":[today],
         "Client":["TOTAL"],
         "Total Policies":sum(list(dfsendTot2['Total Policies'].astype(int))),
         "Total Premium":sum(list(dfsendTot2['Total Premium'].astype(int))),
         "Total Policies M2D":sum(list(dfsendTot2['Total Policies M2D'].astype(int))),
         "Total Premium M2D":sum(list(dfsendTot2['Total Premium M2D'].astype(int))),
         "Total Policies Q2D":sum(list(dfsendTot2['Total Policies Q2D'].astype(int))),
         "Total Premium Q2D":sum(list(dfsendTot2['Total Premium Q2D'].astype(int)))
         }
dfTotal = pd.DataFrame(total)

dfsendTot3 = pd.concat([dfsendTot2,dfTotal])
dfsendTot3['Total Premium'] = dfsendTot3['Total Premium'].map("{:,.0f}".format)
dfsendTot3['Total Premium M2D'] = dfsendTot3['Total Premium M2D'].map("{:,.0f}".format)
dfsendTot3['Total Premium Q2D'] = dfsendTot3['Total Premium Q2D'].map("{:,.0f}".format)

#print(dfsendTot3)

output = build_table(dfsendTot3,'blue_light')

def send_mail(body):
    message = MIMEMultipart()
    message['Subject'] = 'Daily Sales Data'
    message['From'] = ''

    recipients = []
    message['To'] = ", ".join(recipients)
    body_content = body
    message.attach(MIMEText(body_content, "html"))
    msg_body = message.as_string()
    server = SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(message['From'], '')
    server.sendmail(message['From'],recipients, msg_body)
    server.quit()


send_mail(output)