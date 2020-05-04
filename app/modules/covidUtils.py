
# coding: utf-8

# # Importing Libraries

# In[1]:


import pandas as pd
import os
import re
import datetime
import numpy as np
import requests
import smtplib, ssl
from email.mime.text import MIMEText
import pytz
import pickle
from pytz import timezone

# # Defining Functions

# In[ ]:


def pullData(url):
    '''Pull the data'''
    response = requests.get(url)
    responseJson = response.json()
    return response, responseJson


# In[76]:


def addZeroPadding(text):
    if len(text) == 1:
        return '0' + text
    else:
        return text
    
def addZeroPaddingToDate(text):
    year = re.search(r'^([0-9]{4})', text).group(1)
    month = re.search(r'-([0-9]{1,2})-', text).group(1)
    day = re.search(r'-([0-9]{1,2})$', text).group(1)
    dateString = addZeroPadding(month) + '-' + addZeroPadding(day) + '-' + year
    return dateString    


# In[77]:


def convertStringToTimestampNew(text, formatIn):
    timestamp = datetime.datetime.strptime(text, formatIn)
    return timestamp

def convertStringToTimestamp(text):
    timestamp = datetime.datetime.strptime(text, '%m-%d-%Y')
    return timestamp

def convertTimestampToString(timestamp):
    string = datetime.datetime.strftime(timestamp, '%m-%d-%Y')
    return string


# In[78]:


def durationtoString(duration):
    duration = str(duration)
    durationMinutes = re.search(r':([0-9]+):', duration).group(1)
    durationSeconds = re.search(r':([0-9]+)\.', duration).group(1)
    durationMilliSeconds = re.search(r'\.([0-9]+)$', duration).group(1)
    durationString = "{durationMinutes} min {durationSeconds} sec".format(durationMinutes = durationMinutes, 
                                                                                                  durationSeconds = durationSeconds, 
                                                                                                  durationMilliSeconds = durationMilliSeconds)
    return durationString


# In[79]:


def makeNewCasesCols(df, metric, colDim):
    newMetric = 'new' + metric.capitalize()
    df[newMetric] = df.groupby(colDim)[metric].diff()
    df[newMetric] = np.where(df[newMetric].isnull(),
                             df[metric],
                             df[newMetric])
    return df


# In[80]:


def cleanState(state):
#     if re.search(r', [A-Z]{2}', state):
    if ', ' in state:    
        state = state.split(',')[1].strip()
    return state.strip()


# In[81]:


def standState(state, dic):
    try:
        return dic[state]
    except:
        return state


# In[82]:


def standCountry(country, dic):
    try:
        return dic[country]
    except:
        return country


# In[150]:


def imputeIncorrect(df, metric, colName):
    metricNew = 'correct' + metric.capitalize()
    df[metricNew] = np.where((df[colName].eq(df[colName].shift(1)))
                             &
                             (df[metric].gt(df[metric].shift(1)) |  
                             df[metric].eq(df[metric].shift()) == False), 
                             np.nan, 
                             df[metric])
    df[metricNew] = df.groupby([colName])[metricNew].ffill()
    df[metricNew] = np.where(df[metricNew].isnull(),
                             df[metric],
                             df[metricNew])
    df.drop(columns = metric, inplace = True)
    df.rename(columns = {metricNew: metric}, inplace = True)
    return df


# In[ ]:



def checksIndf(df):
    
    dfCheck = df.copy(deep = True)
    
    maxTimestamp = dfCheck['timestamp'].max()
    conSum = dfCheck[dfCheck['timestamp'] == maxTimestamp]['confirmed'].sum()
    recSum = dfCheck[dfCheck['timestamp'] == maxTimestamp]['recovered'].sum()
    deaSum = dfCheck[dfCheck['timestamp'] == maxTimestamp]['deaths'].sum()

    newConSum = dfCheck['newConfirmed'].sum()
    newRecSum = dfCheck['newRecovered'].sum()
    newDeaSum = dfCheck['newDeaths'].sum()

    '''Printing the checks'''
    if(conSum == newConSum):
        print ('Confirmed Cases Matched')
    else:
        print ('Confirmed Cases NOT Matched')

    if(recSum == newRecSum):
        print ('Recovered Cases Matched')
    else:
        print ('Recovered Cases NOT Matched')

    if(deaSum == newDeaSum):
        print ('Deaths Cases Matched')
    else:
        print ('Deaths Cases NOT Matched')

    print ('')
    print ('Confirmed : {conSum}'.format(conSum = conSum))
    print ('Recovered : {recSum}'.format(recSum = recSum))
    print ('Deaths : {deaSum}'.format(deaSum = deaSum))



def convertDfToJson(df, orient):
    jsonData = df.to_json(orient = orient)
    return jsonData

def getCurrTime():
    curTime = datetime.datetime.now(pytz.utc).astimezone(timezone('Asia/Kolkata'))
    executionTime = datetime.datetime.strftime(curTime, '%m-%d-%Y %H:%M') + 'IST'
    return executionTime

def prepareEmailContent(executionTime, maxDateDict, linesDict):
    subject = 'COVID-19 Monitor Data Status'
    body = """<h2>Data pull last ran at <b>{executionTime}</b></h2>
<p>World: <b>{worldLines}</b> lines fetched with <b>{worldDate}</b> as max date
<br/>
WorldCountry: <b>{worldCountryLines}</b> lines fetched with <b>{worldCountryDate}</b> as max date
<br/>
India: <b>{indiaLines}</b> lines fetched with <b>{indiaDate}</b> as max date
<br/>
IndiaState: <b>{indiaStateLines}</b> lines fetched with <b>{indiaStateDate}</b> as max date
<br/>
<i>This is an auto-generated email. Please do not reply to this thread.<i/>
</p>
    """.format(executionTime = str(executionTime),
               worldLines = '{:,}'.format(linesDict['world']),
               worldDate = str(maxDateDict['world']),
               worldCountryLines = '{:,}'.format(linesDict['worldCountry']),
               worldCountryDate = str(maxDateDict['worldCountry']),
               indiaLines = '{:,}'.format(linesDict['india']),
               indiaDate = str(maxDateDict['india']),
               indiaStateLines = '{:,}'.format(linesDict['indiaState']),
               indiaStateDate = str(maxDateDict['indiaState']))
    return subject, body

def sendEmail(maxDateDict, linesDict):
    try:
        executionTime = getCurrTime()
        subject, body = prepareEmailContent(executionTime, maxDateDict, linesDict)

        hostName = 'smtp.gmail.com'
        userName = "anomaly.alert@bridgei2i.com"
        password = "Bi2i@1234"
        receiverList = ['rohit.mehrotra@bridgei2i.com']

        msg = MIMEText(body, 'html')
        msg['Subject'] = subject
        msg['From'] = (userName)
        msg['To'] = ", ".join(receiverList)

        server = smtplib.SMTP(hostName)
        server.starttls()
        server.login(userName, password)
        server.sendmail(msg.get('From'),msg["To"],msg.as_string())
        server.quit()
        return 'Email sent!'
    except:
        return "Email could't send"