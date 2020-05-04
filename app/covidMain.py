
# coding: utf-8

# # Importing Libraries
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
import logging


pd.options.mode.chained_assignment = None  # default='warn'
logging.basicConfig(filename='main.log',level=logging.DEBUG)
logging.info('''Importing Libraries''')


logging.info('''Importing Custom Modules''')
from app.modules.covidUtils import *


# # Defining Required Variables

logging.info('''Defining Variables''')
mainPath = './data/'
outputDataPath = './outputData/'
metricList = ['confirmed', 'recovered', 'deaths']
maxDateDict = {}
linesDict = {}

countryLatLongCsvName = 'countryLatLong.csv'
countryMappingGoogleName = 'countryMappingGoogle.csv'
indiaStateMappingName = 'indiaStateMapping.csv'

worldDataUrl = 'https://pomber.github.io/covid19/timeseries.json'
indiaDataUrl = 'https://api.rootnet.in/covid19-in/stats/history'


# # Pulling Local Data

logging.info('''Pulling Local Data''')
dfLatLong = pd.read_csv(mainPath + countryLatLongCsvName)
# logging.info(dfLatLong.shape)

'''Making a dictionary of country mapping to standardize country names'''
dfCountryMappingGoogle = pd.read_csv(mainPath + countryMappingGoogleName)
# logging.info(dfCountryMappingGoogle.shape)

countryMappingGoogleDict = {}
for obj in dfCountryMappingGoogle.to_dict(orient = 'records'):
    key = obj['current']
    value = obj['subs']
    countryMappingGoogleDict[key] = value
# countryMappingGoogleDict  

'''Making a dictionary of country mapping to standardize country names'''
dfindiaStateMapping = pd.read_csv(mainPath + indiaStateMappingName)
# logging.info(dfindiaStateMapping.shape)

indiaStateMappingDict = {}
for obj in dfindiaStateMapping.to_dict(orient = 'records'):
    key = obj['current']
    value = obj['subs']
    indiaStateMappingDict[key] = value
# indiaStateMappingDict  


# # Pulling Data from APIs

# def getDataFromApi():
#     logging.info('''Pulling Data from APIs''')
#     logging.info('Pulling World Data')
#     response, worldDataJson = pullData(worldDataUrl)
#     logging.info(response)

#     logging.info('Pulling India Data')
#     response, indiaDataJson = pullData(indiaDataUrl)
#     logging.info(response)

#     return 


# # Making Required Dataframes from the Json

# ### World

logging.info('''Making Required Dataframes from the Json''')
logging.info('''World''')

def makeWorldData():

    '''Get Data from API'''
    response, worldDataJson = pullData(worldDataUrl)
    logging.info(response)

    '''Defining Required Variables'''
    formatIn = '%m-%d-%Y'

    '''Making dataframe from the response Json'''
    dfFinal = pd.DataFrame()
    for key in worldDataJson.keys():
        dfTemp = pd.DataFrame.from_dict(worldDataJson[key])
        dfTemp['country'] = key
        dfFinal = pd.concat([dfFinal, dfTemp])
    # logging.info(dfFinal.shape)

    '''Formatting date to timestamp'''
    dfFinal['dateFormatted'] = dfFinal['date'].apply(addZeroPaddingToDate) 
    dfFinal['timestamp'] = dfFinal['dateFormatted'].apply(lambda x: convertStringToTimestampNew(x, formatIn)) 

    '''Groupby Timestamp'''
    dfFinal = dfFinal.groupby(['timestamp']).agg({'confirmed':'sum', 
                                                'deaths':'sum',
                                                'recovered':'sum'})[['confirmed','deaths','recovered']].reset_index()

    # logging.info(dfFinal.shape)
    '''Sort by Timestamp'''
    dfFinal.sort_values(by = ['timestamp'], inplace = True)


    '''Adding Other Relevant Metrics'''
    dfFinal['active'] = dfFinal['confirmed'] - dfFinal['deaths'] - dfFinal['recovered']

    dfFinal['newConfirmed'] = dfFinal['confirmed'] - dfFinal['confirmed'].shift(1)
    dfFinal['newConfirmed'] = np.where(dfFinal['newConfirmed'].isna(),
                                    dfFinal['confirmed'],
                                    dfFinal['newConfirmed'])

    dfFinal['newDeaths'] = dfFinal['deaths'] - dfFinal['deaths'].shift(1)
    dfFinal['newDeaths'] = np.where(dfFinal['newDeaths'].isna(),
                                    dfFinal['deaths'],
                                    dfFinal['newDeaths'])

    dfFinal['newRecovered'] = dfFinal['recovered'] - dfFinal['recovered'].shift(1)
    dfFinal['newRecovered'] = np.where(dfFinal['newRecovered'].isna(),
                                    dfFinal['recovered'],
                                    dfFinal['newRecovered'])

    '''Adding Growth Rate & Doubling Time'''
    dfFinal['growthRate'] = np.where((dfFinal['timestamp'].eq(dfFinal['timestamp'].shift(1))), 
                                    dfFinal['newConfirmed']/dfFinal['confirmed'].shift(1),
                                    np.nan)
    dfFinal['growthRate'].fillna(0.0, inplace = True)
    dfFinal['growthRate'].replace(np.inf, 0.0, inplace = True)

    dfFinal['movingAvg'] = dfFinal.groupby('timestamp')['growthRate'].rolling(7).mean().reset_index(drop = True)
    dfFinal['movingAvg'].fillna(0.0, inplace = True)
    dfFinal['movingAvg'].replace(np.inf, 0.0, inplace = True)

    dfFinal['doublingTime'] = np.log10(2)/np.log10(dfFinal['movingAvg']+1)
    dfFinal['doublingTime'].fillna(0.0, inplace = True)
    dfFinal['doublingTime'].replace(np.inf, 0.0, inplace = True)
    dfFinal['doublingTime'] = dfFinal['doublingTime'].apply(lambda x: int(round(x,0)))

    '''Making a cross-joined table of all combinations'''
    dfTimestamp = dfFinal[['timestamp']].drop_duplicates()
    dfTimestamp21stJan = pd.DataFrame({'timestamp': dfTimestamp['timestamp'].min() - datetime.timedelta(1)}, index = [0])
    dfTimestamp = pd.concat([dfTimestamp, dfTimestamp21stJan])
    # logging.info(dfTimestamp.shape)

    '''Merging with the existing dataframe'''
    dfLeft = pd.merge(dfTimestamp,
                    dfFinal,
                    on = ['timestamp'],
                    how = 'left')
    # logging.info(dfLeft.shape)
    dfLeft.fillna(0.0, inplace = True)

    dfFinal = dfLeft.copy(deep = True)
    dfFinal.sort_values(['timestamp'], ascending = [True], inplace = True)
    dfFinal.reset_index(drop = True, inplace = True)

    '''Adding Days Column'''
    dfFinal['Days'] = dfFinal.index

    '''Convert to JSON'''
    dfWorldGoogle = dfFinal.copy(deep = True)
    dfWorldGoogle['timestamp'] = dfWorldGoogle['timestamp'].apply(convertTimestampToString)
    dfWorldGoogle.to_csv(outputDataPath + 'dfWorld.csv', index = False)    
    jsonData = convertDfToJson(dfWorldGoogle, 'records')

    return jsonData



# ### World - Country
logging.info('''World - Country''')

def makeCountryData():

    '''Get Data from API'''
    response, worldDataJson = pullData(worldDataUrl)
    logging.info(response)

    '''Defining Required Variables'''
    formatIn = '%m-%d-%Y'

    '''Making dataframe from the response Json'''
    dfFinal = pd.DataFrame()
    for key in worldDataJson.keys():
        dfTemp = pd.DataFrame.from_dict(worldDataJson[key])
        dfTemp['country'] = key
        dfFinal = pd.concat([dfFinal, dfTemp])
    # logging.info(dfFinal.shape)

    '''Formatting date to timestamp'''
    dfFinal['dateFormatted'] = dfFinal['date'].apply(addZeroPaddingToDate) 
    dfFinal['timestamp'] = dfFinal['dateFormatted'].apply(lambda x: convertStringToTimestampNew(x, formatIn)) 

    '''Standardizing Country Name'''
    dfFinal['country'] = dfFinal['country'].apply(lambda x: standCountry(x, countryMappingGoogleDict))

    '''Sort by Timestamp & Country'''
    dfFinal.sort_values(by = ['country', 'timestamp'], inplace = True)


    '''Adding New Cases Columns'''
    colDim = ['country']
    # colDim = ['country']
    for metric in metricList:
        dfFinal = makeNewCasesCols(dfFinal, metric, colDim)
        
    summed = 0
    '''Finding sum of negative values'''
    for metric in metricList:
        summed += len(dfFinal[dfFinal['new' + metric.capitalize()]<0])


    '''Imputing incorrect values'''
    colDim = ['country']
    colName = 'country'
    counter = 1
    startTime = datetime.datetime.now()
    while summed>0:
        summed = 0
        for metric in metricList:
    #         dfFinal = imputeIncorrectCountry(dfFinal, metric, colDim)
            dfFinal = imputeIncorrect(dfFinal, metric, colName)
            dfFinal = makeNewCasesCols(dfFinal, metric, colDim)
            summed += len(dfFinal[dfFinal['new' + metric.capitalize()]<0])
        counter += 1
        
    logging.info('Ran for {counter} times'.format(counter = counter))    
    endTime = datetime.datetime.now()
    duration = endTime-startTime
    logging.info(durationtoString(duration))
    # logging.info(dfFinal.shape)


    '''Adding Growth Rate & Doubling Time'''
    dfFinal['growthRate'] = np.where((dfFinal['country'].eq(dfFinal['country'].shift(1))), 
                                    dfFinal['newConfirmed']/dfFinal['confirmed'].shift(1),
                                    np.nan)
    dfFinal['growthRate'].fillna(0.0, inplace = True)
    dfFinal['growthRate'].replace(np.inf, 0.0, inplace = True)

    dfFinal['movingAvg'] = dfFinal.groupby('country')['growthRate'].rolling(7).mean().reset_index(drop = True)
    dfFinal['movingAvg'].fillna(0.0, inplace = True)
    dfFinal['movingAvg'].replace(np.inf, 0.0, inplace = True)

    dfFinal['doublingTime'] = np.log10(2)/np.log10(dfFinal['movingAvg']+1)
    dfFinal['doublingTime'].fillna(0.0, inplace = True)
    dfFinal['doublingTime'].replace(np.inf, 0.0, inplace = True)
    dfFinal['doublingTime'] = dfFinal['doublingTime'].apply(lambda x: int(round(x,0)))

    '''Making a cross-joined table of all combinations'''
    dfCountry = dfFinal[['country']].drop_duplicates()
    dfCountry['key'] = 1
    dfTimestamp = dfFinal[['timestamp']].drop_duplicates()
    dfTimestamp21stJan = pd.DataFrame({'timestamp': dfTimestamp['timestamp'].min() - datetime.timedelta(1)}, index = [0])
    dfTimestamp = pd.concat([dfTimestamp, dfTimestamp21stJan])
    dfTimestamp['key'] = 1
    # logging.info(dfCountry.shape)
    # logging.info(dfTimestamp.shape)
    # logging.info(dfCountry.shape[0]*dfTimestamp.shape[0])

    dfLeft = pd.merge(dfCountry,
                    dfTimestamp,
                    on = 'key',
                    how = 'inner')
    # logging.info(dfLeft.shape)
    dfLeft = dfLeft[['country', 'timestamp']]
    # logging.info(dfLeft.shape)

    '''Merging with the existing dataframe'''
    dfLeft = pd.merge(dfLeft,
                    dfFinal,
                    on = ['country', 'timestamp'],
                    how = 'left')
    # logging.info(dfLeft.shape)
    dfLeft.fillna(0.0, inplace = True)

    dfFinal = dfLeft.copy(deep = True)
    dfFinal.sort_values(['country', 'timestamp'], ascending = [True, True], inplace = True)
    dfFinal.reset_index(drop = True, inplace = True)

    '''Join to get the Lat/Long'''
    # logging.info(dfFinal.shape)
    dfFinal = pd.merge(dfFinal,
                    dfLatLong,
                    how = 'left',
                    on = ['country'])
    # logging.info(dfFinal.shape)

    '''Retain Rows from First Infection'''
    dfCountry = dfFinal.copy(deep = True)
    dfCountry['firstInfectionMinus1Flag'] = np.where((dfCountry['confirmed'] == 0.0)
                                                    &
                                                    (dfCountry['confirmed'].shift(-1) > 0.0)
                                                    &
                                                    (dfCountry['country'].eq(dfCountry['country'].shift(-1))),
                                                    1,
                                                    0)
    # logging.info(dfCountry.shape)
    dfCountryFinal = dfCountry[(dfCountry['firstInfectionMinus1Flag'] == 1)
                            |
                            (dfCountry['confirmed']>0.0)]
    # dfCountryFinal = dfCountry[dfCountry['confirmed']>0.0]
    dfCountryFinal['Days'] = dfCountryFinal.groupby(['country'])['timestamp'].rank(method = 'first')
    dfCountryFinal['Days'] = dfCountryFinal['Days']-1

    '''Adding Active Cases'''
    dfCountryFinal['active'] = dfCountryFinal['confirmed'] - dfCountryFinal['deaths'] - dfCountryFinal['recovered']

    '''Output to CSV (Replace with Google Sheet Upload)'''
    dfCountryFinal.replace('NULL', np.nan, inplace = True)
    colOrder = ['country', 'timestamp', 'lat', 'long',
                'confirmed', 'recovered', 'deaths', 'active',
                'newConfirmed', 'newRecovered', 'newDeaths', 'Days',
            'growthRate', 'movingAvg', 'doublingTime']
    dfCountryFinal = dfCountryFinal[colOrder]



    '''UPDATE to Google Sheets'''
    dfCountryFinalGoogle = dfCountryFinal.copy(deep = True)
    dfCountryFinalGoogle['timestamp'] = dfCountryFinalGoogle['timestamp'].apply(convertTimestampToString)
    dfCountryFinalGoogle.fillna('', inplace = True)
    dfCountryFinalGoogle.to_csv(outputDataPath + 'dfWorldCountry.csv', index = False)
    jsonData = convertDfToJson(dfCountryFinalGoogle, 'records')

    return jsonData







# ### India

logging.info('''India''')

def makeIndiaData():

    '''Pulling Data from API'''
    response, indiaDataJson = pullData(indiaDataUrl)

    #Make the necessary Dataframe
    df = pd.DataFrame.from_dict(indiaDataJson['data'], orient = 'columns')
    formatIn = '%Y-%m-%d'

    dfIndia = df[['day', 'summary']]
    dfIndia['deaths'] = dfIndia['summary'].apply(lambda x: x['deaths'])
    dfIndia['recovered'] = dfIndia['summary'].apply(lambda x: x['discharged'])
    dfIndia['confirmed'] = dfIndia['summary'].apply(lambda x: x['total'])
    dfIndia['timestamp'] = dfIndia['day'].apply(lambda x: convertStringToTimestampNew(x, formatIn))
    dfIndia = dfIndia[['timestamp', 'confirmed', 'recovered', 'deaths']]

    '''Making a cross-joined table of all combinations'''
    dfTimestamp = dfIndia[['timestamp']].drop_duplicates()
    dfTimestamp21stJan = pd.DataFrame({'timestamp': dfTimestamp['timestamp'].min() - datetime.timedelta(1)}, index = [0])
    dfTimestamp = pd.concat([dfTimestamp, dfTimestamp21stJan])
    # logging.info(dfTimestamp.shape)

    '''Merging with the existing dataframe'''
    dfLeft = pd.merge(dfTimestamp,
                    dfIndia,
                    on = ['timestamp'],
                    how = 'left')
    # logging.info(dfLeft.shape)
    dfLeft.fillna(0.0, inplace = True)

    dfIndia = dfLeft.copy(deep = True)
    dfIndia.sort_values(['timestamp'], ascending = [True], inplace = True)
    dfIndia.reset_index(drop = True, inplace = True)


    dfIndia['newConfirmed'] = dfIndia['confirmed'] - dfIndia['confirmed'].shift(1)
    dfIndia['newConfirmed'] = np.where(dfIndia['newConfirmed'].isna(),
                                    dfIndia['confirmed'],
                                    dfIndia['newConfirmed'])

    dfIndia['newDeaths'] = dfIndia['deaths'] - dfIndia['deaths'].shift(1)
    dfIndia['newDeaths'] = np.where(dfIndia['newDeaths'].isna(),
                                    dfIndia['deaths'],
                                    dfIndia['newDeaths'])

    dfIndia['newRecovered'] = dfIndia['recovered'] - dfIndia['recovered'].shift(1)
    dfIndia['newRecovered'] = np.where(dfIndia['newRecovered'].isna(),
                                    dfIndia['recovered'],
                                    dfIndia['newRecovered'])

    dfIndia['growthRate'] = dfIndia['newConfirmed']/dfIndia['confirmed'].shift(1)
    dfIndia['growthRate'].fillna(0.0, inplace = True)
    dfIndia['growthRate'].replace(np.inf, 0.0, inplace = True)

    dfIndia['movingAvg'] = dfIndia['growthRate'].rolling(window=7).mean()
    dfIndia['movingAvg'].replace(np.inf, 0.0, inplace = True)
    dfIndia['movingAvg'].fillna(0.0, inplace = True)

    dfIndia['doublingTime'] = np.log10(2)/np.log10(dfIndia['movingAvg']+1)
    dfIndia['doublingTime'].fillna(0.0, inplace = True)
    dfIndia['doublingTime'].replace(np.inf, 0.0, inplace = True)
    dfIndia['doublingTime'] = dfIndia['doublingTime'].apply(lambda x: int(round(x,0)))

    dfIndia['Days'] = dfIndia.index


    '''UPDATE to Google Sheets'''
    dfIndiaGoogle = dfIndia.copy(deep = True)
    dfIndiaGoogle['timestamp'] = dfIndiaGoogle['timestamp'].apply(convertTimestampToString)
    dfIndiaGoogle.to_csv(outputDataPath + 'dfIndia.csv', index = False)
    
    jsonData = convertDfToJson(dfIndiaGoogle, 'records')
    return jsonData


# ### India State
logging.info('''India - State''')

def makeIndiaStateData():
    '''Pulling Data from API'''
    response, indiaDataJson = pullData(indiaDataUrl)

    #Make the necessary Dataframe
    df = pd.DataFrame.from_dict(indiaDataJson['data'], orient = 'columns')

    formatIn = '%Y-%m-%d'

    # logging.info(df.shape)
    dfIndiaStates = pd.DataFrame()
    for index in df.index:
        day = str(df['day'].values[index])
        dfTemp = pd.DataFrame(df['regional'].values[index])
        dfTemp['day'] = day
        dfIndiaStates = pd.concat([dfIndiaStates, dfTemp])
    # logging.info(dfIndiaStates.shape)

    '''Rename Columns'''
    dfIndiaStates.rename(columns = {'loc': 'state',
                                    'totalConfirmed': 'confirmed',
                                    'discharged': 'recovered'}, inplace = True)
    dfIndiaStates.drop(columns = ['confirmedCasesForeign', 'confirmedCasesIndian'], inplace = True)

    '''Timestamp Col Conversion'''
    dfIndiaStates['timestamp'] = dfIndiaStates['day'].apply(lambda x: convertStringToTimestampNew(x, formatIn))

    '''Sorting'''
    dfIndiaStates.sort_values(by = ['state', 'timestamp'], inplace = True)

    '''Standardizing State Name'''
    dfIndiaStates['state'] = dfIndiaStates['state'].apply(lambda x: standCountry(x, indiaStateMappingDict))

    '''Retain Rows from First Infection'''
    dfIndiaStates = dfIndiaStates.groupby(['state', 'timestamp']).agg({'confirmed':'sum', 
                                                                    'deaths':'sum',
                                                                    'recovered':'sum'})[['confirmed','deaths','recovered']].reset_index()


    '''Adding New Cases Columns'''
    colDim = ['state']
    # colDim = ['country']
    for metric in metricList:
        dfIndiaStates = makeNewCasesCols(dfIndiaStates, metric, colDim)
        
    summed = 0
    '''Finding sum of negative values & removing incorrect values'''
    for metric in metricList:
        metricNew = 'new' + metric.capitalize()
        summed += len(dfIndiaStates[dfIndiaStates[metricNew]<0])


    '''Imputing incorrect values'''
    colDim = ['state']
    colName = 'state'
    counter = 1
    startTime = datetime.datetime.now()
    while summed>0:
        summed = 0
        for metric in metricList:
    #         dfIndiaStates = imputeIncorrectCountry(dfIndiaStates, metric, colDim)
            dfIndiaStates = imputeIncorrect(dfIndiaStates, metric, colName)
            dfIndiaStates = makeNewCasesCols(dfIndiaStates, metric, colDim)
            summed += len(dfIndiaStates[dfIndiaStates['new' + metric.capitalize()]<0])
        counter += 1
        
    logging.info('Ran for {counter} times'.format(counter = counter))    
    endTime = datetime.datetime.now()
    duration = endTime-startTime
    logging.info(durationtoString(duration))
    # logging.info(dfIndiaStates.shape)

    '''Adding Growth Rate & Doubling Time'''
    dfIndiaStates['growthRate'] = np.where((dfIndiaStates['state'].eq(dfIndiaStates['state'].shift(1))), 
                                    dfIndiaStates['newConfirmed']/dfIndiaStates['confirmed'].shift(1),
                                    np.nan)
    dfIndiaStates['growthRate'].fillna(0.0, inplace = True)
    dfIndiaStates['growthRate'].replace(np.inf, 0.0, inplace = True)

    dfIndiaStates['movingAvg'] = dfIndiaStates.groupby('state')['growthRate'].rolling(7).mean().reset_index(drop = True)
    dfIndiaStates['movingAvg'].fillna(0.0, inplace = True)
    dfIndiaStates['movingAvg'].replace(np.inf, 0.0, inplace = True)

    dfIndiaStates['doublingTime'] = np.log10(2)/np.log10(dfIndiaStates['movingAvg']+1)
    dfIndiaStates['doublingTime'].fillna(0.0, inplace = True)
    dfIndiaStates['doublingTime'].replace(np.inf, 0.0, inplace = True)
    dfIndiaStates['doublingTime'] = dfIndiaStates['doublingTime'].apply(lambda x: int(round(x,0)))


    '''Making a cross-joined table of all combinations'''
    dfState = dfIndiaStates[['state']].drop_duplicates()
    dfState['key'] = 1
    dfTimestamp = dfIndiaStates[['timestamp']].drop_duplicates()
    dfTimestamp21stJan = pd.DataFrame({'timestamp': dfTimestamp['timestamp'].min() - datetime.timedelta(1)}, index = [0])
    dfTimestamp = pd.concat([dfTimestamp, dfTimestamp21stJan])
    dfTimestamp['key'] = 1

    dfLeft = pd.merge(dfState,
                    dfTimestamp,
                    on = 'key',
                    how = 'inner')
    # logging.info(dfLeft.shape)
    dfLeft = dfLeft[['state', 'timestamp']]
    # logging.info(dfLeft.shape)

    '''Merging with the existing dataframe'''
    dfLeft = pd.merge(dfLeft,
                    dfIndiaStates,
                    on = ['state', 'timestamp'],
                    how = 'left')
    # logging.info(dfLeft.shape)
    dfLeft.fillna(0.0, inplace = True)

    dfIndiaStates = dfLeft.copy(deep = True)
    dfIndiaStates.sort_values(['state', 'timestamp'], ascending = [True, True], inplace = True)
    dfIndiaStates.reset_index(drop = True, inplace = True)

    '''Retain Rows from First Infection'''
    dfIndiaStatesSubset = dfIndiaStates.copy(deep = True)
    dfIndiaStatesSubset['firstInfectionMinus1Flag'] = np.where((dfIndiaStatesSubset['confirmed'] == 0.0)
                                                                &
                                                                (dfIndiaStatesSubset['confirmed'].shift(-1) > 0.0)
                                                                &
                                                                (dfIndiaStatesSubset['state'].eq(dfIndiaStatesSubset['state'].shift(-1))),
                                                                1,
                                                                0)
    # logging.info(dfIndiaStatesSubset.shape)
    dfIndiaStatesFinal = dfIndiaStatesSubset[(dfIndiaStatesSubset['firstInfectionMinus1Flag'] == 1)
                                            |
                                            (dfIndiaStatesSubset['confirmed']>0.0)]
    # dfCountryFinal = dfCountry[dfCountry['confirmed']>0.0]
    dfIndiaStatesFinal['Days'] = dfIndiaStatesFinal.groupby(['state'])['timestamp'].rank(method = 'first')
    dfIndiaStatesFinal['Days'] = dfIndiaStatesFinal['Days']-1

    '''Adding Active Cases'''
    dfIndiaStatesFinal['active'] = dfIndiaStatesFinal['confirmed'] - dfIndiaStatesFinal['deaths'] - dfIndiaStatesFinal['recovered']

    '''UPDATE to Google Sheets'''
    colOrder = ['state', 'timestamp', 
                'confirmed', 'recovered', 'deaths', 'active',
                'newConfirmed', 'newRecovered', 'newDeaths', 'Days',
            'growthRate', 'movingAvg', 'doublingTime']
    dfIndiaStatesFinal = dfIndiaStatesFinal[colOrder]

    dfIndiaStatesFinalGoogle = dfIndiaStatesFinal.copy(deep = True)
    dfIndiaStatesFinalGoogle['timestamp'] = dfIndiaStatesFinalGoogle['timestamp'].apply(convertTimestampToString)
    dfIndiaStatesFinalGoogle.fillna('', inplace = True)
    dfIndiaStatesFinalGoogle.to_csv(outputDataPath + 'dfIndiaStates.csv', index = False)
    jsonData = convertDfToJson(dfIndiaStatesFinalGoogle, 'records')
    return jsonData