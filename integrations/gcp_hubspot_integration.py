import requests
import json
import pandas as pd
import numpy as np
import datetime as dt
import time
from datetime import timezone,timedelta
from pandas.io.json import json_normalize
import urllib.request
import urllib.parse
import html2text
from bs4 import BeautifulSoup
import re
import pydata_google_auth
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq as gbq

credentials = service_account.Credentials.from_service_account_file(
    'password file path')
client = bigquery.Client(project = 'bigquery project', credentials=credentials)

pd.set_option('display.max_columns', None)

def cleanStr_to_dict(text_list):
    new_list = []
    for i in range(len(text_list)):
        cleanString = re.sub('person_properties','pp',  text_list[i])
        cleanString = str.lower(cleanString)
        cleanString = re.sub('[ \.\!\@\#\$\%\¨\&\*\(\)\-]','_',  cleanString)
        cleanString = re.sub('(á|â|à|ã|ä)','a',  cleanString)
        cleanString = re.sub('(é|ê|è|ë|&)','e',  cleanString)
        cleanString = re.sub('(í|î|ì|ï)','i',  cleanString)
        cleanString = re.sub('(ó|ô|ò|õ|ö)','o',  cleanString)
        cleanString = re.sub('(ú|û|ù|ü)','u',  cleanString)
        cleanString = re.sub('(ç)','c',  cleanString)
        new_list.append(cleanString)
    dict_result = dict(zip(text_list, new_list))
    return dict_result

## GET ALL ENGAGEMENTS ###
 
max_results = 100000
hapikey = 'key' 
engagements = []
property_list = []
get_all_contacts_url = "https://api.hubapi.com/engagements/v1/engagements/paged?"
parameter_dict = {'hapikey': hapikey, 'limit': 250}
headers = {}

# Paginate your request using offset
has_more = True
while has_more:
    parameters = urllib.parse.urlencode(parameter_dict)
    print(parameters)
    get_url = get_all_contacts_url + parameters
    print(get_url)
    r = requests.get(url= get_url, headers = headers)
    response_dict = json.loads(r.text)
    has_more = response_dict['hasMore']
    engagements.extend(response_dict['results'])
    parameter_dict['offset'] = response_dict['offset']
    if len(engagements) >= max_results: # Exit pagination, based on whatever value you've set your max results variable to. 
        print('maximum number of results exceeded')
        break
print('loop finished')

list_length = len(engagements) 

print("You've succesfully parsed through {} contact records and added them to a list".format(list_length))
      
      
## GET RECENT ENGAGEMENTS ####

max_results = 10000
hapikey = 'key' 
engagements = []
property_list = []
get_all_contacts_url = "https://api.hubapi.com/engagements/v1/engagements/recent/modified?since=1577836800000&"
parameter_dict = {'hapikey': hapikey, 'count': 100}
headers = {}

# Paginate your request using offset
has_more = True
while has_more:
    parameters = urllib.parse.urlencode(parameter_dict)
    print(parameters)
    get_url = get_all_contacts_url + parameters
    print(get_url)
    r = requests.get(url= get_url, headers = headers)
    response_dict = json.loads(r.text)
    has_more = response_dict['hasMore']
    engagements.extend(response_dict['results'])
    parameter_dict['offset'] = response_dict['offset']
    if len(engagements) >= max_results: # Exit pagination, based on whatever value you've set your max results variable to. 
        print('maximum number of results exceeded')
        break
print('loop finished')

list_length = len(engagements) 

print("You've succesfully parsed through {} contact records and added them to a list".format(list_length))

## WRITE ENGAGEMENTS ## 

with open('hubspot_2020.json', 'w') as f:
    json.dump(engagements, f)
    
## READ ENGAGEMENTS ALL ##

with open('hubspot_total.json', 'r') as myfile:
    data=myfile.read()
engagements_19 = json.loads(data)

with open('hubspot_2020.json', 'r') as myfile:
    data=myfile.read()
engagements_20 = json.loads(data)

## CRIA DATAFRAME ###

engagements_20[-1]

df_19 = json_normalize(engagements_19)
df_19['associations.companyIds'] = df_19['associations.companyIds'].apply(lambda y: np.nan if len(y)==0 else y)
df_19['companyId'] = df_19['associations.companyIds'].str[0]
df_19['companyId1'] = df_19['associations.companyIds'].str[1]
df_19['companyId2'] = df_19['associations.companyIds'].str[2]
df_19['companyId3'] = df_19['associations.companyIds'].str[3]
df_4 = df_19[df_19['associations.companyIds'].notnull()]
df_4 = df_4[['associations.companyIds',
                'engagement.createdAt',
                'engagement.createdBy',
                'engagement.id',
                'engagement.lastUpdated',
                'engagement.modifiedBy',
                'engagement.ownerId',
                'engagement.portalId',
                'engagement.type',
                'metadata.body',
                #'metadata.completionDate',
                'metadata.from.email',
                'metadata.from.firstName',
                'metadata.from.lastName',
                'metadata.subject',
                'metadata.text',
                'metadata.threadId',
                'metadata.to',
                'companyId',
                'companyId1',
                'companyId2',
                'companyId3']]
df_4['createdAt'] = pd.to_datetime(df_4['engagement.createdAt'],unit='ms')
df_4['lastUpdated'] = pd.to_datetime(df_4['engagement.lastUpdated'],unit='ms')
df_4.drop(columns=df_4[[
    'engagement.lastUpdated',
    'engagement.createdAt']],
          inplace=True)
          
df_20 = json_normalize(engagements_20)
df_20['associations.companyIds'] = df_20['associations.companyIds'].apply(lambda y: np.nan if len(y)==0 else y)
df_20['companyId'] = df_20['associations.companyIds'].str[0]
df_20['companyId1'] = df_20['associations.companyIds'].str[1]
df_20['companyId2'] = df_20['associations.companyIds'].str[2]
df_20['companyId3'] = df_20['associations.companyIds'].str[3]
df_2 = df_20[df_20['associations.companyIds'].notnull()]
df_2 = df_2[['associations.companyIds',
                'engagement.createdAt',
                'engagement.createdBy',
                'engagement.id',
                'engagement.lastUpdated',
                'engagement.modifiedBy',
                'engagement.ownerId',
                'engagement.portalId',
                'engagement.type',
                'metadata.body',
                #'metadata.completionDate',
                'metadata.from.email',
                'metadata.from.firstName',
                'metadata.from.lastName',
                'metadata.subject',
                'metadata.text',
                'metadata.threadId',
                'metadata.to',
                'companyId',
                'companyId1',
                'companyId2',
                'companyId3']]
df_2['createdAt'] = pd.to_datetime(df_2['engagement.createdAt'],unit='ms')
df_2['lastUpdated'] = pd.to_datetime(df_2['engagement.lastUpdated'],unit='ms')
df_2.drop(columns=df_2[[
    'engagement.lastUpdated',
    'engagement.createdAt']],
          inplace=True)
          
print(len(df_2))
print(len(df_4))

result = pd.concat([df_2,df_4],sort=False)
result.drop_duplicates('engagement.id', inplace=True)
result.head().sort_values(by='metadata.threadId')
type(result)
result.rename(cleanStr_to_dict(result.columns), axis='columns', inplace=True)