#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 25 17:43:35 2022

@author: Jaghen
"""

import requests 
import json
import re
import zipfile
import tempfile
import gzip 
import pandas as pd
from datetime import datetime
import glob
import os
from google.cloud import secretmanager
from sqlalchemy import create_engine


###############################################################################
#     Section commented because the database doesnt exist, is a dummy data    #
###############################################################################
#load Functions

# def load_credentials(project_id,name_secret):
#     secret_id = f"projects/{project_id}/secrets/{name_secret}/versions/latest"    
#     client = secretmanager.SecretManagerServiceClient()
#     response = client.access_secret_version(request={"name": secret_id})
#     payload = response.payload.data.decode("UTF-8")
#     cred = json.loads(payload)
    
#     return cred

#def msql_write(df,schema,table,env,write_type):

#    user = config.get(f'DB_{env}').get("user")
#    password = config.get(f'DB_{env}').get("password")
#    host = config.get(f'DB_{env}').get("host")
#    port = config.get(f'DB_{env}').get("port_write")
#    
#    engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + ":" + port + "/" + schema)

    #write
#    df.to_sql(table, con = engine, if_exists = write_type,index = False, chunksize = 10000)
#    print(f'Table on {env}: {schema}.{table} updated.')


#Get credentials for database

#db_cred_prod = load_credentials("test",'db_test')

#config = {
#    "DB_PROD": {
#        "user": db_cred_prod["user"],
#        "password": db_cred_prod["password"],
#        "host": db_cred_prod["host"],
#        'port_read': db_cred_prod["port_read"],
#        'port_write': db_cred_prod["port_write"],
#    }
#}

# Request data from url 
url = f'https://smn.conagua.gob.mx/webservices/?method=3'
response = requests.get(url,auth = None, verify = False, stream=True)

# save gz file to temp 
with open('incoming', 'wb') as f:
    for chunk in response.raw.stream(1024, decode_content=False):
        if chunk:
            f.write(chunk)
zipfile = ('incoming')
dataList = json.loads(gzip.open(zipfile).read())
df = pd.DataFrame.from_records(dataList,index=range(len(dataList)))

# Get las 2 hours regs

df['time'] = df['hloc'].str.replace('T','').astype('int')

df = df.sort_values(by=['ides','idmun','hloc',], ascending=[True,True,False], na_position='last')

current = int(datetime.today().strftime('%Y%m%d%H'))
last2_h = int(datetime.today().strftime('%Y%m%d%H'))-2

df = df[(df['time'] >= last2_h) & (df['time'] <= current)]

# Calculate mean by prec and temp

df['temp'] = df['temp'].astype('float') 
df['prec'] = df['prec'].astype('float') 
df_temp_prec = df.groupby(['ides','idmun','nes','nmun'],as_index=False).agg({'prec':'mean','temp':'mean'})
df_temp_prec['ides'] = df_temp_prec['ides'].astype('int')
df_temp_prec['idmun'] = df_temp_prec['idmun'].astype('int') 
# search all files inside a specific folder

path_file = []
for file in glob.glob('**/*.*',recursive=True):
    if 'data_municipios' in file:
        path = path_file.append(file)

path_file.sort(key = lambda x: x.split('/')[1])
df_mun = pd.read_csv(path_file[-1])

df_temp_prec.rename(columns={'ides':'Cve_Ent','idmun':'Cve_Mun'}, inplace=True)

df_mun = df_mun.merge(df_temp_prec, on=['Cve_Ent','Cve_Mun'], how='left')
df_mun['updated_at'] = int(datetime.today().strftime('%Y%m%d%H'))

# Create table on database

###############################################################################
#     Section commented because the database doesnt exist, is a dummy data    #
###############################################################################

#schema = 'test'
#table = 'mun_temp_prec'  

#msql_write(df_mun,schema,table,env = 'PROD',write_type = 'replace')
    
#print('Write Finished')

print('Process Finished')

