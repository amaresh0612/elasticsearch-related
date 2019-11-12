from elasticsearch import helpers, Elasticsearch
import pandas as pd
from daily_file_extraction import log_index, extract_statuscode, extract_3s_slowresponse, extract_1s_slowresponse
import csv
import os
import shutil
import logging
import time
import india_success_code

#Logging
logger = logging.getLogger("status_code")
logger.setLevel(logging.INFO)
handler = logging.FileHandler('status_code.log')# change to prd directory
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

es_uat_monitor = Elasticsearch(IP)
country = "in"

#create temp directory
path = "tmp_inmb"
try:
    os.mkdir(path)
except OSError:
    print("Creation of the directory %s failed" % path)
else:
    print("Successfully created the directory %s " % path)
    
  log = log_index(country)
statuscode_df = extract_statuscode(log)
Statuscode_filename = "tmp_inmb/temp_statuscode_in.csv"
statuscode_df.to_csv(Statuscode_filename, index=False)

with open(Statuscode_filename) as f:
    reader = csv.DictReader(f)
    helpers.bulk(es_uat_monitor, reader, index='statuscode_count_in_day', doc_type='statuscode_count_in_day')  
    
API_list = india_success_code.API_list
addon_success_list = india_success_code.addon_success_list
#####################################################################################

df1 = pd.read_csv(Statuscode_filename,  index_col=False)
df2 = df1[df1.statusCode.isin(success_list)]
df3 = df1[df1.service_name.isin(API_list) & df1.statusCode.isin (addon_success_list) ]
df2 = df2.append(df3)
final_df = pd.DataFrame()
final_df["total"] = df1.groupby('date_and_time')['Num_of_requests'].sum()
final_df["successful"] = df2.groupby('date_and_time')['Num_of_requests'].sum()
final_df["failure"] = final_df["total"] - final_df["successful"]
final_df["Percent_Success"] = ((final_df["successful"]/ final_df["total"])*100).round(decimals=2)
final_df["Percent_Failure"] = ((final_df["failure"]/ final_df["total"])*100).round(decimals=2)
final_df=final_df.reset_index()
agg_filename = "tmp_inmb/temp_statuscode_in_agg.csv"
final_df.to_csv(agg_filename, index=False)    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
