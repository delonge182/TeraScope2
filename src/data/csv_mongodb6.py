from pymongo import MongoClient
import pandas as pd
import json
from datetime import datetime

## load csv
#dataset_gpu = pd.read_csv('data/gpu.csv')
#dataset_application = pd.read_csv('data/application-checkpoints.csv')

## save to db
client = MongoClient()
client = MongoClient('localhost', 3306)
db = client.csv_merge

apps = db.apps
taskid_set = db.taskIds
taskid_host = db.taskids_host

df_taskid = pd.DataFrame(list(taskid_set.find()))
df_apps = pd.DataFrame(list(apps.find({'eventType': 'START'})))
dataframe3 = pd.merge(df_taskid, df_apps, left_on=['taskid', 'eventName'], right_on=['taskId', 'eventName'], how='left')

dataframe3.columns

df_cut = dataframe3.iloc[:, [1,2,3,4,5,8, 9]]

print(len(dataframe3))

taskid_host.insert_many(df_cut.to_dict('records'))
