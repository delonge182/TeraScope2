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

db_gpu = db.gpu
db_gpu_master = db.gpu_master

df_gpu = pd.DataFrame(list(db_gpu.find()))

df_gpu.columns

df_gpu = df_gpu.iloc[:, [2,4,6]]

df_gpu.drop_duplicates(subset ="hostname", keep = 'first', inplace = True)

df_gpu = df_gpu.sort_values(by=['gpuSerial'])
df_gpu = df_gpu.reset_index(drop=True)
df_gpu['hostnumber'] = df_gpu.index + 1

print(len(df_gpu))

db_gpu_master.insert_many(df_gpu.to_dict('records'))
