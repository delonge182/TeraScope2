from pymongo import MongoClient
import pandas as pd
import json

## load csv
#dataset_gpu = pd.read_csv('data/gpu.csv')
#dataset_application = pd.read_csv('data/application-checkpoints.csv')

## save to db
client = MongoClient()
client = MongoClient('localhost', 3306)
db = client.csv_merge


#json_gpu = json.load(dataset_gpu.to_json(orient='records'))

gpus = db.gpu
xx = 0
for chunk in pd.read_csv('data/gpu.csv', chunksize = 500, low_memory=False):
  json_gpu = json.loads(chunk.to_json(orient='records'))
  gpus.insert_many(json_gpu)
  xx += 1
  print(xx)

apps = db.apps
yy = 0
for chunk in pd.read_csv('data/application-checkpoints.csv', chunksize = 1000, low_memory=False):
  json_apps = json.loads(chunk.to_json(orient='records'))
  apps.insert_many(json_apps)
  yy += 1
  print(yy)




#posts.insert_one(dataset_gpu)
#posts.insert_one(dataset_application)
