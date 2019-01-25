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

tiles = db.taskXY
xx = 0
for chunk in pd.read_csv('data/task-x-y.csv', chunksize = 500, low_memory=False):
  json_gpu = json.loads(chunk.to_json(orient='records'))
  tiles.insert_many(json_gpu)
  xx += 1
  print(xx)





#posts.insert_one(dataset_gpu)
#posts.insert_one(dataset_application)
