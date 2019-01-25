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
gpus = db.gpu
tiles = db.taskXY

taskset = set()
for taskid in apps.find():
  taskset.add(taskid['taskId'])

my_list = list(taskset)
print(my_list.index('1507e9b5-48b9-44ba-b97b-db634aa72971'))
print(my_list[16043])



