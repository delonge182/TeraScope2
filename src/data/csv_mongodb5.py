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
taskids_host_set = db.taskids_host
gpu_set = db.gpu

hostnameset = set()
for hostname in apps.find():
  hostnameset.add(hostname['hostname'])

my_list = list(hostnameset)
print(len(my_list))

for list_member in my_list:
  #print(list_member)
  df_gpu = pd.DataFrame(list(gpu_set.find({'hostname': list_member})))
  for gpu in df_gpu:
    df_gpu = pd.DataFrame(list(taskids_host_set.find({'hostname': gpu['hostname'], 'eventName': 'Render',
                                                      'startTime': {
                                                          '$lte': gpu['timestamp']
                                                      },
                                                      'stopTime': {
                                                          '$gte': gpu['timestamp']
                                                      }})))
  print(len(df_gpu))
  
  
df_gpu = pd.DataFrame(list(gpu_set.find({'hostname': '95b4ae6d890e4c46986d91d7ac4bf08200000U'})))  
df_gpu_cut = list(df_gpu['timestamp'])
df_gpu_cut[1]
print(len(df_gpu_cut))

test_df = pd.DataFrame(list(taskids_host_set.find({'hostname': '95b4ae6d890e4c46986d91d7ac4bf08200000U', 'eventName': 'Render',
                                                  'startTime': {
                                                      '$lte': datetime.strptime(df_gpu['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                                                  },
                                                  'stopTime': {
                                                      '$gte': datetime.strptime(df_gpu['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                                                  }})))

#for task_gpu_tile in df_task_gpu_tile:
#  print(task_gpu_tile)
#  gpu_per_task = pd.DataFrame(list(task_gpu_tile['gpu']))
#  json1 = json.loads(gpu_per_task.to_json(orient='records'))[0]
#  test_gpu_df = pd.DataFrame(json1)
#  test_gpu_df = test_gpu_df.T
#  
#  print (type(test_gpu_df.timestamp))
      
      
#### get all dataset inside the db
  