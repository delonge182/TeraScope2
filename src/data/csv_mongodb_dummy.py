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
task_gpu_tile_db = db.task_gpu_tile
totalRenderStart, totalRenderStop = '', ''
counter = 0
for app in apps.find({'eventName': 'TotalRender', 'taskId':'3839045b-782f-4bf1-abdf-2b0528b43362'}):  
#  print(app)
  print(counter)
  counter += 1
  if app['eventType'] == 'START':
    totalRenderStart = app['timestamp']
  if app['eventType'] == 'STOP':
    totalRenderStop = app['timestamp']

##### Assumption: it is not possible to process more than one process on the same host/machine/GPU
  if ((totalRenderStart != '') & (totalRenderStop != '')):
    gpus_json = []
    for gpu in gpus.find({
                            'hostname': app['hostname'],
                            'timestamp': {
                                '$gte': totalRenderStart,
                                '$lte': totalRenderStop
                            }
                        }):
      
      temp_json = {
        "gpuSerial": gpu["gpuSerial"],
        "gpuUUID": gpu["gpuUUID"],
        "powerDrawWatt": gpu["powerDrawWatt"],
        "gpuTempC": gpu["gpuTempC"],
        "gpuUtilPerc": gpu["gpuUtilPerc"],
        "gpuMemUtilPerc":gpu["gpuMemUtilPerc"]
      }
      gpus_json.append(temp_json)
      
      print(gpus_json)
#      print(type(gpus_json))