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
print(len(my_list))
print(my_list.index('2002b8c0-b3dc-4ad4-af9f-85e52b0bf628'))
#print(my_list.index('1507e9b5-48b9-44ba-b97b-db634aa72971'))
40045 - 4014
task_gpu_db = db.task_gpu

print(my_list[26870])

totalRenderStart, totalRenderStop = '', ''
counter = 26870
for taskx in my_list[26870:]:
  gpus_json = []
  tasktile1 = ''
  print(counter)
  counter += 1
  for app in apps.find({'taskId': taskx, 'eventName': 'Render'}):  
  #  print(app)
    if app['eventType'] == 'START':
      totalRenderStart = app['timestamp']
    if app['eventType'] == 'STOP':
      totalRenderStop = app['timestamp']
  
  ##### Assumption: it is not possible to process more than one process on the same host/machine/GPU
    if ((totalRenderStart != '') & (totalRenderStop != '')):
      for gpu in gpus.find({
                              'hostname': app['hostname'],
                              'timestamp': {
                                  '$gte': totalRenderStart,
                                  '$lte': totalRenderStop
                              }
                          }):
        
        task_gpu_tile = json.dumps({'result':[{
          "gpuSerial": gpu["gpuSerial"],
          "gpuUUID": gpu["gpuUUID"],
          "powerDrawWatt": gpu["powerDrawWatt"],
          "gpuTempC": gpu["gpuTempC"],
          "gpuUtilPerc": gpu["gpuUtilPerc"],
          "gpuMemUtilPerc":gpu["gpuMemUtilPerc"],
          "timestamp":gpu["timestamp"],
          "jobid": app['jobId'],
          "taskid": app['taskId'],
          "hostname": app['hostname'],
          "start": totalRenderStart,
          "stop": totalRenderStop,
          "eventname": 'Render'
        }]})
    #    print(type(task_gpu_tile))
        task_gpu_tile = json.loads(task_gpu_tile)
    #    print(type(task_gpu_tile['result']))
        task_gpu_db.insert_many(task_gpu_tile['result'])
        totalRenderStart, totalRenderStop = '',''
        
my_list[6501]
