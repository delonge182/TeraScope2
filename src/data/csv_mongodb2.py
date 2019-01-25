from pymongo import MongoClient
import pandas as pd
import numpy as np
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
taskIds = db.taskIds
#durations = db.durations
#(tilingStart, tilingStop, totalRenderStart, totalRenderStop, savingConfigStart, 
# savingConfigStop, renderStart, renderStop, uploadStart, uploadStop) = datetime()
taskDuration = json.dumps({'result':[{'key1':'value1','key2':'value2'}, {'key1':'value3','key2':'value4'}]})

tasks = set()
for taskid in apps.find():
  tasks.add(taskid['taskId'])
  
print(len(tasks))
#print(tasks.remove(daf [29756])
my_list = list(tasks)
#print(apps.find_raw_batches())

i = 0
for taskid in (tasks):
  print (i)
  i += 1
  print(taskid)
  for app in apps.find({"taskId": taskid}):  
    if((app['eventName'] == 'Tiling') & (app['eventType'] == 'START')):
      tilingStart = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')  
    elif ((app['eventName'] == 'Tiling') & (app['eventType'] == 'STOP')):
      tilingStop = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    elif ((app['eventName'] == 'TotalRender') & (app['eventType'] == 'START')):
      totalRenderStart = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    elif ((app['eventName'] == 'TotalRender') & (app['eventType'] == 'STOP')):
      totalRenderStop = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    elif ((app['eventName'] == 'Saving Config') & (app['eventType'] == 'START')):
      savingConfigStart = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    elif ((app['eventName'] == 'Saving Config') & (app['eventType'] == 'STOP')):
      savingConfigStop = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    elif ((app['eventName'] == 'Render') & (app['eventType'] == 'START')):
      renderStart = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    elif ((app['eventName'] == 'Render') & (app['eventType'] == 'STOP')):
      renderStop = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    elif ((app['eventName'] == 'Uploading') & (app['eventType'] == 'START')):
      uploadStart = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    elif ((app['eventName'] == 'Uploading') & (app['eventType'] == 'STOP')):
      uploadStop = datetime.strptime(app['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
  
#  tempJSON = json.loads(taskDuration)
#  tempJSON['taskid'] = app['taskId']
#  tempJSON['eventName'] = 'Tiling'
#  tempJSON['startTime'] = str(tilingStart)
#  tempJSON['stopTime'] = str(tilingStop)
#  tempJSON['duration'] = (tilingStop-tilingStart).microseconds/1000
##  json.dumps(tempJSON)
#  oldData = json.loads(taskDuration)
#  taskDuration =  oldData + tempJSON
    
  tempJSON = json.loads(taskDuration)
  new_result = [{'taskid':app['taskId'], 
                 'eventName':'Tiling', 
                 'startTime':str(tilingStart), 
                 'stopTime':str(tilingStop), 
                 'duration':((tilingStop-tilingStart).seconds*1000) + ((tilingStop-tilingStart).microseconds/1000)},
                {'taskid':app['taskId'], 
                 'eventName':'TotalRender', 
                 'startTime':str(totalRenderStart), 
                 'stopTime':str(totalRenderStop), 
                 'duration':((totalRenderStop-totalRenderStart).seconds*1000) + ((totalRenderStop-totalRenderStart).microseconds/1000)},
                {'taskid':app['taskId'], 
                 'eventName':'Saving Config', 
                 'startTime':str(savingConfigStart), 
                 'stopTime':str(savingConfigStop), 
                 'duration':((savingConfigStop-savingConfigStart).seconds*1000) + ((savingConfigStop-savingConfigStart).microseconds/1000)},
                {'taskid':app['taskId'], 
                 'eventName':'Render', 
                 'startTime':str(renderStart), 
                 'stopTime':str(renderStop), 
                 'duration':((renderStop-renderStart).seconds*1000) + ((renderStop-renderStart).microseconds/1000)},
                {'taskid':app['taskId'], 
                 'eventName':'Uploading', 
                 'startTime':str(uploadStart), 
                 'stopTime':str(uploadStop), 
                 'duration':((uploadStop-uploadStart).seconds*1000) + ((uploadStop-uploadStart).microseconds/1000)}]
  
  # assign 'new_result' as a key
  tempJSON['new_result'] = new_result
  taskIds.insert_many(tempJSON['new_result'])  
  

#print(taskDuration)

#print('tiling start: ', tilingStart, ' tiling stop: ', tilingStop)
#
#stt = datetime.strptime('2018-11-08 08:16:35.157000', '%Y-%m-%d %H:%M:%S.%f')
#stp = datetime.strptime('2018-11-08 08:17:20.156000', '%Y-%m-%d %H:%M:%S.%f')
#print(stt, ' ', stp)
#print(stp-stt)
#print('delta: ', ((stp-stt).seconds*1000) + ((stp-stt).microseconds/1000))
