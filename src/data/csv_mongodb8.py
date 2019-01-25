from pymongo import MongoClient
import pandas as pd
import json


## load db
client = MongoClient()
client = MongoClient('localhost', 3306)
db = client.csv_merge

apps = db.apps
gpus = db.gpu
tiles = db.taskXY
taskset_render = db.task_gpu
task_gpu_tile_set2 = db.task_gpu_tile
db_task_gpu_class = db.task_gpu_class

taskset = set()
for taskid in apps.find():
  taskset.add(taskid['taskId'])

my_list = list(taskset)

for task_ith in my_list[0:100] :
  df_taskset_render = pd.DataFrame(list(taskset_render.find({'taskid': task_ith})))
  df_taskset_render = df_taskset_render.iloc[:, [1,7,12,13]] 
  
  df_task_gpu_tile2 = pd.DataFrame(list(task_gpu_tile_set2.find({'taskid': task_ith})))
  df_gpu_per_task2 = pd.DataFrame(list(df_task_gpu_tile2['gpu']))
  json2 = json.loads(df_gpu_per_task2.to_json(orient='records'))[0]
  df_gpu_per_task2 = pd.DataFrame(json2)
  df_gpu_per_task2 = df_gpu_per_task2.T
  
  df_merge1 = pd.merge(df_gpu_per_task2, df_taskset_render, on='timestamp', how='left')
  df_merge1['taskid'] = task_ith
  
  db_task_gpu_class.insert_many(db_task_gpu_class.to_dict('records'))