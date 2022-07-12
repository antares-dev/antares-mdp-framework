# Databricks notebook source
import requests
import json
from ast import literal_eval

# COMMAND ----------

def ListNotebooks(instanceName,loc="/"):
    PAT = dbutils.secrets.get(scope = "secret-scope", key = "Databricks-PAT")
    headers = {
        'Authorization': f'Bearer {PAT}',
    }
    data_path = '{{"path": "{0}"}}'.format(loc)
    instance = instanceName
    url = '{}/api/2.0/workspace/list'.format(instance)
    response = requests.get(url, headers=headers, data=data_path)

    response.raise_for_status()
    jsonResponse = response.json()
    list = []
    for i,result in jsonResponse.items():
        for value in result:
            dump = json.dumps(value)
            data = literal_eval(dump)
            if data['object_type'] == 'DIRECTORY':
                rec_req(instanceName,data['path'])
            elif data['object_type'] == 'NOTEBOOK':
                 list.append(data['path'])
    else:
        pass
    return list

# COMMAND ----------

def ListCurrentNotebooks():
    path = "/".join(CurrentNotebookPath().split("/")[:-1])
    return ListNotebooks("https://australiaeast.azuredatabricks.net", path)

# COMMAND ----------

def CurrentNotebookPath():
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

def ListNotebooks(instanceName,loc="/"):
    PAT = dbutils.secrets.get(scope = "secret-scope", key = "Databricks-PAT")
    headers = {
        'Authorization': f'Bearer {PAT}',
    }
    data_path = '{{"path": "{0}"}}'.format(loc)
    instance = instanceName
    url = '{}/api/2.0/workspace/list'.format(instance)
    response = requests.get(url, headers=headers, data=data_path)

    response.raise_for_status()
    jsonResponse = response.json()
    list = []
    for i,result in jsonResponse.items():
        for value in result:
            dump = json.dumps(value)
            data = literal_eval(dump)
            if data['object_type'] == 'DIRECTORY':
                rec_req(instanceName,data['path'])
            elif data['object_type'] == 'NOTEBOOK':
                 list.append(data['path'])
    else:
        pass
    return list

# COMMAND ----------

INSTANCE_NAME = "https://australiaeast.azuredatabricks.net"

# COMMAND ----------

def GetAuthenticationHeader():
    pat = dbutils.secrets.get(scope = "secret-scope", key = "Databricks-PAT")
    headers = {
        'Authorization': f'Bearer {pat}',
    }
    return headers

# COMMAND ----------

def ListTokens():
    headers = GetAuthenticationHeader()
    url = f'{INSTANCE_NAME}/api/2.0/token/list'
    response = requests.get(url, headers=headers)

    response.raise_for_status()
    jsonResponse = response.json()
    print(jsonResponse)
    return
    list = []
    for i,result in jsonResponse.items():
        for value in result:
            dump = json.dumps(value)
            list.append(dump)
            print(dump)
            #data = literal_eval(dump)
            #print(data)
    else:
        pass
    #return list
ListToken()

# COMMAND ----------


