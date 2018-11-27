
#!/usr/bin/python3


# 1. Which libraries do I need ?
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch,helpers
import json
import datetime
from consts import *
#from pyes.connection import connect

#------------------------------涓€銆乺ead  ES  data

# Convert a panda's dataframe to json
def append_index(host,port,df,ES_index,fromnrow):  #add
    es=Elasticsearch(host=host, port=port, http_auth=(es_user, es_pwd))
# Add a id for looping into elastic search index
    #df["myindex"] = [x+1 for x in range(len(df.ix[:,0]))] #first column
# Convert into json
    df=df.tail(fromnrow)
    tmp = df.to_json(orient = "records",date_format="iso")
# Load each record into json format before bulk
    df_json= json.loads(tmp)
    print (df_json[0])
# Create an index into elastic search. If we need to create a mapping, we ought to do it here
    index_name = ES_index
# Bulk index
    for doc in df_json:

        es.index(body=doc, index=index_name,doc_type="data")   #doc_type 鏄换鎰忚祴鍊肩殑
#id=doc["myindex"],
def update_index(host,port,df,ES_index):  #fromnrow 没用
    es=Elasticsearch(host=host, port=port, http_auth=(es_user, es_pwd))
# Add a id for looping into elastic search index
    #df["myindex"] = [x+1 for x in range(len(df.ix[:,0]))] #first column
# Convert into json
    tmp = df.to_json(orient = "records",date_format="iso")
# Load each record into json format before bulk
    df_json= json.loads(tmp)
    print (df_json[0])
# Create an index into elastic search. If we need to create a mapping, we ought to do it here
    index_name = ES_index
# Bulk index
    es.indices.delete(index=index_name, ignore=[400, 404])
    for doc in df_json:
        es.index(body=doc, index=index_name,doc_type="data")   #doc_type 鏄换鎰忚祴鍊肩殑

def append_bulk(host,port,df,ES_index,fromnrow):  #add
    es=Elasticsearch(host=host, port=port, http_auth=(es_user, es_pwd))
    df=df.tail(fromnrow)
    tmp = df.to_json(orient = "records",date_format="iso")
    # Load each record into json format before bulk
    
    df_json= json.loads(tmp)
    print (df_json[0])
    # Create an index into elastic search. If we need to create a mapping, we ought to do it here
 
    # Bulk index    
    actions = []
 
    for row in df_json:
        actions.append({
  
           "_op_type": "index",
           "_index": ES_index,
           "_type": "data",
           "_source": row
        })
        if len(actions) == 100:
            helpers.bulk(es, actions, index=ES_index, doc_type="data", request_timeout=60)
            actions = []

def update_bulk(host,port,df,ES_index):  #add
    es=Elasticsearch(host=host, port=port, http_auth=(es_user, es_pwd))   
    tmp = df.to_json(orient = "records",date_format="iso")
    # Load each record into json format before bulk
    
    df_json= json.loads(tmp)
    print (df_json[0])
    # Create an index into elastic search. If we need to create a mapping, we ought to do it here
    index_name = ES_index
    # Bulk index
    es.indices.delete(index=index_name, ignore=[400, 404])

    actions = []

   
    for row in df_json:
        actions.append({
  
           "_op_type": "index",
           "_index": ES_index,
           "_type": "data",
           "_source": row
        })
        if len(actions) == 100:
            helpers.bulk(es, actions, index=ES_index, doc_type="data", request_timeout=60)
            actions = []

def writer_bulk(host,port,df,ES_index):  #add
    es=Elasticsearch(host=host, port=port, http_auth=(es_user, es_pwd))
    #if fromnrow>0:
    #   df=df[nrecord:]  #after n rows
    tmp = df.to_json(orient = "records",date_format="iso")   
    df_json = json.loads(tmp)
    actions = [] 
    for row in df_json:
        actions.append({
  
           "_op_type": "index",
           "_index": ES_index,
           "_type": "data",
           "_source": row
        })
        if len(actions) == 100:
            helpers.bulk(es, actions, index=ES_index, doc_type="data", request_timeout=60)
            actions = []

    helpers.bulk(es, actions, index=ES_index, doc_type="data", request_timeout=60)  #write the rest


if __name__=="__main__":
                  # ---parameter
    #d={'datetime':["2018-01-19 00:57:55.431","2018-02-19 00:57:55.431"],'value':[3,4]}
    #df=pd.DataFrame(data=d)
    #df["time"]=[x[11:] for x in df["datetime"]]
          #df["col1"]=pd.to_datetime(df["col1"])
          #df["col1"]=df["col1"].astype(str)
           #df["col3"]=[datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S') for x in df["col1"]]                     #df["col1"].values.astype('datetime64[D]')

<<<<<<< HEAD
    host="98.11.56.20"
    port="10092"
   
    es=Elasticsearch(host=host, port=port, http_auth=(es_user, es_pwd))   
  
    index_name ="alert_csp_monlog_*"
=======
    host="192.168.0.21"
    port="9900"   
    es=Elasticsearch(host=host, port=port)   
  
    index_name ="test"
>>>>>>> 7e3b1e3ee09933f4bb6f4589c59c00bed89271d0
    # Bulk index
    es.indices.delete(index=index_name, ignore=[400, 404])


