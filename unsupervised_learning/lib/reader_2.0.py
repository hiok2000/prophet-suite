#!/usr/bin/python5


from elasticsearch import Elasticsearch  
from elasticsearch import helpers
import pandas as pd
import time
from datetime import datetime
from consts import *

def query_by_ts(es_host, es_port, data_index, start, end,field):
    resp = helpers.scan(
            client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd)),
            index = data_index,
            preserve_order = True,
            query = {
                "query": {
                    "range": {
                        "datetime": {
                            "gte": start,
                            "lt": end
                    }
                        }
                            }
                                }
                                )
    lines = []
 
    for item in resp:
        line = {
            'datetime': datetime.strptime(item['_source']['datetime'],'%Y-%m-%dT%H:%M:%S.000Z'),
            'value':item['_source']['value'],
            'cleanvalue':item['_source']['cleanvalue']
               }
        lines.append(line)
    data_frame = pd.DataFrame(lines)
    data_frame = data_frame.filter(items=["datetime","value","cleanvalue"])
    data_frame["value"]=[int(x) for x in data_frame["value"]]
    data_frame["cleanvalue"]=[int(x) for x in data_frame["cleanvalue"]]

    data_frame = data_frame.sort_values(by = "datetime",ascending = True)
    
    
    print (data_frame.head(10))
    return data_frame



#don't need the moving function
#--------------------------------------1.average aggregation functions------
#parameters:filed="string", start="2018-03-08T00:00:00",end="2018-03-09T00:00:00"

def average_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lt": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "avg": { "field": field } }}


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    data_frame["value"]=[round(x,2) for x in data_frame["value"]]
    #data_frame.columns=["ts","avg_"+field]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     


#to :< , from :>=
#最近15分钟：start="now-15m",end="now"
#最近10天：start="now-10d/d",end="now/d"
#最近两个月：start="now-2M/M",end="now/M"
def moving_average(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))
    resp = client.search(
        index = data_index,
        body = {
            "query":{
                "range": {
                    "@timestamp": {
                        "from": start,
                        "to": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "avg": { "field": field } }}


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    data_frame["value"]=[round(x,2) for x in data_frame["value"]]
    #data_frame.columns=["ts","avg_"+field]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     


#-------------------------------2.count aggregation functions------

def totalcount_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lt": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "value_count": { "field": field } }}


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    #data_frame.columns=["ts","total_count"]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     

def moving_totalcount(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "from": start,
                        "to": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "value_count": { "field": field } }}
                    }
                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    #data_frame.columns=["ts","total_count"]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     

def failcount_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
			  "bool":{
				      "must":{
                        "range": {
                           "@timestamp": {
                              "gte": start,
                              "lte": end
                                         }
                                }
							},
				  "must_not":{
							"term":{"RspCode":"000000"}
							}
						}
				
					},
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "value_count": { "field": field } }}


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    #data_frame.columns=["ts","failcount"]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     

def moving_failcount(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
			  "bool":{
				      "must":{
                        "range": {
                           "@timestamp": {
                              "gte": start,
                              "lte": end
                                         }
                                }
							},
				  "must_not":{
							"term":{"RspCode":"000000"}
							}
						}
				
					},
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "value_count": { "field": field } }}
                    }
                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    #data_frame.columns=["ts","failcount"]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     
#-----------------------------fail ratio  functions-----------------
def failratio_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lt": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "total_count": { "value_count": { "field": field } },
                        "total_sucess_count": {
                                                "filter": {
                                                        "term": {
                                                                    "RspCode.keyword": "000000"
                                                                }
                                                        },
                                                "aggs": {
                                                    "sucess_count": {"value_count": { "field": field }}
                                                        }
                                             },
                        "fail-percentage": {
                                        "bucket_script": {
                                            "buckets_path": {
                                                "total": "total_count",
                                                "sucess": "total_sucess_count.sucess_count"
                                                            },
                                        "script": "100-params.sucess / params.total * 100"
                                                        }
                                            }


                            }


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        
        fail = item.get('fail-percentage', {'value': 0}) 
        line = {
            'ts': item['key_as_string'],
            'value': round(fail['value'] ,2)     #round(item['fail-percentage']['value'],2)
        }
        lines.append(line)
        
    data_frame = pd.DataFrame(lines)
    #data_frame.columns=["ts","total_count"]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     


    
def gfront_failratio_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lt": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "total_count": { "value_count": { "field": field } },
                        "total_sucess_count": {
                                                "filter": {
                                                        "term": {
                                                                    "RspMsg.keyword": "交易成功"
                                                                }
                                                        },
                                                "aggs": {
                                                    "sucess_count": {"value_count": { "field": field }}
                                                        }
                                             },
                        "fail-percentage": {
                                        "bucket_script": {
                                            "buckets_path": {
                                                "total": "total_count",
                                                "sucess": "total_sucess_count.sucess_count"
                                                            },
                                        "script": "100-params.sucess / params.total * 100"
                                                        }
                                            }


                            }


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        
        fail = item.get('fail-percentage', {'value': 0}) 
        line = {
            'ts': item['key_as_string'],
            'value': round(fail['value'] ,2)     #round(item['fail-percentage']['value'],2)
        }
        lines.append(line)
        
    data_frame = pd.DataFrame(lines)
    #data_frame.columns=["ts","total_count"]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     
#-------netbank fail 
def netbank_failratio_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lt": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "total_count": { "value_count": { "field": field } },
                        "total_sucess_count1": {
                                                "filter": {
                                                        "term": {
                                                                    "RspCode.keyword": "000000"
                                                                }
                                                        },
                                                "aggs": {
                                                    "sucess_count": {"value_count": { "field": field }}
                                                        }
                                             },
                         "total_sucess_count2": {
                                                "filter": {
                                                        "term": {
                                                                    "RetCd.keyword": "000000"
                                                                }
                                                        },
                                                "aggs": {
                                                    "sucess_count": {"value_count": { "field": field }}
                                                        }
                                                },
                        

                        "fail-percentage": {
                                        "bucket_script": {
                                            "buckets_path": {
                                                "total": "total_count",
                                                "sucess1": "total_sucess_count1.sucess_count",
                                                "sucess2": "total_sucess_count2.sucess_count"   
                                                            },
                                        "script": "100-params.sucess1 / params.total * 100-params.sucess2 / params.total * 100"
                                                        }
                                            }


                            }


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        
        fail = item.get('fail-percentage', {'value': 0}) 
        line = {
            'ts': item['key_as_string'],
            'value': round(fail['value'] ,2)     #round(item['fail-percentage']['value'],2)
        }
        lines.append(line)
        
    data_frame = pd.DataFrame(lines)
    #data_frame.columns=["ts","total_count"]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     



def wlqz_failratio_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lt": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "total_count": { "value_count": { "field": field } },
                        "total_sucess_count": {
                                                "filter": {
                                                        "term": {
                                                                    "BizStsCd": "00000000"
                                                                }
                                                        },
                                                "aggs": {
                                                    "sucess_count": {"value_count": { "field": field }}
                                                        }
                                             },
                        "fail-percentage": {
                                        "bucket_script": {
                                            "buckets_path": {
                                                "total": "total_count",
                                                "sucess": "total_sucess_count.sucess_count"
                                                            },
                                        "script": "100-params.sucess / params.total * 100"
                                                        }
                                            }


                            }


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        
        fail = item.get('fail-percentage', {'value': 0}) 
        line = {
            'ts': item['key_as_string'],
            'value': round(fail['value'] ,2)     #round(item['fail-percentage']['value'],2)
        }
        lines.append(line)
        
    data_frame = pd.DataFrame(lines)
    #data_frame.columns=["ts","total_count"]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     


 





















#--------------------------------3.sum  aggregation functions---------------
def sum_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lt": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "sum": { "field": field } }}


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    data_frame["value"]=[round(x,2) for x in data_frame["value"]]
    #data_frame.columns=["ts","sum_"+field]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     

def moving_sum(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "from": start,
                        "to": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "sum": { "field": field } }}
                    }
                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    data_frame["value"]=[round(x,2) for x in data_frame["value"]]
    #data_frame.columns=["ts","sum_"+field]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     

#--------------------------------4.max  aggregation functions---------------
def max_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lt": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "max": { "field": field } }}


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    data_frame["value"]=[round(x,2) for x in data_frame["value"]]
    #data_frame.columns=["ts","max_"+field]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     

def moving_max(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "from": start,
                        "to": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "max": { "field": field } }}
                    }
                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    data_frame["value"]=[round(x,2) for x in data_frame["value"]]
    #data_frame.columns=["ts","max_"+field]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     

#--------------------------------5.std  aggregation functions---------------
def std_by_ts(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start,
                        "lt": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "std_deviation": { "field": field } }}


                    }




                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    data_frame["value"]=[round(x,2) for x in data_frame["value"]]
    #data_frame.columns=["ts","std_"+field]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     

def moving_std(es_host, es_port, data_index, start, end,field):
    client = Elasticsearch(host=es_host, port=es_port, http_auth=(es_user, es_pwd))

    resp = client.search(
        index = data_index,
        body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "from": start,
                        "to": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "result": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "minute", # or "1m"
                        "format":"yyyy-MM-dd HH:mm:ss"
                    },
                    "aggs": {
                        "stat_value": { "std_deviation": { "field": field } }}
                    }
                }
            }
       
    )
    lines = []
    for item in resp['aggregations']['result']['buckets']:
        line = {
            'ts': item['key_as_string'],
            'value':item['stat_value']['value']
        }
        lines.append(line)

    data_frame = pd.DataFrame(lines)
    data_frame["value"]=[round(x,2) for x in data_frame["value"]]
    #data_frame.columns=["ts","std_"+field]
    data_frame.columns=["datetime","value"]
    data_frame["cleanvalue"]=data_frame["value"].interpolate()
    return data_frame     
    
if __name__=="__main__":
    es_host="98.11.56.20"
    es_port="10092"
    start="2018-06-08T00:00:00"  
    end="2018-07-04T00:00:00"  
    data_index="wy-netbank-log-*"
    field="@timestamp"
    df=netbank_failratio_by_ts(es_host, es_port, data_index, start, end,field)
    print (df.head(10))
