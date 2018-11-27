#!/usr/bin/python5

from sys import exit
from math import sqrt
import numpy as np
from numpy import array
from collections import namedtuple
from scipy.optimize import fmin_l_bfgs_b
import pandas as pd
from itertools import chain
#import matplotlib.pylab as plt
from PyAstronomy import pyasl
#def running_median_insort
from collections import deque
from bisect import insort, bisect_left
from itertools import islice
from os.path import basename
import  pickle
import datetime
import math
import json
from datetime import timedelta

from elasticsearch import Elasticsearch
#import my defined functions
#import read
import writer
import clean
import dshw
#df:datetime,cleanvalue

def simple_threshold_model(df,direction,name,output_start,upper_bound=0,lower_bound=0) :   
    
    max_anom=0
    if direction=="both":
        max_anom=max(df["cleanvalue"]-upper_bound,lower_bound-df["cleanvalue"] )+1
    elif direction=="positive":
        max_anom=max(df["cleanvalue"]-upper_bound)+1
    elif direction=="negative":
        max_anom=max(lower_bound-df["cleanvalue"] )+1
    
    if max_anom<=1:
        #print ("max_anom:",max_anom)
        #print ("There are no anomaly point")
        #print ("----------You could   definie a smaller cf  confidence interval  or definie a greater range of period and try  again---------")
        df["anom_index"]=1
        df["serverity"]=0
        df["level"]="NORMAL"
    if max_anom>1:
        #print (" detecting  the data............ ")
        if direction=="both":
            df["anom_index"]=round(df["cleanvalue"]-upper_bound,2)+1
            df.loc[df["cleanvalue"]<lower_bound,"anom_index"]=round(lower_bound-df.loc[df["cleanvalue"]<lower_bound,"cleanvalue"] ,2)+1
            df.loc[(df["cleanvalue"]<=upper_bound) & (df["cleanvalue"]>=lower_bound),"anom_index"]=1
            df.loc[df["cleanvalue"]<=0,"anom_index"]=6
        if direction=="positive":
            df["anom_index"]=round(df["cleanvalue"]-upper_bound,2)+1
            df.loc[df["cleanvalue"]<=upper_bound,"anom_index"]=1
        if direction=="negative":
            df["anom_index"]=round(lower_bound-df["cleanvalue"] ,2)+1
            df.loc[df["cleanvalue"]>=lower_bound,"anom_index"]=1
            df.loc[df["cleanvalue"]<=0,"anom_index"]=10
        #df.loc[df["cleanvalue"]==lower_bound or df["cleanvalue"]==upper_bound,"anom_index"]=1
        df.loc[df["cleanvalue"]==lower_bound,"anom_index"]=1
        df.loc[df["cleanvalue"]==upper_bound,"anom_index"]=1

        df["serverity"]=[abs(y) for y in (round(math.log(x,max_anom)*0.95,4)*100 for x in df["anom_index"])]
        df.loc[df["serverity"]>95,"serverity"]=95
        df["level"]="WARNING" #
        df.loc[df["serverity"]==0,"level"]="NORMAL"
        df.loc[df["serverity"]>50,"level"]="ERROR"
    df["name"]=name
    df["timespan"]=60000  #做可选项比较好
        #df["info"]="value:"+df["value"].astype(str)+",  anom_index:"+df["anom_index"].astype(str)
    info=df.filter(items=["value","anom_index","upper_bound","lower_bound"])
    info=info.to_json(orient="records",date_format="iso",lines=True)
    tempArr = info.split("\n")
    result=[]
    for temp in tempArr:
        result.append(json.loads(temp))
    #print (result[0],type(result[0]))
    df["info"]=result
    #df["info"]=json.loads(info)  # info.split("\n"))  
    
    df["createdAt"]=pd.to_datetime(df["datetime"])                              #pd.to_datetime(df["time"])    #online:datetime.datetime.now()
    df["updatedAt"]=datetime.datetime.today().date().strftime('%Y-%m-%d')
    output=df.filter(items=["name","serverity","level","timespan","info","createdAt","updatedAt"])
    start=datetime.datetime.utcfromtimestamp(output_start/1000).strftime("%Y-%m-%d %H:%M:%S")
    #print ("before:",max(df["createdAt"]))
    output=output[output['createdAt']>=start]
    #outliers=output.nlargest(10,"serverity")
    #outliers=outliers.sort_values(by="serverity",ascending=False)
    #print ("after:",max(df["createdAt"]))
    #print (outliers.head(10))
    #output=output.sort_values(by="createdAt")
    return output

  

#direction=both,positive,negative
def simple_normal_model(df,cf,direction,name,output_start,minimum=0) :   #df：the  data with fields(time,value),cf:confidence interval ,window
    #minmum>0
    mean=round(df["cleanvalue"].mean(),2)
    std=round(df["cleanvalue"].std(),2)
    upper_bound=mean+cf*std
    lower_bound=max(mean-cf*std,minimum)
    
    max_anom = 0
    if direction == "both" and upper_bound>0:
        max_anom = max(df["cleanvalue"]/upper_bound,lower_bound/df["cleanvalue"] if min(df["cleanvalue"])>0  else 6)
    elif direction == "positive" and upper_bound>0 :
        max_anom = max(df["cleanvalue"]/upper_bound)
    elif direction == "negative":
        max_anom = max(lower_bound/df["cleanvalue"] if min(df["cleanvalue"])>0  else 6)

    if max_anom<=1:
        #print ("max_anom:",max_anom)
        #print ("There are no anomaly point")
        #print ("----------You could   definie a smaller cf  confidence interval  or definie a greater range of period and try  again---------")
        df["anom_index"]=1
        df["serverity"]=0
        df["level"]="NORMAL"
    if max_anom>1:
        #print (" detecting  the data............ ")
        if direction=="both":
            df["anom_index"]=round(df["cleanvalue"]/upper_bound,2)
            df.loc[df["cleanvalue"]<lower_bound,"anom_index"]=round(lower_bound/df.loc[df["cleanvalue"]<lower_bound,"cleanvalue"] if df.loc[df["cleanvalue"]<lower_bound,"cleanvalue"]>0  else 6,2)
            df.loc[(df["cleanvalue"]<=upper_bound) & (df["cleanvalue"]>=lower_bound),"anom_index"]=1
            df.loc[df["cleanvalue"]<=0,"anom_index"]=6
        if direction=="positive":
            df["anom_index"]=round(df["cleanvalue"]/upper_bound,2)
            df.loc[df["cleanvalue"]<=upper_bound,"anom_index"]=1
        if direction=="negative":
            df["anom_index"]=round(lower_bound/df["cleanvalue"] if df["cleanvalue"]>0  else 10,2)
            df.loc[df["cleanvalue"]>=lower_bound,"anom_index"]=1
            df.loc[df["cleanvalue"]<=0,"anom_index"]=10
        #df.loc[df["cleanvalue"]==lower_bound or df["cleanvalue"]==upper_bound,"anom_index"]=1
        df.loc[df["cleanvalue"]==lower_bound,"anom_index"]=1
        df.loc[df["cleanvalue"]==upper_bound,"anom_index"]=1
            
        df["serverity"]=[abs(y) for y in (round(math.log(x,max_anom)*0.95,4)*100 for x in df["anom_index"])]
        df.loc[df["serverity"]>95,"serverity"]=95
        df["level"]="WARNING" #
        df.loc[df["serverity"]==0,"level"]="NORMAL"
        df.loc[df["serverity"]>50,"level"]="ERROR"
    df["name"]=name
    df["timespan"]=60000  #做可选项比较好
        #df["info"]="value:"+df["value"].astype(str)+",  anom_index:"+df["anom_index"].astype(str)
    info=df.filter(items=["value","anom_index","upper_bound","lower_bound"])
    info=info.to_json(orient="records",date_format="iso",lines=True)
    tempArr = info.split("\n")
    result=[]
    for temp in tempArr:
        result.append(json.loads(temp))
    #print (result[0],type(result[0]))
    df["info"]=result
    #df["info"]=json.loads(info)  # info.split("\n"))  

    df["createdAt"]=pd.to_datetime(df["datetime"])                              #pd.to_datetime(df["time"])    #online:datetime.datetime.now()
    df["updatedAt"]=datetime.datetime.today().date().strftime('%Y-%m-%d')
    output=df.filter(items=["name","serverity","level","timespan","info","createdAt","updatedAt"])
    #outliers=output.nlargest(10,"serverity")
    #outliers=outliers.sort_values(by="serverity",ascending=False)
    
    #print (outliers.head(10))
    #output=output.sort_values(by="createdAt")
    start=datetime.datetime.utcfromtimestamp(output_start/1000).strftime("%Y-%m-%d %H:%M:%S")
    output=output[output['createdAt']>=start]
    return output

   
def single_normal_model(df,cf,direction,name,output_start,window=30,minimum=0) :   #df：the  data with fields(time,value),cf:confidence interval ,window
    #minmum>0
    #df["cleanvalue"] = np.array(clean.running_median_insort(df["cleanvalue"], 5))# update
    rm_med=np.array(clean.running_median_insort(df["cleanvalue"], window))  #window for slipping
    df["time"]=[x[11:] for x in df["datetime"]]
    rm_med_mean=round(df.groupby(df['time'])['cleanvalue'].mean(),2)
    
    baselines=pd.DataFrame(rm_med_mean)
    baselines['time']=baselines.index
    baselines=baselines.filter(items=["time","cleanvalue"])
    baselines.columns=["time","mean"] #beacause there are only the data of one date,the mean is egal to the value.the upper_bound   and lower_bound  have no sense
    rm_med=baselines["mean"]
    std=rm_med[~np.isnan(rm_med)].std()

    baselines["upper_bound"]=[round(x) for x in baselines["mean"]+cf*std]
    baselines["lower_bound"]=[round(x) for x in baselines["mean"]-cf*std]
    baselines.loc[baselines["lower_bound"]<0,"lower_bound"]=minimum   
    baselines=baselines.filter(items=["time","mean","upper_bound","lower_bound"] )

    #print ("----------detecting the data------")

    df=pd.merge(df,baselines)   
    df=df.filter(items=["datetime","time","value","cleanvalue","mean","upper_bound","lower_bound"])
    df.to_csv("/home/voyager/windows/lz-deploy/test_df2.csv")
    print ("write ok")
    if direction=="both":
        max_anom=max(max(df.loc[df["upper_bound"]>0 ,"cleanvalue"]/df.loc[df["upper_bound"]>0 ,"upper_bound"]) ,max(df.loc[df["cleanvalue"]>0,"lower_bound"]/df.loc[df["cleanvalue"]>0,"cleanvalue"]) if min(df["cleanvalue"])>0 else 6  )  
    elif direction=="positive":
        max_anom=max(df.loc[df["upper_bound"]>0 ,"cleanvalue"]/df.loc[df["upper_bound"]>0 ,"upper_bound"])
    elif direction=="negative":
        max_anom=max(df["lower_bound"]/df["cleanvalue"]) if min(df["cleanvalue"])>0 else 6
    if max_anom<=1:
        #print ("max_anom:",max_anom)
        #print ("There are no anomaly point")
        #print ("----------You could   definie a smaller cf  confidence interval  or definie a greater range of period and try  again---------")
        df["anom_index"]=1
        df["serverity"]=0
        df["level"]="NORMAL"
    if max_anom>1:
        #print (" detecting  the data............ ")
        if direction=="both":
         
            df["anom_index"]=round(df["cleanvalue"]/df["upper_bound"],2)
            df.loc[df["cleanvalue"]<df["lower_bound"] ,"anom_index"]=round(df.loc[df["cleanvalue"]<df["lower_bound"] ,"lower_bound"]/df.loc[df["cleanvalue"]<df["lower_bound"] ,"cleanvalue"] ,2) #if  df.loc[df["cleanvalue"]<df["lower_bound"] ,"cleanvalue"]>0 else 6
            df.loc[(df["cleanvalue"]<=df["upper_bound"]) & (df["cleanvalue"]>=df["lower_bound"]),"anom_index"]=1
            df.loc[df["cleanvalue"]<=0,"anom_index"]=6
        if direction=="positive":
            df["anom_index"]=round(df["cleanvalue"]/df["upper_bound"],2)
            df.loc[df["cleanvalue"]<=df["upper_bound"],"anom_index"]=1
        if direction=="negative":
            df.loc[df["cleanvalue"]>0,"anom_index"]=round(df.loc[df["cleanvalue"]>0,"lower_bound"]/df.loc[df["cleanvalue"]>0,"cleanvalue"] ,2)
            df.loc[df["cleanvalue"]>=df["lower_bound"],"anom_index"]=1
            df.loc[df["cleanvalue"]<=0,"anom_index"]=10
        #df.loc[df["cleanvalue"]==df["lower_bound"] or df["cleanvalue"]==df["upper_bound"] ,"anom_index"]=1
        df.loc[df["cleanvalue"]==df["lower_bound"],"anom_index"]=1
        df.loc[df["cleanvalue"]==df["upper_bound"],"anom_index"]=1
        
        df["serverity"]=[abs(y) for y in (round(math.log(x,max_anom)*0.95,4)*100 for x in df["anom_index"])]
        df.loc[df["serverity"]>95,"serverity"]=95
        df["level"]="WARNING" #
        df.loc[df["serverity"]==0,"level"]="NORMAL"
        df.loc[df["serverity"]>50,"level"]="ERROR"

    
    
    df["name"]=name
    df["timespan"]=60000  #做可选项比较好
    #df["info"]="value:"+df["value"].astype(str)+",  anom_index:"+df["anom_index"].astype(str)
    info=df.filter(items=["value","anom_index","upper_bound","lower_bound"])
    
    info=info.to_json(orient="records",date_format="iso",lines=True)
    #print ("--------------------------info-------------------")
    #print (info)
    tempArr = info.split("\n")
    result=[]
    for temp in tempArr:
        result.append(json.loads(temp))
    #print (result[0],type(result[0]))
    df["info"]=result
    #df["info"]=json.loads(info)  # info.split("\n"))

    df["createdAt"]=pd.to_datetime(df["datetime"])                              #pd.to_datetime(df["time"])    #online:datetime.datetime.now()
    df["updatedAt"]=datetime.datetime.today().date().strftime('%Y-%m-%d')
    
    output=df.filter(items=["name","serverity","level","timespan","info","createdAt","updatedAt"])
    start=datetime.datetime.utcfromtimestamp(output_start/1000).strftime("%Y-%m-%d %H:%M:%S")
    output=output[output['createdAt']>=start]
    #outliers=output.nlargest(10,"serverity")
    #outliers=outliers.sort_values(by="serverity",ascending=False)
    
    #print (outliers.head(10))
    #output=output.sort_values(by="createdAt")
    return output

#-------------------------------------------3-dshw------------------------------------------------------------------------

<<<<<<< HEAD
def double_dshw_model(df,cf,direction,name,output_start,window=30,minimum=0,m=1440,m2=1440*7) : #m 周期一，m2周期二
    forecast=10
=======
def double_dshw_model(df,cf,direction,name,output_start,forecast_start,window=30,minimum=0,m=1440,m2=1440*7) : #m 周期一，m2周期二
    
    forecast=1440   #10
>>>>>>> 7e3b1e3ee09933f4bb6f4589c59c00bed89271d0
    #x=list(df["cleanvalue"])
    
    x=list(np.array(clean.running_median_insort(df["cleanvalue"], window)))
    
    fit=dshw.double_seasonal(x, m=m, m2=m2,forecast=forecast, alpha = None, beta = None, gamma = None,delta=None,autocorrelation=None, initial_values_optimization=[0.1, 0.1, 0.2, 0.2, 0.9], optimization_type="MSE")
    #params=fit[1]
    #print ("alpha, beta, gamma, delta, autocorrelation:")
    #print (params)
    

    estimate=fit[2][1:]  #历史预测值
    estimate=[round(x) for x in estimate]
    estimate=pd.DataFrame(estimate,columns=["estimate"])
    df=pd.merge(df,estimate,left_index=True,right_index=True,how="outer")  
   #df=pd.concat([df,estimate],axis=1)
    df["residual"]=df["cleanvalue"]-df["estimate"]  
    print (" dataframe :datetime"      ,df.head(10))
    rm_med=np.array(clean.running_median_insort(df["residual"], window))
   
    m=rm_med[~np.isnan(rm_med)].mean()
    st=rm_med[~np.isnan(rm_med)].std()
   
    df["upper_bound"]=[round(x) for x in df["estimate"]+cf*st]
    df["lower_bound"]=[round(x) for x in df["estimate"]-cf*st]
    df.loc[df["lower_bound"]<0,"lower_bound"]=minimum
    #df["description"]="NORMAL"
    #df.loc[df["cleanvalue"]<df["lower_bound"],"description"]="too lower"
    #df.loc[(df["cleanvalue"]>df["upper_bound"]),"description"]="too higher"
    
    #df.to_csv("/home/voyager/windows/lz-deploy/test_df.csv")
    #print ("csv ok")
    df=df.filter(items=["datetime","value","cleanvalue","upper_bound","lower_bound"])
    
<<<<<<< HEAD
    
    
=======
>>>>>>> 7e3b1e3ee09933f4bb6f4589c59c00bed89271d0
    if direction=="both":
        max_anom=max(max(df.loc[df["upper_bound"]>0 ,"cleanvalue"]/df.loc[df["upper_bound"]>0 ,"upper_bound"]) ,max(df.loc[df["cleanvalue"]>0,"lower_bound"]/df.loc[df["cleanvalue"]>0,"cleanvalue"]) if min(df["cleanvalue"])>0 else 6  )  
    elif direction=="positive":
        max_anom=max(df.loc[df["upper_bound"]>0 ,"cleanvalue"]/df.loc[df["upper_bound"]>0 ,"upper_bound"])
    elif direction=="negative":
        max_anom=max(df["lower_bound"]/df["cleanvalue"]) if min(df["cleanvalue"])>0 else 6
    if max_anom<=1:
        #print ("max_anom:",max_anom)
        #print ("There are no anomaly point")
        #print ("----------You could   definie a smaller cf  confidence interval  or definie a greater range of period and try  again---------")
        df["anom_index"]=1
        df["serverity"]=0
        df["level"]="NORMAL"
    
    if max_anom>1:
        #print (" detecting  the data............ ")
        if direction=="both":
            df["anom_index"]=1
            df["anom_index"]=round(df.loc[df["upper_bound"]>0 ,"cleanvalue"]/df.loc[df["upper_bound"]>0 ,"upper_bound"],2)
            df.loc[df["cleanvalue"]<df["lower_bound"] ,"anom_index"]=round(df.loc[df["cleanvalue"]<df["lower_bound"] ,"lower_bound"]/df.loc[df["cleanvalue"]<df["lower_bound"] ,"cleanvalue"] ,2) #if  df.loc[df["cleanvalue"]<df["lower_bound"] ,"cleanvalue"]>0 else 6
            df.loc[(df["cleanvalue"]<=df["upper_bound"]) & (df["cleanvalue"]>=df["lower_bound"]),"anom_index"]=1
            df.loc[df["cleanvalue"]<=0,"anom_index"]=6
        if direction=="positive":
            df["anom_index"]=1
            df["anom_index"]=round(df.loc[df["upper_bound"]>0 ,"cleanvalue"]/df.loc[df["upper_bound"]>0 ,"upper_bound"],2)
            df.loc[df["cleanvalue"]<=df["upper_bound"],"anom_index"]=1
        if direction=="negative":
            df.loc[df["cleanvalue"]>0,"anom_index"]=round(df.loc[df["cleanvalue"]>0,"lower_bound"]/df.loc[df["cleanvalue"]>0,"cleanvalue"] ,2)
            df.loc[df["cleanvalue"]>=df["lower_bound"],"anom_index"]=1
            df.loc[df["cleanvalue"]<=0,"anom_index"]=10
        #df.loc[df["cleanvalue"]==df["lower_bound"] or df["cleanvalue"]==df["upper_bound"] ,"anom_index"]=1    
        df.loc[df["cleanvalue"]==df["lower_bound"],"anom_index"]=1
        df.loc[df["cleanvalue"]==df["upper_bound"],"anom_index"]=1
        
        df["serverity"]=[abs(y) for y in (round(math.log(x,max_anom)*0.95,4)*100 for x in df["anom_index"])]
        df.loc[df["serverity"]>95,"serverity"]=95
        df["level"]="WARNING" #
        df.loc[df["serverity"]==0,"level"]="NORMAL"
        df.loc[df["serverity"]>50,"level"]="ERROR"
    
    
    df["name"]=name
    df["timespan"]=60000  #做可选项比较好

    #df["info"]="value:"+df["value"].astype(str)+",  anom_index:"+df["anom_index"].astype(str)+",  description:"+df["description"]
    info=df.filter(items=["value","anom_index","upper_bound","lower_bound"])
    
    info=info.to_json(orient="records",date_format="iso",lines=True)
    #print ("--------------------------info-------------------")
    #print (info)
    tempArr = info.split("\n")
    result=[]
    for temp in tempArr:
        result.append(json.loads(temp))
    #print (result[0],type(result[0]))
    df["info"]=result
    #df["info"]=json.loads(info)  # info.split("\n"))

    df["createdAt"]=pd.to_datetime(df["datetime"])                              #pd.to_datetime(df["time"])    #online:datetime.datetime.now()
    df["updatedAt"]=datetime.datetime.today().date().strftime('%Y-%m-%d')
    #print (df.head(10))
    output=df.filter(items=["name","serverity","level","timespan","info","createdAt","updatedAt"])
    #outliers=output.nlargest(10,"serverity")
    #outliers=outliers.sort_values(by="serverity",ascending=False)
    #print (outliers.head(10))
    start=datetime.datetime.utcfromtimestamp(output_start/1000).strftime("%Y-%m-%d %H:%M:%S")
    output=output[output['createdAt']>=start]

    #-------  forcast  ----------
    print ("len(fit[0]:         ",len(fit[0]))  
    #print ("fit[0]         :    ",fit[0])
    forecast = fit[0][1:]  #预测一天的值

    forecast_start=datetime.datetime.utcfromtimestamp(forecast_start/1000).strftime("%Y-%m-%d %H:%M:%S")

    #-----whole minute list--
    starttime = pd.to_datetime(max(df['createdAt']),format='%Y-%m-%d %H:%M:%S')+timedelta(seconds=60)
    timelist=[starttime.strftime("%Y-%m-%d %H:%M:%S")]
    i = 0
    while  i<len(forecast):
        starttime += timedelta(seconds=60)
        timelist.append(starttime.strftime("%Y-%m-%d %H:%M:%S"))
        i += 1
    timelist=pd.DataFrame(timelist)
    timelist.columns=["time"]
    forecast = pd.DataFrame(forecast)
    forecast=pd.merge(timelist,forecast)
    forecast.columns=["createdAt","forecast_value"]
    add_forecast=forecast[forecast['createdAt']>=forecast_start] 
    #print (type(cleandata))
    #print (cleandata.head(10))
    return output,add_forecast
