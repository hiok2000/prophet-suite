
# coding: utf-8

# In[1]:


import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from dtw import dtw
from dtw import accelerated_dtw
from scipy.spatial.distance import cosine
from fastdtw import fastdtw
import itertools
from  numpy.linalg import norm
from sklearn.preprocessing import StandardScaler
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


# 查看路径
dir = "/home/liu/桌面"  #根据自己目录修改
print(os.listdir(dir))
timeseries = pd.read_csv("/home/liu/files/files/repo/dtw/df_all.csv",index_col=0)
timeseries = timeseries.fillna(0)
euclidean_norm = lambda x, y: np.abs(x - y)/(np.abs(x)+np.abs(y))    
# euclidean_norm = lambda x, y: np.abs(x - y)


# In[3]:


#计算相应的关系系数矩阵

corr_data=pd.DataFrame(columns=timeseries.columns[:5],index=timeseries.columns[:5])
#传入时序数据并且将其标准化之后返回
def scale(timeseries):
    scale=StandardScaler()
    for i in timeseries.columns:
        timeseries[i]=scale.fit_transform(timeseries[i].values.reshape(-1,1))
    return timeseries

#将标准化之后的矩阵计算相关系数矩阵
def corrdata_fun(timeseries_scale):
    for i in timeseries_scale.columns:
        for j in timeseries_scale.columns:
            distance,path=fastdtw(timeseries_scale[i],timeseries_scale[j],dist=euclidean_norm)
            distance=distance/(2*len(timeseries_scale))
            corr_data.loc[i,j]=distance
            corr_data.loc[j,i]=distance
    return corr_data
timeseries_scale=scale(timeseries.iloc[:,:5])
corr_data=corrdata_fun(timeseries_scale)
print(corr_data)


# In[4]:


get_ipython().run_cell_magic('time', '', 'def time_num(time):\n    hour=time.split(" ")[1].split(":")[0]\n    minu=time.split(" ")[1].split(":")[1]\n    num=int(minu)+(int(hour)*60)\n    return num\n\ndef num_time(num):\n    hour=num//60\n    min=num - hour * 60\n    return str(hour)+":"+str(min)\n\n#corr_data表示相关性矩阵\n#timeseries_scale表示经过标准化之后的矩阵\n#type表示属性种类\ndef order(type_,num,corr_data,timeseries_scale):\n    order=list(corr_data.loc[type_].sort_values().index) \n    result=pd.DataFrame(index=order,columns=["Time"])                   #result:最终的时间dataframe\n    for i in order:\n        x = timeseries_scale[type_].values.reshape(-1,1)\n        y = timeseries_scale[i].values.reshape(-1,1)\n        p1,b1 = fastdtw(x,y,dist=euclidean_norm)\n        b1=pd.DataFrame(b1)                          #b1是得到的路径DataFrame\n        the_result=b1[1][num]                        #the_result表示得到的时间点\n        result.loc[i,"Time"]=the_result\n    return result\n\n\ndef influence(type,time,corr_data,timeseries_scale):\n    num = time_num(time)\n    all_influ=order(type,num,corr_data,timeseries_scale)\n    all_influ=all_influ.drop(index=type)\n    return all_influ[\'Time\'].apply(num_time)\n\n\nall_influ=influence("万能打印_count","2018-06-01 23:09",corr_data,timeseries_scale)')


# In[6]:


all_influ

