
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






all_influ

