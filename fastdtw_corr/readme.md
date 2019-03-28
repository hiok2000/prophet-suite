fastdtw-通过时序数据来得到之间的相关性
=====

fastdtw经过改动之后输出与dtw基本相同,且运算速度很快.



输入数据
----

fastdtw输入数据为两组时序数据




example
---


s1=[1, 2, 3, 2, 1]
s2=[1, 0, 3]

distance , pattern = fastdtw(s1,s2,dist=euclidean_norm)

使用fastdtw得到之间的距离distance=0.2125

路径pattern为[(0, 0), (1, 1), (2, 2), (3, 2), (4, 2)]


distance,C,D1,pattern=dtw(s1,s2,dist=euclidean_norm)

其中distance表示距离,pattern表示经过的路径

经过dtw计算得到距离distance=0.2125,路径path为(array([0, 1, 2, 3, 4]), array([0, 1, 2, 2, 2]))



more
----

![](https://github.com/Orientsoft/prophet-suite/blob/master/fastdtw_corr/2019-03-22%2015-32-39%20%E7%9A%84%E5%B1%8F%E5%B9%95%E6%88%AA%E5%9B%BE.png)



计算对应意外点时间
===


主要通过fastdtw计算出来所有标准化之后的联系矩阵,并且存储起来.

当某个属性出现意外点的时候,根据存储的联系矩阵选择出来与该属性关联性最高的前五个属性.

通过fastdtw计算出来路径取值,将得到的路径记录下来并且转换成dataframe形式,并且从其中找出与意外点相对应的其他属性的点,最终返回时间点以及时间点对应的属性值.

example
---

①首先根据时序数据计算相关系数矩阵,通过notebook中的scale函数计算并返回标准化之后的矩阵.并且保存

timeseries_scale=scale(timeseries.iloc[:,:5])

其中timeseries表示我们的时序数据,scale函数返回的是经过标准化的时序数据


②通过fastdtw计算并且返回由标准化之后的矩阵得到的相关系数矩阵,并且保存系数矩阵

corr_data=corrdata_fun(timeseries_scale)

corrdata_fun()函数接受一个标准化之后的时序数据,一个时序数据相关系数表


③调用influence函数并且返回对应相关性最高的属性的时间点

all_influ=influence("万能打印_count","2018-06-01 23:09",corr_data,timeseries_scale)

influence()函数接受四个参数,分别是属性名称"万能打印",发生异常的事件"2018-06-01 23:09",属性之间的相关系数矩阵corr_data,以及标准化之后的时序数据timeseries_scale

返回一组dataframe,相对应的其他属性可能发生异常的时间.


