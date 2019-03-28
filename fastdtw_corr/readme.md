fastdtw-通过时序数据来得到之间的相关性
=====

输入数据
----

fastdtw输入数据与dtw无异,都是两组时序数据,数据之间长度可以不相等,只不过fastdtw计算速度更快.
经过修改后的fastdtw计算出来的数值和dtw基本无异.

example
---

s1=[1, 2, 3, 2, 1]
s2=[1, 0, 3]
distance , pattern = fastdtw(s1,s2,dist=euclidean_norm)

使用fastdtw得到之间的距离distance=0.2125, 路径pattern为[(0, 0), (1, 1), (2, 2), (3, 2), (4, 2)]

distance,C,D1,pattern=dtw(s1,s2,dist=euclidean_norm)
其中distance表示距离,pattern表示经过的路径

经过dtw计算得到距离distance=0.2125,路径path为(array([0, 1, 2, 3, 4]), array([0, 1, 2, 2, 2]))

more
----

![](https://github.com/Orientsoft/prophet-suite/blob/master/fastdtw_corr/2019-03-22%2015-32-39%20%E7%9A%84%E5%B1%8F%E5%B9%95%E6%88%AA%E5%9B%BE.png)
