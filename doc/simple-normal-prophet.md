# 算法帮助  

无周期的正态分布检测算法：根据统计的“中心极限定理”通过足够多的样本逼近正态分布，检测异常

## 适用场景
适用于:

* 没有周期特点的时间序列
* 序列的值为非负数，即大于或等于零

## 常量  

常量保存在算法lib目录下，以consts.py命名。  
目前支持的配置项有：
```
es_user # ES的用户名
es_pwd # ES的密码
```  

## 参数说明

算法参数分两类Operator.py定义的公共参数和算法脚本定义的局部参数。
所有的参数都可在AIdefender的tasks页面的弹出框中设置。
#### Operator公共参数有：  
```
--input_name <INPUT_NAME>            #数据源index名称，在AIdefender的弹出框的I/O port部分设置
--output-name <OUTPUT_NAME>          #输出index名称，在AIdefender的弹出框的I/O port部分设置

--es_host <ES_HOST>                  #ES所在的主机（机器名或IP）
--es_port <ES_PORT>                  #ES所在的PORT

--watchdog_threshold   <WATCHDOG_THRESHOLD>    #看门狗的时限，超出该时限未收到Prophet-Server的心跳，则脚本自动退出
--mongo_host   <MONGO_HOST>           #mongo所在主机（机器名或IP）   
--mongo_port   <MONGO_PORT>           #mongo数据库所在PORT

--loop_interval  <LOOP_INTERVAL>      #检查数据源（是否有更新）的时间间隔,以秒为单位，默认30秒，
--loop_window_minutes <LOOP_WINDOW_MINUTES>    #每次计算时，向前推移的时间窗口大小（单位分钟）

--reader_module <READER_MODULE>       #调用的读取脚本
--reader_function  <READER_FUNCTION>  #调用的读取函数
```

#### 该算法的局部参数有： 

```
--metric_name <METRIC_NAME>           #告警名称，用在AIdefender泳道图中
--cf <CF>                             #置信区间参数
--direction <DIRECTION>               #异常方向
--minimum <MINIMUM>                   #下限，经验值
```


## 调参技巧 

+ --loop_interval：实时检测数据的时间间隔。目前默认值是30秒检查一次是否有新数据，以保证每隔一分钟都运行一次。如果代码运行时间间隔不变，该参数不需要调整。

    \*调参技巧：根据要求的运行时间间隔调参\*

+ --loop_window_minutes：实时计算时利用的历史窗口大小，不同算法赋值不一样。理论上所有算法的窗口越大结果越准确，但考虑到窗口越大时效性越差，该参数的设置优先考虑有效性的情况下，兼顾时效性：

    \*调参技巧：如果结果有效性差，增加窗口长度，如果时效性差，缩短窗口长度。\*

    例如：双周期double-dshw：有两个周期，默认1天和一个星期，窗口必须大于“2倍最大周期”即大于2个星期。窗口大小可设置为1440*28,即4个星期
    单周期single-normal：有1个周期，默认1天，窗口必须大于两倍周期，窗口大小可设置为两天
    简单的正态分布检测法simple-normal：根据统计的“中心极限定理”通过足够多的样本逼近正态分布，检测异常值，样本必须大"500"即窗口大于500，窗口大小可设置为1440分钟（1天）
    阀值simple-threshold:无周期，固定阀值。不需要历史数据，窗口可设置为了0。


+ --direction：异常方向。根据监测指标含义调整。如果仅在该指标偏高时，报异常，方向为正“positive”；如果仅在该指标偏低时，报异常，方向为负“negative”；如果在该指标偏高或偏低时，都报异常，方向为双向“both”。

+ --minimum：最小值。该值为经验值，默认为零，且只有方向为“both”或"negative"才有用。

    \*调参技巧：根据业务和经验\*

+ --cf:置信区间参数。对于所有算法调参方式一样，该参数大于“3”，一般设置3到5”，参数的变化导致置信区间上下限往相反方向平移。

    \*调参技巧：主要考虑结果的有效性，如果正常数据误判为异常数据较多，将该值调大。如果异常数据误判为正常数据，将该值调小。\*




