# 算法开发规范  

## 基本结构  

算法包括数据流处理器、AI/机器学习算法、告警规则实现等，用Python/JS语言编写。  
实现上算法是独立的脚本，本身可以独立执行，但运行时参数、输入输出等需要遵循一些规范。prophet-server提供算法脚本框架，可以帮助开发人员快速实现符合规范的脚本。  

---  
## 运行时参数  

对于Python脚本，算法框架采用argparse进行运行时参数处理。  
对于JS脚本，采用Commander.js进行参数处理。  
prophet-server内部参数有：  
```
--input-type <INPUT_PORT_TYPE>
--input-name <INPUT_PORT_NAME>
--output-type <OUTPUT_PORT_TYPE>
--output-name <OUTPUT_PORT_NAME>
```

以上参数用于指定IO端口。其他自定义的运行时参数脚本自行访问即可。  
prophet-server定义了一个特殊的内部参数：  
```
--test
```

该参数用于测试脚本，脚本可以检查该参数，并实现测试逻辑，快速退出并输出返回码。  
测试逻辑默认需要在10s内退出，否则会被prophet-server强行停止，并认为测试成功（只要进程成功启动就算测试成功）。  
不实现测试的脚本10s后会被强制退出，并认为测试成功。  

---  
## 日志输出  

算法框架提供logger对象方便脚本日志输出，脚本只需直接调用logger对象的方法即可输出日志。  
**使用logger输出的日志会被prophet-server展示在任务历史状态中。**  
脚本输出到STDOUT的信息，会被当作文本日志保存在`~/.pm2/logs/app_name-out.log`和`~/.pm2/logs/app_name-error.err`文件中，方便开发人员进行调试。  
logger API:  
```
tbf
```

---  
## 文件组织与命名规则  

算法以目录形式存在，目录的命名以`algorithm-data-type`的形式，比如`lstm-tploader-prophet`。  
算法的主入口脚本的命名以`algorithm-data-type.ext`的形式，比如`lstm-tploader-prophet.py`。  
type的取值：  
```
trainer - AI训练器
prophet - AI预测器
alert - 告警规则
```
告警如果没有特殊算法，algorithm命名就指定为`rule`。  
如果算法不针对某种数据，data的命名可以指定为`any`。  

---  
## 外部引用  

对于非自动安装的外部引用，可以在算法目录中建立lib/子目录保存。 
自动安装的外部引用，可以在算法目录中进行安装，如Node会安装算法目录的node_modules中。  
