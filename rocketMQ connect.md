![image.png](https://cdn.nlark.com/yuque/0/2022/png/23038086/1648622549606-4922b417-8564-430f-bc12-e3b50f1f4668.png#clientId=ue34ac3c8-c6de-4&crop=0&crop=0&crop=1&crop=1&from=paste&id=u59a3af33&margin=%5Bobject%20Object%5D&name=image.png&originHeight=527&originWidth=725&originalType=url&ratio=1&rotation=0&showTitle=false&size=39403&status=done&style=none&taskId=u9fb6089a-2c51-4c67-83b9-ac1075c03d3&title=)

# 服务发现和负载均衡
![image.png](https://cdn.nlark.com/yuque/0/2022/png/23038086/1648622565256-d30c5062-511e-47fd-ad33-4c669d9a70f7.png#clientId=ue34ac3c8-c6de-4&crop=0&crop=0&crop=1&crop=1&from=paste&id=ue78c8772&margin=%5Bobject%20Object%5D&name=image.png&originHeight=685&originWidth=885&originalType=url&ratio=1&rotation=0&showTitle=false&size=74143&status=done&style=none&taskId=uc417bda0-c41a-4e73-ab6b-54c9eacd6ec&title=)
# connector-runtime 
需要新建4个Topic以及2个consumerGroup ，topic的queue.num读写均为1
## Topic

1. **connector-cluster-topic**
1. **connector-config-topic**
1. **connector-offset-topic**
1. **connector-position-topic**
## ConsumerGroup 手工无需参与,自动创建

1. **connector-consumer-group**
1. **connector-cluster-group**



**修改/resource配置文件 connect.conf**
```properties
workerId=CONNECTOR_WORKER_1  #本节点名称,各个worker之间必须唯一
storePathRootDir=E:/rocketmq-connect/storeRoot #本地持久化文件路径,保存偏移量和config信息

# Http port for user to access REST API
httpPort=8082 #rest地址

# Rocketmq namesrvAddr
namesrvAddr=127.0.0.1:9876  #sink端会从改nameSrv指定的rocketMQ集群上拉取消息,同时也会作为节点信息交换的媒介

# RocketMQ acl #账号需要有admin的权限
aclEnable=false
accessKey=rocketmq
secretKey=12345678

autoCreateGroupEnable=true #自动调用admin工具创建consumerGroup.如手工创建可设置为false

#sink和source插件的目录,注意每个sink和source都是独立的类加载器，所以sink和souce打包时如果有依赖需要用shade打成fat包
pluginPaths=C:/Users/11722/.m2/repository/org/apache/rocketmq/rocketmq-connect-rocketmq/0.0.1-SNAPSHOT,C:/Users/11722/.m2/repository/org/apache/rocketmq/rocketmq-connect-kafka/0.0.1-SNAPSHOT
```
# rocketmq-connect-kafka

**发送kafka消息到rocketMQ集群**

在启动runtime之后，通过发送http消息到任意一台runtime机器，携带connector和task的参数，启动connector即可,task会自动分配轮询分配到其他runtime节点<br />kafka的consumerID固定为 : connector-consumer-group

**

- **tasks.num**: 启动的task数目,各task的配置完全相同，所以如果数量超过messageQueue的话也没用
- **kafka.topics**: rocketMQ的topic列表,多个topic通过逗号“,”隔开。拉取消息后发到相同topic名称的kafka集群中
- **kafka.bootstrap.server**: kafka的broker地址

**新增Connector示例见**<br />GET [http://127.0.0.1:8082/connectors/create/kafkaSource?config={"tasks.num":"4","kafka.topics":"kafkaconnect","kafka.bootstrap.server":"127.0.0.1:9092"}](http://127.0.0.1:8082/connectors/create/kafkaSource?config={"tasks.num":"4","kafka.topics":"kafkaconnect","kafka.bootstrap.server":"127.0.0.1:9092"})

其中 127.0.0.1:8082 为runtime的任意一台机器,kafkaSource为connector的名称，connector名称需要唯一。

# rocketmq-connect-rocketMQ

**发送rocketMQ消息到kafka集群**

在启动runtime之后，通过发送http消息到任意一台runtime机器，携带connector和task的参数，启动connector即可,task会自动分配轮询分配到其他runtime节点

**参数说明**

- **tasks.num**: 启动的task数目,各task的配置完全相同，所以如果数量超过messageQueue的话也没用
- **topicNames**: rocketMQ的topic列表,多个topic通过逗号“,”隔开。拉取消息后发到相同topic名称的kafka集群中
- **kafka.bootstrap.server**: kafka的broker地址

**新增Connector示例**<br />GET [http://127.0.0.1:8082/connectors/create/kafkaSink?config={"topicNames":"kafkaconnect","tasks.num":"2","kafka.bootstrap.server":"127.0.0.1:9092"}](http://127.0.0.1:8082/connectors/create/kafkaSink?config=%7B%22topicNames%22:%22kafkaconnect%22,%22tasks.num%22:%222%22,%22kafka.bootstrap.server%22:%22127.0.0.1:9092%22%7D)

其中 127.0.0.1:8082 为runtime的任意一台机器,kafkaSink 为connector的名称，connector名称需要唯一
# 运维命令
//查看全部connector config以及对应的task config<br />app.get("/getConnectorTask", this::getConnectorTask);<br />//查看全部connector config<br />app.get("/getConnectors", this::getAllConnectors);<br />//查看单个connector config<br />app.get("/getConnectors/:connectorName", this::getConnectors);<br />//查看指定connector的配置信息<br />app.get("/getConnectorTask/:connectorName", this::handleQueryConnectorConfig);<br />//查看指定connector状态,启用或者禁用或者删除状态<br />app.get("/getConnectorsStatus/:connectorName", this::handleQueryConnectorStatus);

//查看集群信息<br />app.get("/getClusterInfo", this::getClusterInfo);<br />//查看分配给自己的任务的状态<br />app.get("/getAllocatedTask", this::getAllocatedTask);<br />//插件重新加载<br />app.get("/plugin/reload", this::reloadPlugins);

//新增connector<br />app.get("/connectors/create/:connectorName", this::handleCreateConnector);<br />//修改connector,可只传入需要修改的参数部分,内部直接覆盖原配置<br />app.get("/connectors/update/:connectorName", this::handleUpdateConnector);<br />//调整task数量,taskNum为目标任务数量<br />app.get("/connectors/taskNum/:connectorName/:taskNum", this::handleTaskNum);

//启用<br />app.get("/connectors/all/enable", this::handleEnableAllConnector);<br />//暂时禁用,配置文件读出来也不会去执行,同时停止该connector对应的Task. task的stop是通过把connector禁用，这样maintainTaskStat时候就不会分配该task，达到维护task的目的<br />app.get("/connectors/all/disable", this::handleDisableAllConnector);<br />//删除<br />app.get("/connectors/all/remove", this::handleRemoveAllConnector);

//启用<br />app.get("/connectors/single/:connectorName/enable", this::handleEnableConnector);<br />//暂时禁用,配置文件读出来也不会去执行,同时停止该connector对应的Task<br />app.get("/connectors/single/:connectorName/disable", this::handleDisableConnector);<br />//删除<br />app.get("/connectors/single/:connectorName/remove", this::handleRemoveConnector);

源代码查看某个被引入的类来源是哪里，不然经常报class找不到<br />org.reflections.scanners.AbstractScanner#put
