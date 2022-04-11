**rocketmq-connect-rocketMQ**

**发送rocketMQ消息到kafka集群**

在启动runtime之后，通过发送http消息到任意一台runtime机器，携带connector和task的参数，启动connector即可,task会自动分配轮询分配到其他runtime节点

**参数说明**
- **tasks.num**: 启动的task数目,各task的配置完全相同，所以如果数量超过messageQueue的话也没用
- **topicNames**: rocketMQ的topic列表,多个topic通过逗号“,”隔开。拉取消息后发到相同topic名称的kafka集群中
- **kafka.bootstrap.server**: kafka的broker地址


**新增Connector示例**
GET http://127.0.0.1:8082/connectors/create/kafkaSink?config={"topicNames":"kafkaconnect","tasks.num":"2","kafka.bootstrap.server":"127.0.0.1:9092"}

其中 127.0.0.1:8082 为runtime的任意一台机器,kafkaSink 为connector的名称，connector名称需要唯一

**其他运维命令见Runtime的README.md**