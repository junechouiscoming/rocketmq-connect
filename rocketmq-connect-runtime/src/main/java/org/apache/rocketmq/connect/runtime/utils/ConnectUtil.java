/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.utils;

import java.util.*;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateTaskStrategy;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class ConnectUtil {

    public static String createGroupName(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("-");
        sb.append(RemotingUtil.getLocalAddress()).append("-");
        sb.append(UtilAll.getPid()).append("-");
        sb.append(System.nanoTime());
        return sb.toString().replace(".", "-");
    }

    public static String createGroupName(String prefix, String postfix) {
        return new StringBuilder().append(prefix).append("-").append(postfix).toString();
    }

    public static String createInstance(String servers) {
        String[] serversArray = servers.split(";");
        List<String> serversList = new ArrayList<String>();
        for (String server : serversArray) {
            if (!serversList.contains(server)) {
                serversList.add(server);
            }
        }
        Collections.sort(serversList);
        return String.valueOf(serversList.toString().hashCode());
    }

    public static String createUniqInstance(String prefix) {
        return new StringBuffer(prefix).append("-").append(UUID.randomUUID().toString()).toString();
    }

    public static AllocateTaskStrategy initAllocateConnAndTaskStrategy(ConnectConfig connectConfig) {
        try {
            return (AllocateTaskStrategy) Thread.currentThread().getContextClassLoader().loadClass(connectConfig.getAllocTaskStrategy()).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static DefaultMQProducer initDefaultMQProducer(ConnectConfig connectConfig) {
        RPCHook rpcHook = null;
        if (connectConfig.getAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        producer.setInstanceName(createUniqInstance(connectConfig.getNamesrvAddr()));
        producer.setProducerGroup(connectConfig.getRmqProducerGroup());
        producer.setSendMsgTimeout(connectConfig.getOperationTimeout());
        producer.setMaxMessageSize(RuntimeConfigDefine.MAX_MESSAGE_SIZE);
        producer.setLanguage(LanguageCode.JAVA);
        return producer;
    }

    /**
     * 默认consumerGrp为connector-consumer-group,也就是sinkTask的消费组名称，这个其实是个工具方法，所以不同情况下的消费组名称会重新set的
     * @param connectConfig
     * @return
     */
    public static DefaultMQPullConsumer initDefaultMQPullConsumer(ConnectConfig connectConfig) {
        RPCHook rpcHook = null;
        if (connectConfig.getAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(rpcHook);
        consumer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        consumer.setInstanceName(createUniqInstance(connectConfig.getNamesrvAddr()));
        consumer.setConsumerGroup(connectConfig.getRmqConsumerGroup());
        consumer.setMaxReconsumeTimes(connectConfig.getRmqMaxRedeliveryTimes());
        consumer.setBrokerSuspendMaxTimeMillis(connectConfig.getBrokerSuspendMaxTimeMillis());
        consumer.setConsumerPullTimeoutMillis((long) connectConfig.getRmqMessageConsumeTimeout());
        consumer.setLanguage(LanguageCode.JAVA);
        return consumer;
    }

    public static DefaultMQPushConsumer initDefaultMQPushConsumer(ConnectConfig connectConfig) {
        RPCHook rpcHook = null;
        if (connectConfig.getAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        //没有指定要消费哪个集群，理论上如果一个集群下没有consumer的订阅未创建的话其实也无法消费,而下面createSubGroup创建订阅则考虑了集群
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(rpcHook);
        consumer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        consumer.setInstanceName(createUniqInstance(connectConfig.getNamesrvAddr()));
        consumer.setConsumerGroup(createGroupName(connectConfig.getRmqConsumerGroup()));
        consumer.setMaxReconsumeTimes(connectConfig.getRmqMaxRedeliveryTimes());
        consumer.setConsumeTimeout((long) connectConfig.getRmqMessageConsumeTimeout());
        consumer.setConsumeThreadMin(connectConfig.getRmqMinConsumeThreadNums());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setLanguage(LanguageCode.JAVA);
        return consumer;
    }

    public static DefaultMQAdminExt startMQAdminTool(ConnectConfig connectConfig) throws MQClientException {
        RPCHook rpcHook = null;
        if (connectConfig.getAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setNamesrvAddr(connectConfig.getNamesrvAddr());
        defaultMQAdminExt.setAdminExtGroup(connectConfig.getAdminExtGroup());
        defaultMQAdminExt.setInstanceName(ConnectUtil.createUniqInstance(connectConfig.getNamesrvAddr()));
        defaultMQAdminExt.start();
        return defaultMQAdminExt;
    }

    public static String createSubGroup(ConnectConfig connectConfig, String subGroup) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(connectConfig);
            SubscriptionGroupConfig initConfig = new SubscriptionGroupConfig();
            initConfig.setGroupName(subGroup);

            //根据配置的集群名称，找到集群内所有的master的地址，然后每个master都调用createAndUpdateSubscriptionGroupConfig创建consumer
            //因为一个nameSrv下可能有多个集群
            //可是发送和消费时候又不区分集群...所以应该是这里改掉,不考虑集群,给nameSrv集群上的所有master都添加订阅消费组
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            HashMap<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> masterSet = new HashSet<>();
            if (clusterAddrTable!=null) {
                HashMap<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();

                for (Map.Entry<String/*clusterName*/, Set<String/*brokerName*/>> entry : clusterAddrTable.entrySet()) {
                    Set<String> brokerNameSet = entry.getValue();
                    for (String brokerName : brokerNameSet) {
                        if (brokerAddrTable!=null) {
                            BrokerData brokerData = brokerAddrTable.get(brokerName);
                            if (brokerData!=null) {
                                HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
                                if (brokerAddrs!=null && brokerAddrs.size()>0) {
                                    String addr = brokerAddrs.get(0L);
                                    masterSet.add(addr);
                                }
                            }
                        }
                    }


                }
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, initConfig);
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("create subGroup: " + subGroup + " failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return subGroup;
    }
}
