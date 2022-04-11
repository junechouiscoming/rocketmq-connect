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

package org.apache.rocketmq.connect.runtime.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.rocketmq.connect.runtime.ConnectController;
import org.apache.rocketmq.connect.runtime.common.AllocateResultConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateTaskStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distribute connectors and tasks in current cluster.
 */
public class RebalanceImpl {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Worker to schedule connectors and tasks in current process.
     */
    private final Worker worker;

    /**
     * ConfigManagementService to access current config info.
     */
    private final ConfigManagementService configManagementService;

    /**
     * ClusterManagementService to access current cluster info.
     */
    private final ClusterManagementService clusterManagementService;

    /**
     * Strategy to allocate connectors and tasks.
     */
    private AllocateTaskStrategy allocateTaskStrategy;

    private final ConnectController connectController;

    public RebalanceImpl(Worker worker, ConfigManagementService configManagementService,
                         ClusterManagementService clusterManagementService, AllocateTaskStrategy strategy, ConnectController connectController) {

        this.worker = worker;
        this.configManagementService = configManagementService;
        this.clusterManagementService = clusterManagementService;
        this.allocateTaskStrategy = strategy;
        this.connectController = connectController;
    }

    public void checkClusterStoreTopic() {
        if (!clusterManagementService.hasClusterStoreTopic()) {
            log.error("cluster store topic not exist, apply first please!");
        }
    }

    /**
     * Distribute connectors and tasks according to the {@link RebalanceImpl#allocateTaskStrategy}.
     * 如果一个消费组下面有多个consumer,则可以动态负载均衡分配任务执行，各个节点运行相同的负载均衡算法
     * 这个会最终一致性，但是系统逐步更新的过程一定会有2个任务同时跑在其他节点上。实际运行中应该不会发生。系统正常而言一直都是一致的。
     * 可能会发生一个任务分给自己,然后刚启动就又被停止按照最新的数据分给别人了,抖动可能比较大,抖动处理好应该还OK
     */
    public void doRebalance() {
        //mz 下面这个getAllAliveWorkers 其实就是 findConsumerIdList(connectConfig.getClusterStoreTopic(), connectConfig.getConnectClusterId()) 其实就是同一topic+group下面的消费者有哪些
        List<String> curAliveWorkers = clusterManagementService.getAllAliveWorkers();
        if (curAliveWorkers==null && clusterManagementService.hasClusterStoreTopic()) {
            throw new RuntimeException(String.format("clusterStoreTopic may not exist in rocketMQ,please create it first,the topic default name is '%s'", "connector-cluster-topic"));
        }
        log.debug("Current Alive workers : " + curAliveWorkers.size());
        Map<String, ConnectKeyValue> allConnectors;
        Map<String, List<ConnectKeyValue>> allTasks;
        synchronized (configManagementService){
            allConnectors = configManagementService.getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_LST);
            allTasks = configManagementService.getTaskConfigs(RuntimeConfigDefine.CONFIG_ENABLE_LST);
        }

        AllocateResultConfigs allocateResult = allocateTaskStrategy.allocate(curAliveWorkers, clusterManagementService.getCurrentWorker(), allTasks);
        updateProcessConfigsInRebalance(allocateResult);

        log.debug("doRebalance Current ConnectorConfigs : " + JSON.toJSONString(allConnectors,SerializerFeature.PrettyFormat));
        log.debug("doRebalance Current TaskConfigs : " + JSON.toJSONString(allTasks,SerializerFeature.PrettyFormat));
        log.debug("doRebalance Allocated task:"+ JSON.toJSONString(allocateResult.getTaskConfigs(), SerializerFeature.PrettyFormat));
    }

    /**
     * Start all the connectors and tasks allocated to current process.
     *
     * @param allocateResult
     */
    private void updateProcessConfigsInRebalance(AllocateResultConfigs allocateResult) {

        try {
            //worker自己其实早早就已经运行起来了
            Map<String, List<ConnectKeyValue>> oldTasks = worker.getTasks();
            Map<String, List<ConnectKeyValue>> inRemove = Worker.difference(oldTasks, allocateResult.getTaskConfigs(),1);
            Map<String, List<ConnectKeyValue>> inAdd = Worker.difference(oldTasks, allocateResult.getTaskConfigs(),2);
            Map<String, Map> printMap = new HashMap<>();
            printMap.put("Remove", inRemove);
            printMap.put("Add", inAdd);
            log.info("now allocated tasks:\n"+ JSON.toJSONString(allocateResult.getTaskConfigs(),SerializerFeature.PrettyFormat));

            log.info("after rebalanced ,in this worker the allocated tasks changes :\n"+ JSON.toJSONString(printMap,SerializerFeature.PrettyFormat));
            worker.setTasks(allocateResult.getTaskConfigs());
        } catch (Exception e) {
            log.error("RebalanceImpl#updateProcessConfigsInRebalance start connector or task failed", e);
        }
    }

}
