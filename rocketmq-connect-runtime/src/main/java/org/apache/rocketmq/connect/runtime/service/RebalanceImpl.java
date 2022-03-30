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

import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.ConnectController;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateConnAndTaskStrategy;
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
    private AllocateConnAndTaskStrategy allocateConnAndTaskStrategy;

    private final ConnectController connectController;

    public RebalanceImpl(Worker worker, ConfigManagementService configManagementService,
        ClusterManagementService clusterManagementService, AllocateConnAndTaskStrategy strategy, ConnectController connectController) {

        this.worker = worker;
        this.configManagementService = configManagementService;
        this.clusterManagementService = clusterManagementService;
        this.allocateConnAndTaskStrategy = strategy;
        this.connectController = connectController;
    }

    public void checkClusterStoreTopic() {
        if (!clusterManagementService.hasClusterStoreTopic()) {
            log.error("cluster store topic not exist, apply first please!");
        }
    }

    /**
     * Distribute connectors and tasks according to the {@link RebalanceImpl#allocateConnAndTaskStrategy}.
     * 如果一个消费组下面有多个consumer,则可以动态负载均衡分配任务执行，各个节点运行相同的负载均衡算法
     */
    public void doRebalance() {
        //mz 下面这个getAllAliveWorkers 其实就是 findConsumerIdList(connectConfig.getClusterStoreTopic(), connectConfig.getConnectClusterId()) 其实就是同一topic+group下面的消费者有哪些
        List<String> curAliveWorkers = clusterManagementService.getAllAliveWorkers();
        log.info("Current Alive workers : " + curAliveWorkers.size());
        Map<String, ConnectKeyValue> curConnectorConfigs = configManagementService.getConnectorConfigs();
        log.info("Current ConnectorConfigs : " + curConnectorConfigs);
        Map<String, List<ConnectKeyValue>> curTaskConfigs = configManagementService.getTaskConfigs();
        log.info("Current TaskConfigs : " + curTaskConfigs);
        //负载均衡结果
        ConnAndTaskConfigs allocateResult = allocateConnAndTaskStrategy.allocate(curAliveWorkers, clusterManagementService.getCurrentWorker(), curConnectorConfigs, curTaskConfigs);
        log.info("Allocated connector:{}", allocateResult.getConnectorConfigs());
        log.info("Allocated task:{}", allocateResult.getTaskConfigs());
        updateProcessConfigsInRebalance(allocateResult);
    }

    /**
     * Start all the connectors and tasks allocated to current process.
     *
     * @param allocateResult
     */
    private void updateProcessConfigsInRebalance(ConnAndTaskConfigs allocateResult) {

        try {
            //worker自己其实早早就已经运行起来了
            worker.startConnectors(allocateResult.getConnectorConfigs(), connectController);
            worker.startTasks(allocateResult.getTaskConfigs());
        } catch (Exception e) {
            log.error("RebalanceImpl#updateProcessConfigsInRebalance start connector or task failed", e);
        }
    }

}
