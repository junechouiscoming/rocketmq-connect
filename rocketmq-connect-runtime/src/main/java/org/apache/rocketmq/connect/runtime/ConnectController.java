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

package org.apache.rocketmq.connect.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.rest.RestHandler;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.OffsetManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.RebalanceImpl;
import org.apache.rocketmq.connect.runtime.service.RebalanceService;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateTaskStrategy;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connect controller to access and control all resource in runtime.
 */
public class ConnectController {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private static ConnectController connectController = null;

    /**
     * Configuration of current runtime.
     */
    private final ConnectConfig connectConfig;

    /**
     * All the configurations of current running connectors and tasks in cluster.
     */
    private final ConfigManagementService configManagementService;

    /**
     * Position management of source tasks.
     */
    private final PositionManagementService positionManagementService;

    /**
     * Offset management of sink tasks.
     */
    private final PositionManagementService offsetManagementService;

    /**
     * Manage the online info of the cluster.
     */
    private final ClusterManagementService clusterManagementService;

    /**
     * A worker to schedule all connectors and tasks assigned to current process.
     */
    private final Worker worker;

    /**
     * A REST handler, interacting with user.
     */
    private final RestHandler restHandler;

    /**
     * Assign all connectors and tasks to all alive process in the cluster.
     */
    private final RebalanceImpl rebalanceImpl;

    /**
     * A scheduled task to rebalance all connectors and tasks in the cluster.
     */
    private final RebalanceService rebalanceService;

    /**
     * Thread pool to run schedule task.
     */
    private ScheduledExecutorService scheduledExecutorService;

    private final Plugin plugin;

    public ConnectController(
        ConnectConfig connectConfig) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        connectController = this;

        List<String> pluginPaths = new ArrayList<>(16);
        if (StringUtils.isNotEmpty(connectConfig.getPluginPaths())) {
            String[] strArr = connectConfig.getPluginPaths().split(",");
            for (String path : strArr) {
                if (StringUtils.isNotEmpty(path)) {
                    pluginPaths.add(path);
                }
            }
        }
        plugin = new Plugin(pluginPaths);
        plugin.initPlugin();

        this.connectConfig = connectConfig;
        this.clusterManagementService = new ClusterManagementServiceImpl(connectConfig);
        this.configManagementService = new ConfigManagementServiceImpl(connectConfig, plugin);
        this.positionManagementService = new PositionManagementServiceImpl(connectConfig);
        this.offsetManagementService = new OffsetManagementServiceImpl(connectConfig);
        //创建一个worker
        this.worker = new Worker(connectConfig, positionManagementService, offsetManagementService, plugin);
        AllocateTaskStrategy strategy = ConnectUtil.initAllocateConnAndTaskStrategy(connectConfig);

        //worker传入到负载均衡中,然后会调用updateProcessConfigsInRebalance进而startConnectors和startTasks
        this.rebalanceImpl = new RebalanceImpl(worker, configManagementService, clusterManagementService, strategy, this);
        this.restHandler = new RestHandler(this);
        this.rebalanceService = new RebalanceService(rebalanceImpl, configManagementService, clusterManagementService);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor((Runnable r) -> new Thread(r, "ConnectScheduledThread"));

    }

    public void start() {
        //这里的一堆start其实就是load本地文件
        clusterManagementService.start();
        configManagementService.start();
        positionManagementService.start();
        offsetManagementService.start();
        worker.start();
        rebalanceService.start();

        // 持久化到内存或者磁盘的json文件中
        // TODO 如果一个新的节点上线就立刻开始执行任务，此时它本地肯定是没位移的，然后其他节点的消息还没发送过来位移还没merge，又会导致消息重复,所以新节点拉取消息的初始位移很重要
        // 如果是相同的consumer group，那么这个group需要拉取的queue已经在rocketMQ上进行负载均衡分配好了。然后本地的work进行负载均衡可能会把之前在运行的任务STOP掉
        // 这俩个负载均衡的维度其实不一样，一个是rocketMQ给消费组成员分配queue 一个是本地跑哪些task任务 而一个task会拉哪些queue其实这里并不care？
        // 1个task任务正好对应一个consumer实例，一个consumer实例可能对应多个topic以及多个queue
        // 后者负载均衡的结果会导致这个task的messageQueueOffset中多出一些自己不应该有的queue。这个时候就不应该去提交位移了。所以后者负载均衡发生时候会手动提交位移，然后重置messageQueueOffset中失效的queue
        // Persist configurations of current connectors and tasks.
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                ConnectController.this.configManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist config error.", e);
            }
        }, 1000, this.connectConfig.getConfigPersistInterval(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {

        if (worker != null) {
            worker.stop();
        }

        if (configManagementService != null) {
            configManagementService.stop();
        }

        //stop时候内部会提交位移
        if (positionManagementService != null) {
            positionManagementService.stop();
        }
        //stop时候内部会提交位移
        if (offsetManagementService != null) {
            offsetManagementService.stop();
        }

        //stop后,其他节点也会通过rocketMQ自带的consumerGroup的成员变动触发重平衡
        if (clusterManagementService != null) {
            clusterManagementService.stop();
        }

        this.scheduledExecutorService.shutdown();
        try {
            //wait 5 minutes
            this.scheduledExecutorService.awaitTermination(5*1000*60, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("shutdown scheduledExecutorService error.", e);
        }

        if (rebalanceService != null) {
            rebalanceService.stop();
        }
    }

    public ConnectConfig getConnectConfig() {
        return connectConfig;
    }

    public ConfigManagementService getConfigManagementService() {
        return configManagementService;
    }

    public PositionManagementService getPositionManagementService() {
        return positionManagementService;
    }

    public ClusterManagementService getClusterManagementService() {
        return clusterManagementService;
    }

    public Worker getWorker() {
        return worker;
    }

    public RestHandler getRestHandler() {
        return restHandler;
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    /**
     * 获取connectController实例,这个不会新建connectController
     * @return
     */
    public static ConnectController getInstance(){
        return connectController;
    }
}
