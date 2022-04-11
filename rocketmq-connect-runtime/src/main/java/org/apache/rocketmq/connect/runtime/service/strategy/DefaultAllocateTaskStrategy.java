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

package org.apache.rocketmq.connect.runtime.service.strategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.rocketmq.connect.runtime.common.AllocateResultConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectorAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default allocate strategy, distribute connectors and tasks averagely.
 */
public class DefaultAllocateTaskStrategy implements AllocateTaskStrategy {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    
    @Override
    public AllocateResultConfigs allocate(List<String> allWorker, String curWorker,
                                          Map<String, List<ConnectKeyValue>> taskConfigs) {
        AllocateResultConfigs allocateResult = new AllocateResultConfigs();
        if (null == allWorker || 0 == allWorker.size() || taskConfigs == null) {
            return allocateResult;
        }

        List<String> sortedWorkers = new ArrayList<>(allWorker);
        Collections.sort(sortedWorkers);
        Map<String, List<ConnectKeyValue>> sortedTaskConfigs = new TreeMap<>(taskConfigs);

        int index = 0;
        //分析一下是否会出现task和connector没分到一起的情况 以及task和connector的真实关系到底要如何改进
        //目前分析下来是肯定会发生没分到一起的情况,另外就是connector准备抛弃掉,connector唯一作用就是生成Task和对外暴露接口,内部负载均衡不再处理connector了
        for (String connectorName : sortedTaskConfigs.keySet()) {
            for (ConnectKeyValue keyValue : sortedTaskConfigs.get(connectorName)) {
                String allocatedWorker = sortedWorkers.get(index % sortedWorkers.size());
                index++;
                if (!curWorker.equals(allocatedWorker)) {
                    continue;
                }
                if (null == allocateResult.getTaskConfigs().get(connectorName)) {
                    allocateResult.getTaskConfigs().put(connectorName, new ArrayList<>());
                }
                allocateResult.getTaskConfigs().get(connectorName).add(keyValue);
            }
        }
        return allocateResult;
    }
}
