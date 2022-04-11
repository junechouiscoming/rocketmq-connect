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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.rocketmq.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.rocketmq.config.ConfigDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 配置一个DirectTask,从rocketMQ消费send到kafka
 */
public class RocketMQSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(RocketMQSinkConnector.class);

    private KeyValue connectConfig;

    public RocketMQSinkConnector() {
        super();
    }

    @Override
    public String verifyAndSetConfig(KeyValue config) {
        for (String key : config.keySet()) {
            log.info("connector verifyAndSetConfig: key: {}, value: {}", key, config.getString(key));
        }

        //TODO 校验一下必要的key有没有缺失
        for (String requestKey : ConfigDefine.REQUEST_CONFIG) {
            if (!config.containsKey(requestKey)) {
                return "Request Config key: " + requestKey;
            }
        }
        this.connectConfig = config;
        return "";
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        //connector启停没啥意义,关键是Task
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return RocketMQSinkTask.class;
    }

    @Override
    public List<KeyValue> taskConfigs() {
        if (connectConfig == null) {
            return new ArrayList<KeyValue>();
        }

        log.info("rocketmq Connector taskConfigs enter");
        List<KeyValue> configs = new ArrayList<>();
        int task_num = connectConfig.getInt(ConfigDefine.TASK_NUM);
        log.info("rocketmq Connector taskConfigs: task_num:" + task_num);
        for (int i = 0; i < task_num; ++i) {
            KeyValue config = new DefaultKeyValue();
            //kafka
            config.put(ConfigDefine.BOOTSTRAP_SERVER, connectConfig.getString(ConfigDefine.BOOTSTRAP_SERVER));
            //common
            config.put(ConfigDefine.CONNECTOR_CLASS, "org.apache.rocketmq.connect.rocketmq.connector.RocketMQSinkConnector");

            //从哪个topic消费数据
            config.put(ConfigDefine.TOPIC_NAMES, connectConfig.getString(ConfigDefine.TOPIC_NAMES));
            //config.put(ConfigDefine.OFFSET_COMMIT_INTERVAL_MS_CONFIG, connectConfig.getString(ConfigDefine.OFFSET_COMMIT_INTERVAL_MS_CONFIG));

            configs.add(config);
        }
        return configs;
    }
}
