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

package org.apache.rocketmq.connect.runtime.config;

import com.google.common.collect.Lists;
import io.openmessaging.connector.api.data.DataEntry;

import java.util.*;

/**
 * Define keys for connector and task configs.
 */
public class RuntimeConfigDefine {

    /**
     * 逐条log打印转发的消息
     */
    public static final String LOG_MSG_DETAILS = "logMsgDetail";

    /**
     * The full class name of a specific connector implements.
     */
    public static final String CONNECTOR_CLASS = "connector.class";

    /**
     * sink和source的task需要用这个反射，会从Connector.taskClass()然后put到task的config中
     */
    public static final String TASK_CLASS = "sys_task-class";
    /**
     * n/m 形式，如3/5表示一共5个任务，当前是第3个任务
     */
    public static final String TASK_ID = "sys_task-id";
    /**
     * 每个task从其生命周期都唯一绑定一个uuid
     */
    public static final String TASK_UID = "sys-uid";

    /**
     * Task的并发度
     */
    public static String TASK_NUM = "tasks.num";

    //direct
    /**
     * direct task会put这个key
     */
    public static final String TASK_TYPE = "sys_task-type";
    public static final String CONNECTOR_DIRECT_ENABLE = "connector-direct-enable";
    /**
     * direct task会从connector config中拿这2个key
     */
    public static final String SOURCE_TASK_CLASS = "source-task-class";
    public static final String SINK_TASK_CLASS = "sink-task-class";

    /**
     * Last updated time of the configuration.
     */
    public static final String UPDATE_TIMESTAMP = "sys_update-timestamp";

    /**
     * Whether the current config is deleted,it will delete in the config-file
     */
    public static final String CONFIG_STATUS = "config-status";
    public static final int CONFIG_STATUS_ENABLE = 1;
    public static final int CONFIG_STATUS_DISABLE = 2;
    public static final int CONFIG_STATUS_REMOVE = 3;
    public static final List<Integer> CONFIG_ENABLE_DISABLE_LST = Collections.unmodifiableList(Arrays.asList(RuntimeConfigDefine.CONFIG_STATUS_ENABLE, RuntimeConfigDefine.CONFIG_STATUS_DISABLE));
    public static final List<Integer> CONFIG_ENABLE_LST = Collections.unmodifiableList(Arrays.asList(RuntimeConfigDefine.CONFIG_STATUS_ENABLE));
    public static final List<Integer> CONFIG_DISABLE_LST = Collections.unmodifiableList(Arrays.asList(RuntimeConfigDefine.CONFIG_STATUS_DISABLE));
    public static final List<Integer> CONFIG_REMOVE_LST = Collections.unmodifiableList(Arrays.asList(RuntimeConfigDefine.CONFIG_STATUS_REMOVE));

    /**
     * task在负载均衡时候需要进行对比,判断是否发生变动进而判断是否需要停止Task
     */
    public static final List<String> TASK_UNCOMPARED_KEY_LST = Collections.unmodifiableList(Arrays.asList(TASK_ID,UPDATE_TIMESTAMP));

    /**
     * The full class name of record converter. Which is used to parse {@link DataEntry} to/from byte[].
     */
    public static final String SOURCE_RECORD_CONVERTER = "source-record-converter";

    public static final String NAMESRV_ADDR = "namesrv-addr";

    public static final String RMQ_PRODUCER_GROUP = "rmq-producer-group";

    public static final String RMQ_CONSUMER_GROUP = "rmq-consumer-group";

    public static final String OPERATION_TIMEOUT = "operation-timeout";

    public static final String HASH_FUNC = "consistentHashFunc";

    public static final String LEATSET_NODE = "latest_node";

    public static final String CONNECT_SHARDINGKEY = "connect-shardingkey";

    public static final String CONNECT_TOPICNAME = "connect-topicname";

    public static final String CONNECT_SOURCE_PARTITION = "connect-source-partition";

    public static final String CONNECT_SOURCE_POSITION = "connect-source-position";

    public static final String CONNECT_ENTRYTYPE = "connect-entrytype";

    public static final String CONNECT_TIMESTAMP = "connect-timestamp";

    public static final String CONNECT_SCHEMA = "connect-schema";

    /**
     * The required key for all configurations.
     */
    public static final Set<String> REQUEST_CONFIG = new HashSet<String>() {
        {
            add(CONNECTOR_CLASS);
        }
    };

    /**
     * Maximum allowed message size in bytes, the default vaule is 4M.
     */
    public static final int MAX_MESSAGE_SIZE = Integer.parseInt(System.getProperty("rocketmq.runtime.max.message.size", "4194304"));

}
