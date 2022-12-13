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

package org.apache.rocketmq.connect.rocketmq.config;

import java.util.*;

public class ConfigDefine {
    //kafka配置
    public static String BOOTSTRAP_SERVER = "kafka.bootstrap.server";

    //消息从rocketMQ的哪个topic拉出来,逗号分割
    public static final String ROCKETMQ_TOPIC = "rocketmq.topic";
    public static final String KAFKA_TOPIC = "kafka.topic";

    public static String CONNECTOR_CLASS = "connector.class";
    public static String TASK_NUM = "tasks.num";

    public static final Set<String> REQUEST_CONFIG = new HashSet<String>(){
        {
            add(KAFKA_TOPIC);
            add(ROCKETMQ_TOPIC);
            add(BOOTSTRAP_SERVER);
        }
    };
}
