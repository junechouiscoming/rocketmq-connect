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

package org.apache.rocketmq.connect.kafka.connector;

import io.openmessaging.connector.api.data.SourceDataEntry;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaSourceTaskTest {

    @Test
    public void pollTest() throws Exception {
        List<TopicPartition> topicPartitionList = new CopyOnWriteArrayList<>();

        topicPartitionList.add(new TopicPartition("s", 0));
        topicPartitionList.add(new TopicPartition("s", 1));
        topicPartitionList.add(new TopicPartition("s", 2));
        topicPartitionList.add(new TopicPartition("d", 1));
        topicPartitionList.add(new TopicPartition("d", 3));
        topicPartitionList.add(new TopicPartition("q", 2));


        List<TopicPartition> topicPartitions = new ArrayList<>();

        topicPartitions.add(new TopicPartition("s",2));
        topicPartitions.add(new TopicPartition("d",1));
        topicPartitions.add(new TopicPartition("s",2));

        for (TopicPartition partition : topicPartitions) {
            topicPartitions.remove(partition);
        }

        System.out.println(topicPartitionList);
    }
}
