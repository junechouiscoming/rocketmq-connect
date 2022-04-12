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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.primitives.Longs;
import io.netty.util.internal.ConcurrentSet;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.converter.ByteMapConverter;
import org.apache.rocketmq.connect.runtime.converter.JsonConverter;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetManagementServiceImpl implements PositionManagementService {
    private static Logger logger = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Current offset info in store.
     * 这个store也会在rocketMQ上进行同步和merge的
     */
    private KeyValueStore<ByteBuffer, ByteBuffer> offsetStore;


    /**
     * The updated partition of the task in the current instance.
     */
    private Set<ByteBuffer> needSyncPartition;

    /**
     * Synchronize data with other workers.
     */
    private DataSynchronizer<String, Map<ByteBuffer, ByteBuffer>> dataSynchronizer;

    private final String offsetManagePrefix = "OffsetManage";

    /**
     * Listeners.
     */
    private Set<PositionUpdateListener> offsetUpdateListener;

    public OffsetManagementServiceImpl(ConnectConfig connectConfig) {

        this.offsetStore = new FileBaseKeyValueStore<ByteBuffer,ByteBuffer>(
                FilePathConfigUtil.getOffsetPath(connectConfig.getStorePathRootDir()),
                new TypeReference<Map<ByteBuffer,ByteBuffer>>() {}){
            @Override
            protected Map<ByteBuffer,ByteBuffer> decode(String jsonString) {
                Map<String, String> map = JSON.parseObject(jsonString, new TypeReference<ConcurrentHashMap<String, String>>() {});
                Map<ByteBuffer, ByteBuffer> returnMap = new ConcurrentHashMap<>();
                map.entrySet().stream().forEach(e->{
                    if (e.getKey()!=null && e.getValue()!=null) {
                        returnMap.put(ByteBuffer.wrap(e.getKey().getBytes(StandardCharsets.UTF_8)),ByteBuffer.wrap(e.getValue().getBytes(StandardCharsets.UTF_8)));
                    }
                });
                return returnMap;
            }

            @Override
            protected String encode(Map<ByteBuffer,ByteBuffer> data) {
                Map<String, String> offsetMap = new ConcurrentHashMap<>();
                data.entrySet().stream().forEach(e->{
                    if (e.getValue().hasArray() && e.getValue().array() != null) {
                        String s = new String(e.getValue().array());
                        if(s!=null && s.length()>0){
                            offsetMap.put(new String(e.getKey().array()),new String(e.getValue().array()));
                        }
                    }
                });
                return JSON.toJSONString(offsetMap, SerializerFeature.PrettyFormat);
            }
        };
        this.dataSynchronizer = new BrokerBasedLog(connectConfig,
            connectConfig.getOffsetStoreTopic(),
            ConnectUtil.createGroupName(offsetManagePrefix, connectConfig.getWorkerId()),
            new OffsetChangeCallback(),
            new JsonConverter(),
            new ByteMapConverter());
        this.offsetUpdateListener = new HashSet<>();
        this.needSyncPartition = new ConcurrentSet<>();
    }

    @Override
    public void start() {

        offsetStore.load();
        dataSynchronizer.start();
        sendOnlineOffsetInfo();
    }

    @Override
    public void stop() {

        sendNeedSynchronizeOffset();
        offsetStore.persist();
        dataSynchronizer.stop();
    }

    @Override
    public void persist() {
        offsetStore.persist();
    }

    @Override
    public void synchronize() {

        sendNeedSynchronizeOffset();
    }

    @Override
    public Map<ByteBuffer, ByteBuffer> getPositionTable() {

        return offsetStore.getKVMap();
    }

    @Override
    public ByteBuffer getPosition(ByteBuffer partition) {

        return offsetStore.get(partition);
    }

    @Override
    public void putPosition(Map<ByteBuffer, ByteBuffer> offsets) {

        offsetStore.putAll(offsets);
        needSyncPartition.addAll(offsets.keySet());
    }

    @Override
    public void putPosition(ByteBuffer partition, ByteBuffer position) {

        offsetStore.put(partition, position);
        needSyncPartition.add(partition);
    }

    @Override
    public void removePosition(List<ByteBuffer> offsets) {

        if (null == offsets) {
            return;
        }
        for (ByteBuffer offset : offsets) {
            needSyncPartition.remove(offset);
            offsetStore.remove(offset);
        }
    }

    @Override
    public void registerListener(PositionUpdateListener listener) {

        this.offsetUpdateListener.add(listener);
    }

    private void sendOnlineOffsetInfo() {

        dataSynchronizer.send(OffsetChangeEnum.ONLINE_KEY.name(), offsetStore.getKVMap());
    }


    private void sendNeedSynchronizeOffset() {

        Set<ByteBuffer> needSyncPartitionTmp = needSyncPartition;
        needSyncPartition = new ConcurrentSet<>();
        Map<ByteBuffer, ByteBuffer> needSyncOffset = offsetStore.getKVMap().entrySet().stream()
            .filter(entry -> needSyncPartitionTmp.contains(entry.getKey()))
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

        dataSynchronizer.send(OffsetChangeEnum.OFFSET_CHANG_KEY.name(), needSyncOffset);
    }

    private void sendSynchronizeOffset() {

        dataSynchronizer.send(OffsetChangeEnum.OFFSET_CHANG_KEY.name(), offsetStore.getKVMap());
    }

    private class OffsetChangeCallback implements DataSynchronizerCallback<String, Map<ByteBuffer, ByteBuffer>> {

        @Override
        public void onCompletion(Throwable error, String key, Map<ByteBuffer, ByteBuffer> result) {

            boolean changed = false;
            switch (OffsetChangeEnum.valueOf(key)) {
                case ONLINE_KEY:
                    changed = true;
                    sendSynchronizeOffset();
                    break;
                case OFFSET_CHANG_KEY:
                    changed = mergeOffsetInfo(result);
                    break;
                default:
                    break;
            }
            if (changed) {
                triggerListener();
            }

        }
    }

    private void triggerListener() {
        for (PositionUpdateListener offsetUpdateListener : offsetUpdateListener) {
            offsetUpdateListener.onPositionUpdate();
        }
    }

    /**
     * Merge new received offset info with local store.
     *
     * @param result
     * @return
     */
    private synchronized boolean mergeOffsetInfo(Map<ByteBuffer, ByteBuffer> result) {

        boolean changed = false;
        if (null == result || 0 == result.size()) {
            return changed;
        }

        for (Map.Entry<ByteBuffer, ByteBuffer> newEntry : result.entrySet()) {
            boolean find = false;
            for (Map.Entry<ByteBuffer, ByteBuffer> existedEntry : offsetStore.getKVMap().entrySet()) {
                if (newEntry.getKey().equals(existedEntry.getKey())) {
                    find = true;
                    final String newOffsetStr = new String(newEntry.getValue().array().length==0?"0".getBytes(StandardCharsets.UTF_8):newEntry.getValue().array());
                    final String existedOffsetStr = new String(existedEntry.getValue().array().length==0?"0".getBytes(StandardCharsets.UTF_8):existedEntry.getValue().array());

                    if(Long.parseLong(newOffsetStr) > Long.parseLong(existedOffsetStr)){
                        changed = true;
                        existedEntry.setValue(newEntry.getValue());
                    }else{
                        logger.info("receive a lower offset and will ignore it");
                    }
                    break;
                }
            }
            if (!find) {
                changed = true;
                offsetStore.put(newEntry.getKey(), newEntry.getValue());
            }
        }
        return changed;
    }

    private enum OffsetChangeEnum {

        /**
         * Insert or update offset info.
         */
        OFFSET_CHANG_KEY,

        /**
         * A worker online.
         */
        ONLINE_KEY
    }
}

