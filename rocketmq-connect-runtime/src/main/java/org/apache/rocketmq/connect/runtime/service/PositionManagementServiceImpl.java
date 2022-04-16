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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.converter.ByteBufferConverter;
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

public class PositionManagementServiceImpl implements PositionManagementService {
    private static Logger logger = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Current position info in store.
     */
    private KeyValueStore<ByteBuffer, ByteBuffer> positionStore;

    /**
     * The updated partition of the task in the current instance.
     * */
    private Set<ByteBuffer> needSyncPartition;

    /**
     * Synchronize data with other workers.
     */
    private DataSynchronizer<String, Map<ByteBuffer, ByteBuffer>> dataSynchronizer;

    /**
     * Listeners.
     */
    private Set<PositionUpdateListener> positionUpdateListener;

    private final String positionManagePrefix = "connector-positionManage";

    public PositionManagementServiceImpl(ConnectConfig connectConfig) {
        this.positionStore = new FileBaseKeyValueStore<ByteBuffer,ByteBuffer>(
                FilePathConfigUtil.getPositionPath(connectConfig.getStorePathRootDir()),
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
                        offsetMap.put(new String(e.getKey().array()),new String(e.getValue().array()));
                    }
                });
                return JSON.toJSONString(offsetMap, SerializerFeature.PrettyFormat);
            }
        };

        this.dataSynchronizer = new BrokerBasedLog(connectConfig,
            connectConfig.getPositionStoreTopic(),
            ConnectUtil.createGroupName(positionManagePrefix, connectConfig.getWorkerId()),
            new PositionChangeCallback(),
            new JsonConverter(),
            new ByteMapConverter());
        this.positionUpdateListener = new HashSet<>();
        this.needSyncPartition = new ConcurrentSet<>();
    }

    @Override
    public void start() {

        positionStore.load();
        dataSynchronizer.start();
        sendOnlinePositionInfo();
    }

    @Override
    public void stop() {

        sendNeedSynchronizePosition();
        positionStore.persist();
        dataSynchronizer.stop();
    }

    @Override
    public void persist() {

        positionStore.persist();
    }

    @Override
    public void synchronize() {

        sendNeedSynchronizePosition();
    }

    @Override
    public Map<ByteBuffer, ByteBuffer> getPositionTable() {

        return positionStore.getKVMap();
    }

    @Override
    public ByteBuffer getPosition(ByteBuffer partition) {

        return positionStore.get(partition);
    }

    @Override
    public void putPosition(Map<ByteBuffer, ByteBuffer> positions) {

        positionStore.putAll(positions);
        needSyncPartition.addAll(positions.keySet());
    }

    @Override
    public void putPosition(ByteBuffer partition, ByteBuffer position) {

        positionStore.put(partition, position);
        needSyncPartition.add(partition);
    }

    @Override
    public void removePosition(List<ByteBuffer> partitions) {

        if (null == partitions) {
            return;
        }

        for (ByteBuffer partition : partitions) {
            needSyncPartition.remove(partition);
            positionStore.remove(partition);
        }
    }

    @Override
    public void registerListener(PositionUpdateListener listener) {

        this.positionUpdateListener.add(listener);
    }

    private void sendOnlinePositionInfo() {

        dataSynchronizer.send(PositionChangeEnum.ONLINE_KEY.name(), positionStore.getKVMap());
    }


    /**
     * positionStore中不是所有的kv都需要同步数据，这里就是只同步needSyncPartition包含的kv
     * 每send一次，这个needSyncPartition就会清空一次，然后下次再put进来新kv的话这个needSyncPartition就又有值了
     * 相当于说needSyncPartition就是上次send之后又新增/变化的数据
     */
    private void sendNeedSynchronizePosition() {

        Set<ByteBuffer> needSyncPartitionTmp = needSyncPartition;
        needSyncPartition = new ConcurrentSet<>();
        Map<ByteBuffer, ByteBuffer> needSyncPosition = positionStore.getKVMap().entrySet().stream()
            .filter(entry -> needSyncPartitionTmp.contains(entry.getKey()))
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

        dataSynchronizer.send(PositionChangeEnum.POSITION_CHANG_KEY.name(), needSyncPosition);
    }

    /**
     * 同步整个positionStore的kv
     */
    private void sendSynchronizePosition() {

        dataSynchronizer.send(PositionChangeEnum.POSITION_CHANG_KEY.name(), positionStore.getKVMap());
    }

    private class PositionChangeCallback implements DataSynchronizerCallback<String, Map<ByteBuffer, ByteBuffer>> {

        @Override
        public void onCompletion(Throwable error, String key, Map<ByteBuffer, ByteBuffer> result) {

            boolean changed = false;
            switch (PositionChangeEnum.valueOf(key)) {
                case ONLINE_KEY:
                    //positionManagementService初次上线
                    changed = true;
                    sendSynchronizePosition();
                    break;
                case POSITION_CHANG_KEY:
                    //offset发生变动需要同步
                    changed = mergePositionInfo(result);
                    break;
                default:
                    break;
            }
            if (changed) {
                //其实没有任何监听器
                triggerListener();
            }
        }
    }

    private void triggerListener() {
        for (PositionUpdateListener positionUpdateListener : positionUpdateListener) {
            positionUpdateListener.onPositionUpdate();
        }
    }

    /**
     * Merge new received position info with local store.
     *
     * 将其他节点的position offset同步到自己的缓存里实现数据同步
     *
     * TODO 为什么merge时候不处理needSyncPartition这个变量？因为这是别人给自己同步数据，自己并没有发生变化不需要send出去nnn
     *
     * @param result
     * @return
     */
    private synchronized boolean mergePositionInfo(Map<ByteBuffer, ByteBuffer> result) {

        boolean changed = false;
        if (null == result || 0 == result.size()) {
            return changed;
        }

        for (Map.Entry<ByteBuffer, ByteBuffer> newEntry : result.entrySet()) {
            boolean find = false;
            for (Map.Entry<ByteBuffer, ByteBuffer> existedEntry : positionStore.getKVMap().entrySet()) {
                if (newEntry.getKey().equals(existedEntry.getKey())) {
                    find = true;
                    final String newOffsetStr = new String(newEntry.getValue().array().length==0?"0".getBytes(StandardCharsets.UTF_8):newEntry.getValue().array());
                    final String existedOffsetStr = new String(existedEntry.getValue().array().length==0?"0".getBytes(StandardCharsets.UTF_8):existedEntry.getValue().array());

                    if(Long.parseLong(newOffsetStr) > Long.parseLong(existedOffsetStr)){
                        changed = true;
                        existedEntry.setValue(newEntry.getValue());
                    }else{
                        logger.info("receive a lower position and will ignore it");
                    }
                    break;
                }
            }
            if (!find) {
                changed = true;
                positionStore.put(newEntry.getKey(), newEntry.getValue());
            }
        }
        return changed;
    }

    private enum PositionChangeEnum {

        /**
         * Insert or update position info.
         */
        POSITION_CHANG_KEY,

        /**
         * A worker online.
         */
        ONLINE_KEY
    }
}

