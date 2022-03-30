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

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Connector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.converter.ConnAndTaskConfigConverter;
import org.apache.rocketmq.connect.runtime.converter.JsonConverter;
import org.apache.rocketmq.connect.runtime.converter.ListConverter;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigManagementServiceImpl implements ConfigManagementService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Current connector configs in the store.
     * 把connectorName及其对应的config保存在缓存中，这个name就是URL path中指定的connectorName
     */
    private KeyValueStore<String/*connectorName*/, ConnectKeyValue/*config*/> connectorKeyValueStore;

    /**
     * Current task configs in the store.
     * 上面connectorKeyValueStore是connectorName及对应的config
     * 而这里的taskKeyValueStore是connectorName通过connectorConfig调用getTaskConfig方法又可能拆分出来N个task任务,所以是个List集合
     */
    private KeyValueStore<String/*connectorName*/, List<ConnectKeyValue>/*taskConfig*/> taskKeyValueStore;

    /**
     * All listeners to trigger while config change.
     */
    private Set<ConnectorConfigUpdateListener> connectorConfigUpdateListener;

    /**
     * Synchronize config with other workers.
     */
    private DataSynchronizer<String, ConnAndTaskConfigs> dataSynchronizer;

    private final Plugin plugin;

    private final String configManagePrefix = "ConfigManage";

    public ConfigManagementServiceImpl(ConnectConfig connectConfig, Plugin plugin) {

        this.connectorConfigUpdateListener = new HashSet<>();
        this.dataSynchronizer = new BrokerBasedLog<>(connectConfig,
            connectConfig.getConfigStoreTopic(),
            ConnectUtil.createGroupName(configManagePrefix, connectConfig.getWorkerId()),
            new ConfigChangeCallback(),
            new JsonConverter(),
            new ConnAndTaskConfigConverter());
        this.connectorKeyValueStore = new FileBaseKeyValueStore<>(
            FilePathConfigUtil.getConnectorConfigPath(connectConfig.getStorePathRootDir()),
            new JsonConverter(),
            new JsonConverter(ConnectKeyValue.class));
        this.taskKeyValueStore = new FileBaseKeyValueStore<>(
            FilePathConfigUtil.getTaskConfigPath(connectConfig.getStorePathRootDir()),
            new JsonConverter(),
            new ListConverter(ConnectKeyValue.class));
        this.plugin = plugin;
    }

    @Override
    public void start() {

        connectorKeyValueStore.load();
        taskKeyValueStore.load();
        dataSynchronizer.start();
        sendOnlineConfig();
    }

    @Override
    public void stop() {

        sendSynchronizeConfig();
        connectorKeyValueStore.persist();
        taskKeyValueStore.persist();
        dataSynchronizer.stop();
    }

    @Override
    public Map<String, ConnectKeyValue> getConnectorConfigs() {

        Map<String, ConnectKeyValue> result = new HashMap<>();
        Map<String, ConnectKeyValue> connectorConfigs = connectorKeyValueStore.getKVMap();
        for (String connectorName : connectorConfigs.keySet()) {
            ConnectKeyValue config = connectorConfigs.get(connectorName);
            if (0 != config.getInt(RuntimeConfigDefine.CONFIG_DELETED)) {
                continue;
            }
            result.put(connectorName, config);
        }
        return result;
    }

    @Override
    public Map<String, ConnectKeyValue> getConnectorConfigsIncludeDeleted() {

        Map<String, ConnectKeyValue> result = new HashMap<>();
        Map<String, ConnectKeyValue> connectorConfigs = connectorKeyValueStore.getKVMap();
        for (String connectorName : connectorConfigs.keySet()) {
            ConnectKeyValue config = connectorConfigs.get(connectorName);
            result.put(connectorName, config);
        }
        return result;
    }

    @Override
    public String putConnectorConfig(String connectorName, ConnectKeyValue configs) throws Exception {

        ConnectKeyValue exist = connectorKeyValueStore.get(connectorName);
        if (null != exist) {
            Long updateTimestamp = exist.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
            if (null != updateTimestamp) {
                configs.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, updateTimestamp);
            }
        }
        if (configs.equals(exist)) {
            return "Connector with same config already exist.";
        }

        Long currentTimestamp = System.currentTimeMillis();
        configs.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, currentTimestamp);

        //mz 检查参数的kv对必须含有必要的某个key例如 connector-class
        for (String requireConfig : RuntimeConfigDefine.REQUEST_CONFIG) {
            if (!configs.containsKey(requireConfig)) {
                return "Request config key: " + requireConfig;
            }
        }

        //mz 反射实例化connectorClass
        String connectorClass = configs.getString(RuntimeConfigDefine.CONNECTOR_CLASS);
        ClassLoader classLoader = plugin.getPluginClassLoader(connectorClass);
        Class clazz;
        if (null != classLoader) {
            clazz = Class.forName(connectorClass, true, classLoader);
        } else {
            clazz = Class.forName(connectorClass);
        }
        final Connector connector = (Connector) clazz.getDeclaredConstructor().newInstance();

        //mz 校验并把url参数的config设置到connector中
        String errorMessage = connector.verifyAndSetConfig(configs);
        if (errorMessage != null && errorMessage.length() > 0) {
            return errorMessage;
        }
        //mz 把connectorName及其对应的config保存在缓存中，这个name就是URL path中指定的connectorName
        connectorKeyValueStore.put(connectorName, configs);

        //mz 重新计算,也就是初始化task
        recomputeTaskConfigs(connectorName, connector, currentTimestamp);
        return "";
    }

    @Override
    public void recomputeTaskConfigs(String connectorName, Connector connector, Long currentTimestamp) {
        ConnectKeyValue connectConfig = connectorKeyValueStore.get(connectorName);
        boolean directEnable = Boolean.parseBoolean(connectConfig.getString(RuntimeConfigDefine.CONNECTOR_DIRECT_ENABLE));
        List<KeyValue> taskConfigs = connector.taskConfigs();
        List<ConnectKeyValue> converterdConfigs = new ArrayList<>();
        for (KeyValue keyValue : taskConfigs) {
            //mz 外层一次循环就是一个task
            ConnectKeyValue newKeyValue = new ConnectKeyValue();
            for (String key : keyValue.keySet()) {
                newKeyValue.put(key, keyValue.getString(key));
            }
            if (directEnable) {
                newKeyValue.put(RuntimeConfigDefine.TASK_TYPE, Worker.TaskType.DIRECT.name());
                newKeyValue.put(RuntimeConfigDefine.SOURCE_TASK_CLASS, connectConfig.getString(RuntimeConfigDefine.SOURCE_TASK_CLASS));
                newKeyValue.put(RuntimeConfigDefine.SINK_TASK_CLASS, connectConfig.getString(RuntimeConfigDefine.SINK_TASK_CLASS));
            }
            newKeyValue.put(RuntimeConfigDefine.TASK_CLASS, connector.taskClass().getName());
            newKeyValue.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, currentTimestamp);
            converterdConfigs.add(newKeyValue);
        }
        //放到缓存里面
        putTaskConfigs(connectorName, converterdConfigs);
        //同步config数据到rocketMQ
        sendSynchronizeConfig();
        //触发监听器，这里会启动task任务
        triggerListener();
    }

    @Override
    public void removeConnectorConfig(String connectorName) {

        ConnectKeyValue config = connectorKeyValueStore.get(connectorName);

        config.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, System.currentTimeMillis());
        config.put(RuntimeConfigDefine.CONFIG_DELETED, 1);
        List<ConnectKeyValue> taskConfigList = taskKeyValueStore.get(connectorName);
        taskConfigList.add(config);

        connectorKeyValueStore.put(connectorName, config);
        putTaskConfigs(connectorName, taskConfigList);
        log.info("[ISSUE #2027] After removal The configs are:\n" + getConnectorConfigs().toString());
        sendSynchronizeConfig();
        triggerListener();
    }

    @Override
    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {
        Map<String, List<ConnectKeyValue>> result = new HashMap<>();
        Map<String, List<ConnectKeyValue>> taskConfigs = taskKeyValueStore.getKVMap();
        Map<String, ConnectKeyValue> filteredConnector = getConnectorConfigs();
        for (String connectorName : taskConfigs.keySet()) {
            if (!filteredConnector.containsKey(connectorName)) {
                continue;
            }
            result.put(connectorName, taskConfigs.get(connectorName));
        }
        return result;
    }

    private void putTaskConfigs(String connectorName, List<ConnectKeyValue> configs) {

        List<ConnectKeyValue> exist = taskKeyValueStore.get(connectorName);
        if (null != exist && exist.size() > 0) {
            taskKeyValueStore.remove(connectorName);
        }
        taskKeyValueStore.put(connectorName, configs);
    }

    @Override
    public void persist() {

        this.connectorKeyValueStore.persist();
        this.taskKeyValueStore.persist();
    }

    @Override
    public void registerListener(ConnectorConfigUpdateListener listener) {

        this.connectorConfigUpdateListener.add(listener);
    }

    private void triggerListener() {

        if (null == this.connectorConfigUpdateListener) {
            return;
        }
        for (ConnectorConfigUpdateListener listener : this.connectorConfigUpdateListener) {
            listener.onConfigUpdate();
        }
    }

    private void sendOnlineConfig() {

        ConnAndTaskConfigs configs = new ConnAndTaskConfigs();
        configs.setConnectorConfigs(connectorKeyValueStore.getKVMap());
        configs.setTaskConfigs(taskKeyValueStore.getKVMap());
        dataSynchronizer.send(ConfigChangeEnum.ONLINE_KEY.name(), configs);
    }

    private void sendSynchronizeConfig() {

        ConnAndTaskConfigs configs = new ConnAndTaskConfigs();
        configs.setConnectorConfigs(connectorKeyValueStore.getKVMap());
        configs.setTaskConfigs(taskKeyValueStore.getKVMap());

        //发送到rocketMQ,topic就是config的那个
        dataSynchronizer.send(ConfigChangeEnum.CONFIG_CHANG_KEY.name(), configs);
    }

    private class ConfigChangeCallback implements DataSynchronizerCallback<String, ConnAndTaskConfigs> {

        @Override
        public void onCompletion(Throwable error, String key, ConnAndTaskConfigs result) {

            boolean changed = false;
            //新的configManagementServiceImpl上线即ConfigManagementServiceImpl.start()时候会sendOnline信息,其实body都一样都是自己本地缓存的connectorKeyValueStore和taskKeyValueStore,但是key不一样，key叫做online
            //其他节点要是收到online信息，就知道别的节点上线了，那么将别的节点的config数据同步过来，然后把自己的config缓存数据发出去和对方进行merge,虽然是集群消费，但是每个节点的workerID不一样也就是groupID不一样，其实就是广播消费模式
            switch (ConfigChangeEnum.valueOf(key)) {
                case ONLINE_KEY:
                    mergeConfig(result);
                    changed = true;
                    sendSynchronizeConfig();
                    break;
                case CONFIG_CHANG_KEY://就是source和sink的config信息。
                    changed = mergeConfig(result);
                    break;
                default:
                    break;
            }
            if (changed) {
                //会触发负载均衡
                triggerListener();
            }
        }
    }

    /**
     * Merge new received configs with the configs in memory.
     *
     * @param newConnAndTaskConfig
     * @return
     */
    private boolean mergeConfig(ConnAndTaskConfigs newConnAndTaskConfig) {
        boolean changed = false;
        for (String connectorName : newConnAndTaskConfig.getConnectorConfigs().keySet()) {
            ConnectKeyValue newConfig = newConnAndTaskConfig.getConnectorConfigs().get(connectorName);
            ConnectKeyValue oldConfig = getConnectorConfigsIncludeDeleted().get(connectorName);
            if (null == oldConfig) {
                changed = true;
                connectorKeyValueStore.put(connectorName, newConfig);
                taskKeyValueStore.put(connectorName, newConnAndTaskConfig.getTaskConfigs().get(connectorName));
            } else {

                Long oldUpdateTime = oldConfig.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
                Long newUpdateTime = newConfig.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
                if (newUpdateTime > oldUpdateTime) {
                    changed = true;
                    connectorKeyValueStore.put(connectorName, newConfig);
                    taskKeyValueStore.put(connectorName, newConnAndTaskConfig.getTaskConfigs().get(connectorName));
                }
            }
        }
        return changed;
    }

    private enum ConfigChangeEnum {

        /**
         * Insert or update config.
         */
        CONFIG_CHANG_KEY,

        /**
         * A worker online.
         */
        ONLINE_KEY
    }

    @Override
    public Plugin getPlugin() {
        return this.plugin;
    }
}
