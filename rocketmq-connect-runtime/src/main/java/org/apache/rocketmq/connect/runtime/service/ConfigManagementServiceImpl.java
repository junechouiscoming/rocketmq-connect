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
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Connector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.connect.runtime.common.ConnectorAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.converter.ConnAndTaskConfigConverter;
import org.apache.rocketmq.connect.runtime.converter.JsonConverter;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.jetbrains.annotations.NotNull;
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
    private DataSynchronizer<String, ConnectorAndTaskConfigs> dataSynchronizer;

    private final Plugin plugin;

    private final String configManagePrefix = "connector-configManage";

    public ConfigManagementServiceImpl(ConnectConfig connectConfig, Plugin plugin) {

        this.connectorConfigUpdateListener = new HashSet<>();
        this.dataSynchronizer = new BrokerBasedLog<>(connectConfig,
            connectConfig.getConfigStoreTopic(),
            ConnectUtil.createGroupName(configManagePrefix, connectConfig.getWorkerId()),
            new ConfigChangeCallback(),
            new JsonConverter(),
            new ConnAndTaskConfigConverter());
        this.connectorKeyValueStore = new FileBaseKeyValueStore<String,ConnectKeyValue>(
            FilePathConfigUtil.getConnectorConfigPath(connectConfig.getStorePathRootDir()),new TypeReference<Map<String,ConnectKeyValue>>() {});
        this.taskKeyValueStore = new FileBaseKeyValueStore<String,List<ConnectKeyValue>>(
            FilePathConfigUtil.getTaskConfigPath(connectConfig.getStorePathRootDir()),new TypeReference<Map<String,List<ConnectKeyValue>>>() {}){

        };
        this.plugin = plugin;
    }

    @Override
    public synchronized void start() {
        connectorKeyValueStore.load();
        taskKeyValueStore.load();
        dataSynchronizer.start();
        sendOnlineConfig();
    }

    @Override
    public synchronized void stop() {

        sendSynchronizeConfig();
        connectorKeyValueStore.persist();
        taskKeyValueStore.persist();
        dataSynchronizer.stop();
    }

    /**
     * NORMAL and DISABLE
     * @return
     */
    @Override
    public synchronized Map<String, ConnectKeyValue> getConnectorConfigs(List<Integer> status) {
        Map<String, ConnectKeyValue> result = new HashMap<>();
        Map<String, ConnectKeyValue> connectorConfigs = connectorKeyValueStore.getKVMap();
        for (String connectorName : connectorConfigs.keySet()) {
            ConnectKeyValue config = connectorConfigs.get(connectorName);
            int configInt = config.getInt(RuntimeConfigDefine.CONFIG_STATUS);
            if (status==null || status.contains(configInt)) {
                result.put(connectorName, config);
            }
        }
        return result;
    }

    @Override
    public synchronized String dynamicUpdateTaskNum(String connectorName, int newTaskNum) {
        //只要以前存在，就不允许覆盖，只能删除旧的然后新增新的
        ConnectKeyValue exist = connectorKeyValueStore.get(connectorName);
        if (exist != null) {
            boolean isRemoved = exist.getInt(RuntimeConfigDefine.CONFIG_STATUS) == RuntimeConfigDefine.CONFIG_STATUS_REMOVE;
            if (isRemoved) {
                //已移除的话就可以继续覆盖新增配置,已移除的配置在各节点均不会得到任何有效处理,例如分配Task执行.只是内存或者配置文件可能会驻留
                throw new IllegalArgumentException("Connector was already removed.");
            }
        }else{
            throw new IllegalArgumentException("the Connector not exists");
        }

        //不管是否禁用，直接扩充Task数量即可
        List<ConnectKeyValue> taskKV = taskKeyValueStore.get(connectorName);
        int oldTaskNum = taskKV.size();

        List<ConnectKeyValue> newKVLst = new ArrayList<>();
        if (taskKV.size()>0) {
            if (newTaskNum>oldTaskNum) {
                //扩容
                ConnectKeyValue value = taskKV.get(0);
                for (int i = 0; i < newTaskNum - oldTaskNum; i++) {
                    ConnectKeyValue connectKeyValue = new ConnectKeyValue(new ConcurrentHashMap<>(value.getProperties()));
                    //只有新的task才会赋值TaskUID,老的UID不动，否则负载均衡那边会stop再start
                    connectKeyValue.put(RuntimeConfigDefine.TASK_UID, UUID.randomUUID().toString());
                    newKVLst.add(connectKeyValue);
                }
                for (ConnectKeyValue keyValue : taskKV) {
                    newKVLst.add(new ConnectKeyValue(new ConcurrentHashMap<>(keyValue.getProperties())));
                }
            }else {
                //缩容
                for (ConnectKeyValue keyValue : taskKV) {
                    newKVLst.add(new ConnectKeyValue(new ConcurrentHashMap<>(keyValue.getProperties())));
                }
                Iterator<ConnectKeyValue> iterator = newKVLst.iterator();
                int restC = newKVLst.size()-newTaskNum;
                while (iterator.hasNext() && --restC>=0) {
                    iterator.next();
                    iterator.remove();
                }
            }
        }else{
            throw new UnsupportedOperationException("the old task nums is zero , so can`t increase it , you can remove and create a new connector which taskNum greater than zero");
        }

        //重新赋值taskID序号
        long currentTimeMillis = System.currentTimeMillis();
        int count = 0;
        for (ConnectKeyValue keyValue : newKVLst) {
            keyValue.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, currentTimeMillis);
            keyValue.put(RuntimeConfigDefine.TASK_ID, String.format("%s/%s",++count,newKVLst.size()));
        }

        //放到缓存里面 已经加了lock,所以负载均衡那边就会lock
        putTaskConfigs(connectorName, newKVLst);

        exist.put(RuntimeConfigDefine.TASK_NUM, String.valueOf(newTaskNum));
        exist.put(RuntimeConfigDefine.UPDATE_TIMESTAMP,currentTimeMillis);

        //同步config数据到rocketMQ,这里一发消息,就会收到其他节点的消息也会触发重平衡监听,所以Lock是需要的...
        sendSynchronizeConfig();
        //触发监听器，这里会启动task任务
        triggerListener();
        return String.format("oldTaskNum : %s newTaskNum : %s",oldTaskNum,newTaskNum);
    }

    @Override
    public synchronized void updateConnectorConfig(String connectorName, ConnectKeyValue configs) throws Exception {

        if (configs.getProperties().size()==1 && configs.containsKey(RuntimeConfigDefine.TASK_NUM)) {
            throw new IllegalArgumentException("if you only want to change the num of task , you should use '/connectors/taskNum/:connectorName/:taskNum' to change it ！");
        }

        //只要以前存在，就不允许覆盖，只能删除旧的然后新增新的
        ConnectKeyValue exist = connectorKeyValueStore.get(connectorName);
        if (exist != null) {
            boolean isRemoved = exist.getInt(RuntimeConfigDefine.CONFIG_STATUS) == RuntimeConfigDefine.CONFIG_STATUS_REMOVE;
            if (isRemoved) {
                //已移除的话就可以继续覆盖新增配置,已移除的配置在各节点均不会得到任何有效处理,例如分配Task执行.只是内存或者配置文件可能会驻留
                throw new IllegalArgumentException("Connector was already removed.");
            }
        }else{
            throw new IllegalArgumentException("the Connector not exists");
        }


        //不管是已禁用还是启用状态,都可以覆盖原本的参数配置
        for (Map.Entry<String, String> entry : configs.getProperties().entrySet()) {
            exist.put(entry.getKey(), entry.getValue());
        }
        configs = exist;

        Long currentTimestamp = System.currentTimeMillis();
        exist.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, currentTimestamp);


        //mz 检查参数的kv对必须含有必要的某个key例如 connector-class
        for (String requireConfig : RuntimeConfigDefine.REQUEST_CONFIG) {
            if (!configs.containsKey(requireConfig)) {
                throw new IllegalArgumentException("after update still Request config key: " + requireConfig);
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
            throw new IllegalArgumentException(errorMessage);
        }
        //mz 把connectorName及其对应的config保存在缓存中，这个name就是URL path中指定的connectorName
        connectorKeyValueStore.put(connectorName, configs);

        //mz 重新计算,也就是初始化task
        recomputeTaskConfigs(connectorName, connector, currentTimestamp);
    }

    @Override
    public synchronized void putNewConnectorConfig(String connectorName, ConnectKeyValue configs) throws Exception {

        //只要以前存在，就不允许覆盖，只能删除旧的然后新增新的
        ConnectKeyValue exist = connectorKeyValueStore.get(connectorName);
        if (exist != null) {
            boolean isRemoved = exist.getInt(RuntimeConfigDefine.CONFIG_STATUS) == RuntimeConfigDefine.CONFIG_STATUS_REMOVE;
            if (isRemoved) {
                //已移除的话就可以继续覆盖新增配置,已移除的配置在各节点均不会得到任何有效处理,例如分配Task执行.只是内存或者配置文件可能会驻留
            }else{
                throw new IllegalArgumentException("Connector with same config already exist.");
            }
        }

        Long currentTimestamp = System.currentTimeMillis();
        configs.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, currentTimestamp);
        configs.put(RuntimeConfigDefine.CONFIG_STATUS, RuntimeConfigDefine.CONFIG_STATUS_ENABLE);

        //mz 检查参数的kv对必须含有必要的某个key例如 connector-class
        for (String requireConfig : RuntimeConfigDefine.REQUEST_CONFIG) {
            if (!configs.containsKey(requireConfig)) {
                throw new IllegalArgumentException("Request config key: " + requireConfig);
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
            throw new IllegalArgumentException(errorMessage);
        }
        //mz 把connectorName及其对应的config保存在缓存中，这个name就是URL path中指定的connectorName
        connectorKeyValueStore.put(connectorName, configs);

        //mz 重新计算,也就是初始化task
        recomputeTaskConfigs(connectorName, connector, currentTimestamp);
    }

    @Override
    public synchronized void recomputeTaskConfigs(String connectorName, Connector connector, Long currentTimestamp) {
        List<ConnectKeyValue> converterdConfigs = convertAndGetTaskConfigs(connectorName, connector, currentTimestamp);
        //放到缓存里面
        putTaskConfigs(connectorName, converterdConfigs);
        //同步config数据到rocketMQ,这里一发消息,就会收到其他节点的消息也会触发重平衡监听,所以Lock是需要的...
        sendSynchronizeConfig();
        //触发监听器，这里会启动task任务
        triggerListener();
    }

    private List<ConnectKeyValue> convertAndGetTaskConfigs(String connectorName, Connector connector, Long currentTimestamp) {
        ConnectKeyValue connectConfig = connectorKeyValueStore.get(connectorName);
        boolean directEnable = Boolean.parseBoolean(connectConfig.getString(RuntimeConfigDefine.CONNECTOR_DIRECT_ENABLE));
        List<KeyValue> taskConfigs = connector.taskConfigs();
        List<ConnectKeyValue> converterdConfigs = new ArrayList<>();
        int i = 0;
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
            // mz 小改一下 判空 taskType=direct时候理论不需要这个taskClass
            newKeyValue.put(RuntimeConfigDefine.TASK_CLASS, connector.taskClass()!=null? connector.taskClass().getName():null);
            newKeyValue.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, currentTimestamp);
            newKeyValue.put(RuntimeConfigDefine.TASK_ID,++i+"/"+taskConfigs.size());
            newKeyValue.put(RuntimeConfigDefine.TASK_UID,UUID.randomUUID().toString());
            converterdConfigs.add(newKeyValue);
        }
        return converterdConfigs;
    }

    /**
     * 同时会把该Connector对应的Task停止,并且这个效果会自动同步传播到其他节点
     * @param connectorName
     */
    @Override
    public synchronized void removeConnectorConfig(String connectorName) {
        ConnectKeyValue config = connectorKeyValueStore.get(connectorName);
        if (config==null) {
            throw new IllegalArgumentException(String.format("the connector with name %s not exists!",connectorName));
        }

        if (config.getInt(RuntimeConfigDefine.CONFIG_STATUS)==RuntimeConfigDefine.CONFIG_STATUS_REMOVE) {
            throw new IllegalArgumentException("the connector had already been removed");
        }

        config.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, System.currentTimeMillis());
        config.put(RuntimeConfigDefine.CONFIG_STATUS, RuntimeConfigDefine.CONFIG_STATUS_REMOVE);

        connectorKeyValueStore.put(connectorName, config);
        log.info("After removal connector withe name "+connectorName+",the rest disable and enable configs are:\n" + JSON.toJSONString(getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST),SerializerFeature.PrettyFormat));
        sendSynchronizeConfig();
        triggerListener();
    }

    /**
     * @param connectorName
     */
    @Override
    public synchronized void disableConnectorConfig(String connectorName) {

        ConnectKeyValue config = connectorKeyValueStore.get(connectorName);

        if (config==null) {
            throw new IllegalArgumentException(String.format("the connector with name %s not exists!",connectorName));
        }

        if (config.getInt(RuntimeConfigDefine.CONFIG_STATUS)==RuntimeConfigDefine.CONFIG_STATUS_DISABLE) {
            throw new IllegalArgumentException("the connector had been disabled");
        }

        config.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, System.currentTimeMillis());
        config.put(RuntimeConfigDefine.CONFIG_STATUS, RuntimeConfigDefine.CONFIG_STATUS_DISABLE);

        connectorKeyValueStore.put(connectorName, config);
        log.info("After disable connector withe name "+connectorName+",the rest enable configs are:\n" + JSON.toJSONString(getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_LST),SerializerFeature.PrettyFormat));
        sendSynchronizeConfig();
        triggerListener();
    }

    /**
     *
     * @param connectorName
     * @throws IllegalStateException status = RuntimeConfigDefine.CONFIG_STATUS_REMOVE
     */
    @Override
    public synchronized void enableConnectorConfig(String connectorName) throws IllegalStateException {

        ConnectKeyValue config = connectorKeyValueStore.get(connectorName);

        if (config==null) {
            throw new IllegalArgumentException(String.format("the connector with name %s not exists!",connectorName));
        }

        if (config.getInt(RuntimeConfigDefine.CONFIG_STATUS)==RuntimeConfigDefine.CONFIG_STATUS_REMOVE) {
            throw new IllegalStateException(String.format("the connector with name %s is removed !",connectorName));
        }

        config.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, System.currentTimeMillis());
        config.put(RuntimeConfigDefine.CONFIG_STATUS, RuntimeConfigDefine.CONFIG_STATUS_ENABLE);

        connectorKeyValueStore.put(connectorName, config);
        log.info("After enable connector withe name "+connectorName+",the rest enable configs are:\n" + JSON.toJSONString(getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_LST),SerializerFeature.PrettyFormat));
        sendSynchronizeConfig();
        triggerListener();
    }

    @Override
    public synchronized Map<String, List<ConnectKeyValue>> getTaskConfigs(List<Integer> status) {
        Map<String, List<ConnectKeyValue>> result = new HashMap<>();

        Map<String, ConnectKeyValue> connectorConfigs = getConnectorConfigs(status);
        Map<String, List<ConnectKeyValue>> taskConfigs = taskKeyValueStore.getKVMap();

        if (taskConfigs != null && connectorConfigs != null) {
            for (String connectorName : connectorConfigs.keySet()) {
                List<ConnectKeyValue> values = taskConfigs.get(connectorName);
                result.put(connectorName, values == null ? new ArrayList<>() : new ArrayList<>(values));
            }
        }
        return result;
    }

    private synchronized void putTaskConfigs(String connectorName, List<ConnectKeyValue> configs) {

        List<ConnectKeyValue> exist = taskKeyValueStore.get(connectorName);
        if (null != exist && exist.size() > 0) {
            taskKeyValueStore.remove(connectorName);
        }
        taskKeyValueStore.put(connectorName, configs);
    }

    @Override
    public synchronized void persist() {
        this.connectorKeyValueStore.persist();
        this.taskKeyValueStore.persist();
    }

    @Override
    public synchronized void registerListener(ConnectorConfigUpdateListener listener) {

        this.connectorConfigUpdateListener.add(listener);
    }

    /**
     * 这个lock见recomputeTaskConfigs中的注释
     */
    private synchronized void triggerListener() {

        if (null == this.connectorConfigUpdateListener) {
            return;
        }
        for (ConnectorConfigUpdateListener listener : this.connectorConfigUpdateListener) {
            listener.onConfigUpdate();
        }
    }

    private synchronized void sendOnlineConfig() {

        ConnectorAndTaskConfigs configs = new ConnectorAndTaskConfigs();
        configs.setConnectorConfigs(new HashMap<>(connectorKeyValueStore.getKVMap()));
        configs.setTaskConfigs(new HashMap<>(taskKeyValueStore.getKVMap()));
        dataSynchronizer.send(ConfigChangeEnum.ONLINE_KEY.name(), configs);
    }

    private synchronized void sendSynchronizeConfig() {

        ConnectorAndTaskConfigs configs = new ConnectorAndTaskConfigs();
        configs.setConnectorConfigs(new HashMap<>(connectorKeyValueStore.getKVMap()));
        configs.setTaskConfigs(new HashMap<>(taskKeyValueStore.getKVMap()));

        //发送到rocketMQ,topic就是config的那个
        dataSynchronizer.send(ConfigChangeEnum.CONFIG_CHANG_KEY.name(), configs);
    }

    private class ConfigChangeCallback implements DataSynchronizerCallback<String, ConnectorAndTaskConfigs> {

        /**
         * 需要lock
         * @param error
         * @param key
         * @param result
         */
        @Override
        public void onCompletion(Throwable error, String key, ConnectorAndTaskConfigs result) {

            synchronized (ConfigManagementServiceImpl.this){
                boolean changed = false;
                //新的configManagementServiceImpl上线即ConfigManagementServiceImpl.start()时候会sendOnline信息,其实body都一样都是自己本地缓存的connectorKeyValueStore和taskKeyValueStore,但是key不一样，key叫做online
                //其他节点要是收到online信息，就知道别的节点上线了，那么将别的节点的config数据同步过来，然后把自己的config缓存数据发出去和对方进行merge,虽然是集群消费，但是每个节点的workerID不一样也就是groupID不一样，其实就是广播消费模式
                //一个节点A上线，会把ONLINE发给BCD节点,然后BCD节点都会回一个SYNC信息,对于A而言,他会收到BCD的三条SYNC消息。
                //任意一个节点都会收到其他节点的SYNC信息
                //假设总有1个节点是拥有之前最新最完整的数据,那么它一定会广播出去消息，其他节点一定会merge也成为最完整的数据
                //如果所有节点的数据都不相同,但是因为每个节点都会收到其他节点的数据,那么对于这个节点而言它的数据最终就是汇集所有节点的完整数据了。
                //就是all in one
                switch (ConfigChangeEnum.valueOf(key)) {
                    //这个不是connector上线，这个是connectorController上线才会发ONLINE
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
    }

    /**
     * Merge new received configs with the configs in memory.
     * @param newConnAndTaskConfig
     * @return
     */
    private synchronized boolean mergeConfig(ConnectorAndTaskConfigs newConnAndTaskConfig) {
        boolean changed = false;
        for (String connectorName : newConnAndTaskConfig.getConnectorConfigs().keySet()) {
            ConnectKeyValue newConfig = newConnAndTaskConfig.getConnectorConfigs().get(connectorName);
            ConnectKeyValue oldConfig = getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST).get(connectorName);
            //删除配置该咋搞 new里面肯定是没有了这肯定不行啊，所以说new里面还是得有，但是持久化时候不会持久化到本地。
            //这样如果有一个节点内存里有这个REMOVE,那么其他节点都会收到。然后其他节点都会删除本地的这个config
            //当最后这个节点也下线了，那就是彻底删除了。
            if (null == oldConfig) {
                changed = true;
                connectorKeyValueStore.put(connectorName, newConfig);
                taskKeyValueStore.put(connectorName, newConnAndTaskConfig.getTaskConfigs().get(connectorName));
            } else {
                //对于只有2个节点的系统，如果A是旧的，B是新的，那么A给B发,B会忽略，然后B给A发，A会更新，最终达到一致性
                    //如果A含数据,B不含，那么B会同步到A的，反之A会同步为B的数据
                //对于只有3个节点的系统，如果A是旧的，B是新的，C是更加新的，那么BC收到A的数据都忽略,但是B会更新C的消息，A会更新B和C的消息。如果A先收到B的消息,后续再收到C的消息也会更新的,反正不会
                    //如果A含数据,B含其他数据,C含其他其他数据,各不相同。最终也会一致的,因为自己不包含的数据始终会去同步别人的
                    //比如A收到B和C的,B会收到A和C，C会收到A和B,最终一定会一致
                //其实说白了就是每个节点都会收到其他所有节点的数据,如果自己存在数据则判断数据取那个最新的,逐步更新。如果数据不存在则直接用别人的。整个系统一定会逐步向更新更完备发展。不会出现倒退的情况,因为newUpdateTime > oldUpdateTime才会更新
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
