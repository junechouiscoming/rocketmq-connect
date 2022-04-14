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

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.sink.SinkTask;
import io.openmessaging.connector.api.source.SourceTask;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.TaskPositionCommitService;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.apache.rocketmq.connect.runtime.utils.PluginClassLoader;
import org.apache.rocketmq.connect.runtime.utils.ServiceThread;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A worker to schedule all connectors and tasks in a process.
 */
public class Worker {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    /**
     * 负载均衡当前时刻这一秒钟分配给自己的task列表
     */
    private Map<String, List<ConnectKeyValue>> nowAllocatedTaskConfigs = new ConcurrentHashMap<>();
    /**
     * 上次mainTaskState时候task列表的快照
     */
    private Map<String, List<ConnectKeyValue>> lastMaintainTaskConfigsSnapshot = new ConcurrentHashMap<>();

    /**
     * Current tasks to its Future map 参见currentEpochWorkerTaskMap,这里面这个里面的任务状态不定，可能已经stop或者error或者running等，保证里面是本次任务维护开始时刻仅包含当时负载均衡结果的task的集合
     */
    private Map<Runnable, Future> taskToFutureMap = new ConcurrentHashMap<>();

    /**
     * 当前maintainTaskState epoch的task集合。
     * 每次调度维护任务状态都会动态增删其中的task，保证里面是本次任务维护开始时刻仅包含当时负载均衡结果的task的集合。这个里面的任务状态不定，可能已经stop或者error或者running等
     */
    private Map<ConnectKeyValueWrapper, WorkerTask> currentEpochWorkerTaskMap = new ConcurrentHashMap<>();

    /**
     * Thread pool for connectors and tasks.
     */
    private final ExecutorService taskExecutor;

    /**
     * Position management for source tasks.
     */
    private final PositionManagementService positionManagementService;

    /**
     * Offset management for source tasks.
     */
    private final PositionManagementService offsetManagementService;

    /**
     * A scheduled task to commit source position of source tasks.
     */
    private final TaskPositionCommitService taskPositionCommitService;

    private final ConnectConfig connectConfig;

    private final Plugin plugin;

    /**
     * Atomic state variable
     */
    private AtomicReference<WorkerState> workerState;

    private StateMachineService stateMachineService = new StateMachineService();

    public Worker(ConnectConfig connectConfig,
                  PositionManagementService positionManagementService, PositionManagementService offsetManagementService,
                  Plugin plugin) {
        this.connectConfig = connectConfig;
        //这里这个cached很重要 因为task基本都是要永久运行
        this.taskExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory("WorkTask-Executor-"));
        this.positionManagementService = positionManagementService;
        this.offsetManagementService = offsetManagementService;
        this.taskPositionCommitService = new TaskPositionCommitService(
            this,
            positionManagementService,
            offsetManagementService);
        this.plugin = plugin;
    }

    /**
     * ConnectController.start()时候会把worker也启动
     */
    public void start() {
        workerState = new AtomicReference<>(WorkerState.STARTED);
        taskPositionCommitService.start();
        stateMachineService.start();
    }


    public Map<String, List<ConnectKeyValue>> getTasks() {
        synchronized (nowAllocatedTaskConfigs){
            return new HashMap<>(nowAllocatedTaskConfigs);
        }
    }
    public void setTasks(Map<String, List<ConnectKeyValue>> taskConfigs) {
        synchronized (nowAllocatedTaskConfigs) {
            this.nowAllocatedTaskConfigs.clear();
            this.nowAllocatedTaskConfigs.putAll(taskConfigs);
        }
    }


    /**
     * We can choose to persist in-memory task status
     * so we can view history tasks
     */
    public void stop() {
        workerState.set(WorkerState.TERMINATED);
        stateMachineService.shutdown();
        while (getWorkingTasks().size()>0){
            try {
                log.info("waiting all task to stopped to commit all task position....");
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public Set<WorkerTask> getWorkingTasks() {
        Collection<WorkerTask> values = currentEpochWorkerTaskMap.values();
        Set<WorkerTask> tasks = values.stream().filter(workerTask -> workerTask.getState() == WorkerTaskState.RUNNING).collect(Collectors.toSet());
        return tasks;
    }

    public Set<WorkerTask> getErrorTasks() {
        Collection<WorkerTask> values = currentEpochWorkerTaskMap.values();
        Set<WorkerTask> tasks = values.stream().filter(workerTask -> workerTask.getState() == WorkerTaskState.ERROR).collect(Collectors.toSet());
        return tasks;
    }

    public Set<WorkerTask> getPendingTasks() {
        Collection<WorkerTask> values = currentEpochWorkerTaskMap.values();
        Set<WorkerTask> tasks = values.stream().filter(workerTask -> workerTask.getState() == WorkerTaskState.PENDING).collect(Collectors.toSet());
        return tasks;
    }

    public Set<WorkerTask> getStoppingTasks() {
        Collection<WorkerTask> values = currentEpochWorkerTaskMap.values();
        Set<WorkerTask> tasks = values.stream().filter(workerTask -> workerTask.getState() == WorkerTaskState.STOPPING).collect(Collectors.toSet());
        return tasks;
    }

    public Set<WorkerTask> getStoppedTasks() {
        Collection<WorkerTask> values = currentEpochWorkerTaskMap.values();
        Set<WorkerTask> tasks = values.stream().filter(workerTask -> workerTask.getState() == WorkerTaskState.STOPPED).collect(Collectors.toSet());
        return tasks;
    }

    public void maintainConnectorState() {

    }

    public static void main(String[] args) {
        Map<String, List<ConnectKeyValue>> left = new ConcurrentHashMap<>();
        Map<String, List<ConnectKeyValue>> right = new ConcurrentHashMap<>();


        left.put("1", Arrays.asList(new ConnectKeyValue(),new ConnectKeyValue(),new ConnectKeyValue()));
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put("a", "a");
        left.put("2", Arrays.asList(connectKeyValue,new ConnectKeyValue(),new ConnectKeyValue()));//
        left.put("3", Arrays.asList(new ConnectKeyValue(),new ConnectKeyValue(),new ConnectKeyValue()));


        ConnectKeyValue connectKeyValue2 = new ConnectKeyValue();
        connectKeyValue2.put("a", "a");
        right.put("3", Arrays.asList(connectKeyValue2,new ConnectKeyValue(),new ConnectKeyValue()));//
        right.put("4", Arrays.asList(new ConnectKeyValue(),new ConnectKeyValue(),new ConnectKeyValue()));

        //上次本次都存在的一些task
        Map<String, List<ConnectKeyValue>> inCommon = difference(left,right,0);
        //上次存在，但是本次不存在的一些task
        Map<String, List<ConnectKeyValue>> onlyOnLeft = difference(left,right,1);
        //上次不存在，但是本次存在的一些task
        Map<String, List<ConnectKeyValue>> onlyOnRight = difference(left,right,2);

        System.out.println(inCommon);
        System.out.println(onlyOnLeft);
        System.out.println(onlyOnRight);
    }

    /**
     * myself
     */
    public void maintainTaskState() throws ExecutionException, InterruptedException {

        //confirm task list
        Map<String, List<ConnectKeyValue>> newTaskConfigs = new HashMap<>();
        synchronized (nowAllocatedTaskConfigs) {
            //latestTaskConfigs是负载均衡分配给自己的task列表
            newTaskConfigs.putAll(nowAllocatedTaskConfigs);
        }
        Map<String, List<ConnectKeyValue>> lastTaskConfigsSnapshot = new HashMap<>(this.lastMaintainTaskConfigsSnapshot);
        lastMaintainTaskConfigsSnapshot = new ConcurrentHashMap<>(newTaskConfigs);


        //上次本次都存在的一些task
        Map<String, List<ConnectKeyValue>> inCommon = difference(lastTaskConfigsSnapshot,newTaskConfigs,0);
        //上次存在，但是本次不存在的一些task
        Map<String, List<ConnectKeyValue>> inRemove = difference(lastTaskConfigsSnapshot,newTaskConfigs,1);
        //上次不存在，但是本次存在的一些task
        Map<String, List<ConnectKeyValue>> inAdd = difference(lastTaskConfigsSnapshot,newTaskConfigs,2);

        for (Map.Entry<String, List<ConnectKeyValue>> entry : inCommon.entrySet()) {
            for (ConnectKeyValue connectKeyValue : entry.getValue()) {
                WorkerTask workerTask = currentEpochWorkerTaskMap.get(ConnectKeyValueWrapper.wrap(connectKeyValue));
                List<ConnectKeyValue> newKVLst = newTaskConfigs.get(entry.getKey());
                for (ConnectKeyValue newKV : newKVLst) {
                    String uid = newKV.getString(RuntimeConfigDefine.TASK_UID);
                    if(uid.equals(workerTask.getTaskConfig().getString(RuntimeConfigDefine.TASK_UID))){
                        //替换成新的,主要是因为动态增加任务数量时,虽然UID和其他的一些配置没变,但是TASK-ID却变了
                        workerTask.getTaskConfig().setProperties(newKV.getProperties());
                    }
                }
            }
        }


        //已经不再分配给当前节点了,需要停止运行
        List<WorkerTask> workerTasks = new CopyOnWriteArrayList<>();
        for (Map.Entry<String, List<ConnectKeyValue>> entry : inRemove.entrySet()) {
            for (ConnectKeyValue connectKeyValue : entry.getValue()) {
                workerTasks.add(currentEpochWorkerTaskMap.get(ConnectKeyValueWrapper.wrap(connectKeyValue)));
            }
        }
        CompletableFuture.allOf(workerTasks.stream().map(v -> CompletableFuture.runAsync(() -> {
            try{
                v.stop();
            }catch (Exception ex){
                log.error("",ex);
            }finally {
                //一个任务停止就把它移除出去
                currentEpochWorkerTaskMap.remove(ConnectKeyValueWrapper.wrap(v.getTaskConfig()));
                taskToFutureMap.remove(v);
            }
        }, taskExecutor)).toArray(CompletableFuture[]::new)).whenComplete((unused, throwable) -> {
            //立即提交一次位移
            if (workerTasks.size()>0) {
                taskPositionCommitService.commitTaskPosition();
            }
        }).get();


        //新添加的task需要启动
        for (Map.Entry<String, List<ConnectKeyValue>> entry : inAdd.entrySet()) {
            for (ConnectKeyValue keyValue : entry.getValue()) {
                try {
                    WorkerTask task = createTask(entry.getKey(), keyValue);
                    Future<?> future = taskExecutor.submit(task);
                    taskToFutureMap.put(task,future);
                    currentEpochWorkerTaskMap.put(ConnectKeyValueWrapper.wrap(keyValue), task);
                }catch (Exception ex){
                    log.error(String.format("create task failed connector=%s ConnectKeyValue=%s",entry.getKey(),keyValue),ex);
                }
            }
        }
    }

    /**
     * @param connectorName connector的name
     * @param keyValue task的keyValue
     * @return
     */
    private WorkerTask createTask(String connectorName, ConnectKeyValue keyValue) {
        try {
            String taskType = keyValue.getString(RuntimeConfigDefine.TASK_TYPE);

            if (TaskType.DIRECT.name().equalsIgnoreCase(taskType)) {
                String sourceTaskClass = keyValue.getString(RuntimeConfigDefine.SOURCE_TASK_CLASS);
                Task sourceTask = getTask(sourceTaskClass);

                String sinkTaskClass = keyValue.getString(RuntimeConfigDefine.SINK_TASK_CLASS);
                Task sinkTask = getTask(sinkTaskClass);

                WorkerDirectTask workerDirectTask = new WorkerDirectTask(connectorName,
                        (SourceTask) sourceTask, (SinkTask) sinkTask, keyValue, positionManagementService, workerState);
                return workerDirectTask;
            }

            String taskClass = keyValue.getString(RuntimeConfigDefine.TASK_CLASS);
            ClassLoader loader = plugin.getPluginClassLoader(taskClass);
            final ClassLoader currentThreadLoader = plugin.currentThreadLoader();
            Class taskClazz;
            boolean isolationFlag = false;
            if (loader instanceof PluginClassLoader) {
                taskClazz = ((PluginClassLoader) loader).loadClass(taskClass, false);
                isolationFlag = true;
            } else {
                taskClazz = Class.forName(taskClass);
            }
            final Task task = (Task) taskClazz.getDeclaredConstructor().newInstance();
            final String converterClazzName = keyValue.getString(RuntimeConfigDefine.SOURCE_RECORD_CONVERTER);
            Converter recordConverter = null;
            if (StringUtils.isNotEmpty(converterClazzName)) {
                Class converterClazz = Class.forName(converterClazzName);
                recordConverter = (Converter) converterClazz.newInstance();
            }
            if (isolationFlag) {
                Plugin.compareAndSwapLoaders(loader);
            }
            if (task instanceof SourceTask) {
                DefaultMQProducer producer = ConnectUtil.initDefaultMQProducer(connectConfig);
                //必须保证提交到线程池之前，这里的类加载动作就全部完成。否线程池的类加载器是appClassLoader
                WorkerSourceTask workerSourceTask = new WorkerSourceTask(connectorName,(SourceTask) task, keyValue, positionManagementService, recordConverter, producer, workerState,isolationFlag?loader:currentThreadLoader);
                Plugin.compareAndSwapLoaders(currentThreadLoader);
                return workerSourceTask;

            } else if (task instanceof SinkTask) {
                DefaultMQPullConsumer consumer = ConnectUtil.initDefaultMQPullConsumer(connectConfig);
                if (connectConfig.isAutoCreateGroupEnable()) {
                    //这里我们可以借鉴一下！rocketMQ控制台的创建和使用
                    log.info("create sub group for sink task:"+consumer.getConsumerGroup());
                    ConnectUtil.createSubGroup(connectConfig, consumer.getConsumerGroup());
                }
                WorkerSinkTask workerSinkTask = new WorkerSinkTask(connectorName,(SinkTask) task, keyValue, offsetManagementService, recordConverter, consumer, workerState,isolationFlag?loader:currentThreadLoader);
                Plugin.compareAndSwapLoaders(currentThreadLoader);
                return workerSinkTask;
            }else {
                throw new IllegalArgumentException();
            }
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

    /**
     * @param type 0:common 1:onlyLeft 2:onlyRight
     * @return 比较 ConnectKeyValue 的uid,uid相同则任务相同
     */
    public static Map<String,List<ConnectKeyValue>> difference(Map<String,List<ConnectKeyValue>> left,Map<String,List<ConnectKeyValue>> right,int type){
        Map<String, List<ConnectKeyValue>> returnMap = new ConcurrentHashMap<>();

        Collection<String> keyUnion = CollectionUtils.union(left.keySet(), right.keySet());

        for (String key : keyUnion) {
            List<ConnectKeyValue> leftTaskList = left.get(key)==null?new ArrayList<>():left.get(key);
            List<ConnectKeyValue> rightTaskList = right.get(key)==null?new ArrayList<>():right.get(key);

            List<String/*uid*/> leftTempList = leftTaskList.stream().map(t->t.getProperties().get(RuntimeConfigDefine.TASK_UID)).collect(Collectors.toList());
            List<String/*uid*/> rightTempList = rightTaskList.stream().map(t->t.getProperties().get(RuntimeConfigDefine.TASK_UID)).collect(Collectors.toList());

            Collection<ConnectKeyValue> difference = null;
            if (type==1) {
                Collection<String/*uid*/> tmp = CollectionUtils.subtract(leftTempList,rightTempList);
                //左边比右边多
                difference = leftTaskList.stream().filter(t -> tmp.contains(t.getString(RuntimeConfigDefine.TASK_UID))).collect(Collectors.toList());
            }else if(type==2){
                Collection<String/*uid*/> tmp = CollectionUtils.subtract(rightTempList,leftTempList);
                //右边多
                difference = rightTaskList.stream().filter(t -> tmp.contains(t.getString(RuntimeConfigDefine.TASK_UID))).collect(Collectors.toList());
            }else if(type==0){
                Collection<String/*uid*/> tmp = CollectionUtils.intersection(leftTempList,rightTempList);
                //左右都有,随便拿一边
                difference = leftTaskList.stream().filter(t -> tmp.contains(t.getString(RuntimeConfigDefine.TASK_UID))).collect(Collectors.toList());
            }else{
                throw new IllegalArgumentException();
            }
            if (difference.size()!=0) {
                returnMap.put(key,new ArrayList<>(difference));
            }
        }
        return returnMap;
    }

    private Task getTask(String taskClass) {
        try{
            ClassLoader loader = plugin.getPluginClassLoader(taskClass);
            final ClassLoader currentThreadLoader = plugin.currentThreadLoader();
            Class taskClazz;
            boolean isolationFlag = false;
            if (loader instanceof PluginClassLoader) {
                taskClazz = ((PluginClassLoader) loader).loadClass(taskClass, false);
                isolationFlag = true;
            } else {
                taskClazz = Class.forName(taskClass);
            }
            final Task task = (Task) taskClazz.getDeclaredConstructor().newInstance();
            if (isolationFlag) {
                Plugin.compareAndSwapLoaders(loader);
            }
            Plugin.compareAndSwapLoaders(currentThreadLoader);
            return task;
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

    public class StateMachineService extends ServiceThread {
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                this.waitForRunning(5000);
                try {
                    //empty method
                    Worker.this.maintainConnectorState();
                    //这里会启动task真正执行
                    Worker.this.maintainTaskState();
                } catch (Exception e) {
                    log.error("RebalanceImpl#StateMachineService start connector or task failed", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return StateMachineService.class.getSimpleName();
        }
    }

    public enum TaskType {
        SOURCE,
        SINK,
        DIRECT;
    }

    /**
     * 为了使用map 所以equals方法比较的是TASK_UID
     */
    public static class ConnectKeyValueWrapper{
        private ConnectKeyValue connectKeyValue;
        private ConnectKeyValueWrapper(){}
        public static ConnectKeyValueWrapper wrap(ConnectKeyValue connectKeyValue){
            ConnectKeyValueWrapper connectKeyValueWrapper = new ConnectKeyValueWrapper();
            connectKeyValueWrapper.connectKeyValue = connectKeyValue;
            return connectKeyValueWrapper;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConnectKeyValueWrapper that = (ConnectKeyValueWrapper) o;
            return StringUtils.equals(that.connectKeyValue.getString(RuntimeConfigDefine.TASK_UID),connectKeyValue.getString(RuntimeConfigDefine.TASK_UID));
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectKeyValue.getString(RuntimeConfigDefine.TASK_UID));
        }
    }
}
