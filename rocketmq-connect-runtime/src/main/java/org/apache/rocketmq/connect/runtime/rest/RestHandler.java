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

package org.apache.rocketmq.connect.runtime.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import io.javalin.Context;
import io.javalin.Javalin;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.BiFunction;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.connect.runtime.ConnectController;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rest handler to process http request.
 */
public class RestHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    public static final String GET_ALLOCATED_TASK = "/getAllocatedTask";

    private final ConnectController connectController;

    private static final String CONNECTOR_CONFIGS = "connectorConfigs";

    private static final String TASK_CONFIGS = "taskConfigs";

    final CloseableHttpClient httpClient;

    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    public RestHandler(ConnectController connectController) {
        this.connectController = connectController;
        Javalin app = Javalin.create();
        app.enableCaseSensitiveUrls();
        app = app.start(connectController.getConnectConfig().getHttpPort());

        httpClient = HttpClients.createDefault();

        //查看全部connector info以及对应的task info
        app.get("/getConnectorTask", this::getConnectorTask);
        //查看全部connector info
        app.get("/getConnectors", this::getAllConnectors);
        //查看单个connector
        app.get("/getConnectors/:connectorName", this::getConnectors);
        //查看指定connector的配置信息
        app.get("/getConnectorTask/:connectorName", this::handleQueryConnectorConfig);
        //查看指定connector状态
        app.get("/getConnectorsStatus/:connectorName", this::handleQueryConnectorStatus);
        //动态开启message消息log打印或者关闭打印
        app.get("/logMsgDetail/:trueOrFalse", this::handleLogMsg);

        //查看集群信息
        app.get("/getClusterInfo", this::getClusterInfo);
        //查看分配给自己的任务的状态
        app.get(GET_ALLOCATED_TASK, this::getAllocatedTask);
        //查看所有task的状态，内部会调用其他节点的getAllocatedTask
        app.get("/getAllTask/:byWorker", this::getAllTask);
        //插件重新加载
        app.get("/plugin/reload", this::reloadPlugins);


        //新增connector
        app.get("/connectors/create/:connectorName", this::handleCreateConnector);
        app.get("/connectors/update/:connectorName", this::handleUpdateConnector);
        app.get("/connectors/taskNum/:connectorName/:taskNum", this::handleTaskNum);

        //启用
        app.get("/connectors/all/enable", this::handleEnableAllConnector);
        //暂时禁用,配置文件读出来也不会去执行,同时停止该connector对应的Task. task的stop是通过把connector禁用，这样maintainTaskStat时候就不会分配该task，达到维护task的目的
        app.get("/connectors/all/disable", this::handleDisableAllConnector);
        //删除
        app.get("/connectors/all/remove", this::handleRemoveAllConnector);

        //启用
        app.get("/connectors/single/:connectorName/enable", this::handleEnableConnector);
        //暂时禁用,配置文件读出来也不会去执行,同时停止该connector对应的Task
        app.get("/connectors/single/:connectorName/disable", this::handleDisableConnector);
        //删除
        app.get("/connectors/single/:connectorName/remove", this::handleRemoveConnector);

    }

    private void getAllTask(Context context) {
        boolean byWorker = Boolean.parseBoolean(context.pathParam("byWorker"));

        final ConsumerConnection consumerConnection = connectController.getClusterManagementService().fetchConsumerConnection();
        Map map = new HashMap<>();
        for (Connection connection : consumerConnection.getConnectionSet()) {
            final String clientId = connection.getClientId();
            final String[] split = clientId.split("@");
            String restPort = null;

            String clientAddr = connection.getClientAddr();
            clientAddr = clientAddr.split(":")[0];

            if (split.length>1) {
                final String pidAndPort = split[1];
                if (pidAndPort!=null) {
                    final String[] strings = pidAndPort.split("#");
                    if (strings.length>1) {
                        restPort = strings[1];
                    }
                }
            }
            if (restPort == null || clientAddr == null) {
                log.error("consumerID format is wrong and will ignore this consumer");
                continue;
            }

            HttpUriRequest httpUriRequest  = new HttpGet("http://"+clientAddr+":"+restPort+GET_ALLOCATED_TASK);
            CloseableHttpResponse execute = null;
            try {
                execute = httpClient.execute(httpUriRequest);
                final HttpEntity entity = execute.getEntity();
                final String rs = EntityUtils.toString(entity, "UTF-8");
                final HashMap rsMap = JSON.parseObject(rs, HashMap.class);
                if (byWorker) {
                    map.put(connection.getClientId(),rsMap);
                }else{
                    for (Object key : rsMap.keySet()) {
                        map.merge(key, rsMap.get(key), (oldV, newV) -> {
                            final boolean all = ((Collection) oldV).addAll(((Collection) newV));
                            return oldV;
                        });
                    }
                }
            }catch (IOException e) {
                log.error("",e);
                context.result("failed");
                return;
            }finally {
                if (execute!=null) {
                    try {
                        execute.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        context.result(JSON.toJSONString(map, SerializerFeature.PrettyFormat));
        return;
    }

    private void handleLogMsg(Context context) {
        String trueOrFalse = context.pathParam("trueOrFalse");
        try{
            boolean b = Boolean.parseBoolean(trueOrFalse);
            ConnectConfig.setLogMsgDetail(b);
            context.result("succcess");
        }catch (Exception ex){
            context.result("failed,param must be true or false !");
        }
    }


    /**
     * 查询整个系统运行的全部的connectors
     * @param context
     */
    private void getAllConnectors(Context context) {
        Map<String, ConnectKeyValue> keyValueMap = connectController.getConfigManagementService().getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST);
        context.result(JSON.toJSONString(keyValueMap, SerializerFeature.PrettyFormat,SerializerFeature.WriteDateUseDateFormat));
    }
    private void getConnectors(Context context) {
        String connectorName = context.pathParam("connectorName");
        Map<String, ConnectKeyValue> keyValueMap = connectController.getConfigManagementService().getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST);
        context.result(JSON.toJSONString(keyValueMap.get(connectorName), SerializerFeature.PrettyFormat,SerializerFeature.WriteDateUseDateFormat));
    }

    private void getAllocatedTask(Context context) {
        Map<String, Object> formatter = new HashMap<>();
        formatter.put("pendingTasks", convertWorkerTaskToString(connectController.getWorker().getPendingTasks()));
        formatter.put("runningTasks",  convertWorkerTaskToString(connectController.getWorker().getWorkingTasks()));
        formatter.put("stoppingTasks",  convertWorkerTaskToString(connectController.getWorker().getStoppingTasks()));
        formatter.put("stoppedTasks",  convertWorkerTaskToString(connectController.getWorker().getStoppedTasks()));
        formatter.put("errorTasks",  convertWorkerTaskToString(connectController.getWorker().getErrorTasks()));

        context.result(JSON.toJSONString(formatter, SerializerFeature.PrettyFormat,SerializerFeature.WriteDateUseDateFormat));
    }

    private void getConnectorTask(Context context) {
        Map<String, ConnectKeyValue> connectorConfigs = connectController.getConfigManagementService().getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST);
        Map<String, List<ConnectKeyValue>> taskConfigs = connectController.getConfigManagementService().getTaskConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST);

        Map<String, Map> formatter = new HashMap<>();
        formatter.put(CONNECTOR_CONFIGS, connectorConfigs);
        formatter.put(TASK_CONFIGS, taskConfigs);

        context.result(JSON.toJSONString(formatter, SerializerFeature.PrettyFormat,SerializerFeature.WriteDateUseDateFormat));
    }

    private void getClusterInfo(Context context) {
        context.result(JSON.toJSONString(connectController.getClusterManagementService().getAllAliveWorkers(),SerializerFeature.PrettyFormat));
    }

    private void handleCreateConnector(Context context) {
        String connectorName = context.pathParam("connectorName");
        String arg = context.req.getParameter("config");
        if (arg == null) {
            context.result("failed! query param 'config' is required ");
            return;
        }
        log.info("handle new Connector Config: {}", arg);
        //mz 把rest请求中的config请求体解析为map然后转换为ConnectKeyValue格式
        Map keyValue = JSON.parseObject(arg, Map.class);
        ConnectKeyValue configs = new ConnectKeyValue();
        for (Object key : keyValue.keySet()) {
            configs.put((String) key, keyValue.get(key).toString());
        }
        try {
            //mz 创建一个新的connector实例
            connectController.getConfigManagementService().putNewConnectorConfig(connectorName, configs);
            context.result("success");
        } catch (Exception e) {
            log.error("Handle createConnector error .", e);
            context.result("failed");
        }
    }

    private void handleTaskNum(Context context){
        String connectorName = context.pathParam("connectorName");
        String taskNum = context.pathParam("taskNum");
        if(taskNum==null || taskNum.trim().length()==0){
            context.result("failed cuz taskNum can`t be null or zero");
        }
        try{
            int i = Integer.parseInt(taskNum);
            if (i<=0) {
                context.result("failed cuz taskNum can`t <= 0");
            }
        }catch (Exception ex){
            context.result("failed cuz taskNum is not a number");
        }

        try {
            String str = connectController.getConfigManagementService().dynamicUpdateTaskNum(connectorName, Integer.valueOf(taskNum));
            context.result(str==null || str.length()==0?"success":str);
        } catch (Exception e) {
            log.error("", e);
            context.result("failed:"+e.getMessage());
        }
    }

    private void handleUpdateConnector(Context context) {
        String connectorName = context.pathParam("connectorName");
        String arg = context.req.getParameter("config");
        if (arg == null) {
            context.result("failed! query param 'config' is required ");
            return;
        }
        log.info("handle new Connector Config: {}", arg);
        //mz 把rest请求中的config请求体解析为map然后转换为ConnectKeyValue格式
        Map keyValue = JSON.parseObject(arg, Map.class);
        ConnectKeyValue configs = new ConnectKeyValue();
        for (Object key : keyValue.keySet()) {
            configs.put((String) key, keyValue.get(key).toString());
        }
        try {
            //mz 创建一个新的connector实例
            connectController.getConfigManagementService().updateConnectorConfig(connectorName, configs);
            context.result("success");
        } catch (Exception e) {
            log.error("", e);
            context.result("failed:"+e.getMessage());
        }
    }

    private void handleQueryConnectorConfig(Context context) {

        String connectorName = context.pathParam("connectorName");

        Map<String, ConnectKeyValue> connectorConfigs = connectController.getConfigManagementService().getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST);
        Map<String, List<ConnectKeyValue>> taskConfigs = connectController.getConfigManagementService().getTaskConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST);
        Map<Map<String, ConnectKeyValue>, Map<String, List<ConnectKeyValue>>> map = new HashMap<>();

        ConnectKeyValue connectKeyValue = connectorConfigs.get(connectorName);
        if (connectKeyValue==null) {
            context.result("can`t find this connector");
            return;
        }

        Map map2 = new HashMap();
        map2.put("connector", connectKeyValue);
        map2.put("tasks", taskConfigs.get(connectorName));

        context.result(JSON.toJSONString(map2,SerializerFeature.PrettyFormat));
    }

    private void handleQueryConnectorStatus(Context context) {

        String connectorName = context.pathParam("connectorName");
        Map<String, ConnectKeyValue> connectorConfigs = connectController.getConfigManagementService().getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST);

        if (connectorConfigs.containsKey(connectorName)) {
            int status = connectorConfigs.get(connectorName).getInt(RuntimeConfigDefine.CONFIG_STATUS);
            String rs = "";
            if (RuntimeConfigDefine.CONFIG_STATUS_ENABLE == status) {
                rs = "enable";
            } else if (RuntimeConfigDefine.CONFIG_STATUS_DISABLE == status) {
                rs = "disable";
            }else if (RuntimeConfigDefine.CONFIG_STATUS_REMOVE == status) {
                rs = "remove";
            }else{
                rs = "unknown status";
            }
            context.result(rs);
        } else {
            context.result("can`t find this connector !");
        }
    }

    //启用
    private void handleEnableAllConnector(Context context) {
        try {
            Map<String, ConnectKeyValue> connectorConfigs = connectController.getConfigManagementService().getConnectorConfigs(RuntimeConfigDefine.CONFIG_DISABLE_LST);
            StringBuilder msg = new StringBuilder("success");
            for (String connector : connectorConfigs.keySet()) {
                try{
                    connectController.getConfigManagementService().enableConnectorConfig(connector);
                }catch (Exception ex){
                    msg.append(String.format("failed %s:",connector)+ex.getMessage()).append("\n");
                }
            }
            context.result(msg.toString());
        } catch (Exception e) {
            log.error("", e);
            context.result("failed:"+e.getMessage());
        }
    }
    //禁用
    private void handleDisableAllConnector(Context context) {
        try {
            Map<String, ConnectKeyValue> connectorConfigs = connectController.getConfigManagementService().getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_LST);
            StringBuilder msg = new StringBuilder("success");
            for (String connector : connectorConfigs.keySet()) {
                try{
                    connectController.getConfigManagementService().disableConnectorConfig(connector);
                }catch (Exception ex){
                    msg.append(String.format("failed %s:",connector)+ex.getMessage()).append("\n");
                }
            }
            context.result(msg.toString());
        } catch (Exception e) {
            log.error("", e);
            context.result("failed:"+e.getMessage());
        }
    }
    //删除
    private void handleRemoveAllConnector(Context context) {
        try {
            Map<String, ConnectKeyValue> connectorConfigs = connectController.getConfigManagementService().getConnectorConfigs(RuntimeConfigDefine.CONFIG_ENABLE_DISABLE_LST);
            StringBuilder msg = new StringBuilder("success");
            for (String connector : connectorConfigs.keySet()) {
                try{
                    connectController.getConfigManagementService().removeConnectorConfig(connector);
                }catch (Exception ex){
                    msg.append(String.format("failed %s:",connector)+ex.getMessage()).append("\n");
                }
            }
            context.result(msg.toString());
        } catch (Exception e) {
            log.error("", e);
            context.result("failed:"+e.getMessage());
        }
    }


    //启用
    private void handleEnableConnector(Context context) {
        try {
            String connectorName = context.pathParam("connectorName");
            try{
                connectController.getConfigManagementService().enableConnectorConfig(connectorName);
                context.result("success");
            }catch (Exception ex){
                context.result("failed:"+ex.getMessage());
            }
        }catch (Exception e) {
            log.error("", e);
            context.result("failed:"+e.getMessage());
        }
    }
    //禁用
    private void handleDisableConnector(Context context) {
        try {
            String connectorName = context.pathParam("connectorName");
            connectController.getConfigManagementService().disableConnectorConfig(connectorName);
            context.result("succcess");
        } catch (Exception e) {
            log.error("", e);
            context.result("failed:"+e.getMessage());
        }
    }
    //删除
    private void handleRemoveConnector(Context context) {
        try {
            String connectorName = context.pathParam("connectorName");
            connectController.getConfigManagementService().removeConnectorConfig(connectorName);
            context.result("succcess");
        } catch (Exception e) {
            log.error("", e);
            context.result("failed:"+e.getMessage());
        }
    }



    private Set<Object> convertWorkerTaskToString(Set<WorkerTask> tasks) {
        Set<Object> result = new HashSet<>();
        for (WorkerTask task : tasks) {
            result.add(((WorkerTask) task).getJsonObject());
        }
        return result;
    }

    private void reloadPlugins(Context context) {
        connectController.getConfigManagementService().getPlugin().initPlugin();
        context.result("success");
    }
}
