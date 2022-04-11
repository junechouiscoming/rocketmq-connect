package org.apache.rocketmq.connect.runtime.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AllocateResultConfigs {
    private Map<String, List<ConnectKeyValue>> taskConfigs = new HashMap<>();

    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {
        return taskConfigs;
    }

    public void setTaskConfigs(Map<String, List<ConnectKeyValue>> taskConfigs) {
        this.taskConfigs = taskConfigs;
    }

    @Override
    public String toString() {
        return "AllocateResultConfigs{" +"taskConfigs=" + JSON.toJSONString(taskConfigs, SerializerFeature.PrettyFormat) +'}';
    }
}
