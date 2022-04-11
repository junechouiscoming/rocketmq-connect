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

package org.apache.rocketmq.connect.runtime.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.openmessaging.connector.api.data.Converter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.utils.FileAndPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File based Key value store.
 *
 * @param <K>
 * @param <V>
 */
public class FileBaseKeyValueStore<K, V> extends MemoryBasedKeyValueStore<K, V> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private String configFilePath;

    private TypeReference typeReference;

    public FileBaseKeyValueStore(String configFilePath,TypeReference typeReference) {
        super();
        this.configFilePath = configFilePath;
        this.typeReference = typeReference;
    }


    protected String encode(Map<K,V> data) {
        return JSON.toJSONString(data, SerializerFeature.PrettyFormat);
    }

    protected Map<K,V> decode(String jsonString){
        return (Map)JSON.parseObject(jsonString, typeReference);
    }

    @Override
    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath;
            String jsonString = FileAndPropertyUtil.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                Map<K, V> kvMap = this.decode(jsonString);
                this.data.putAll(kvMap);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath;
            String jsonString = FileAndPropertyUtil.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                Map<K, V> kvMap = this.decode(jsonString);
                this.data.putAll(kvMap);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    @Override
    public void persist() {
        String jsonString = this.encode(this.data);
        if (jsonString != null) {
            String fileName = this.configFilePath;
            try {
                FileAndPropertyUtil.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }
}
