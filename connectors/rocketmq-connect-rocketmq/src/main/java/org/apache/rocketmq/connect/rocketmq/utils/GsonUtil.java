package org.apache.rocketmq.connect.rocketmq.utils;

import com.google.gson.Gson;

public class GsonUtil {
    private static final ThreadLocal<Gson> THREAD_LOCAL_GSON = ThreadLocal.withInitial(Gson::new);

    private GsonUtil() {
    }

    public static String toJson(Object obj) {
        return ((Gson)THREAD_LOCAL_GSON.get()).toJson(obj);
    }

    public static <T> T fromJson(String str, Class<T> type) {
        return ((Gson)THREAD_LOCAL_GSON.get()).fromJson(str, type);
    }
}