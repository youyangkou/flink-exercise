package com.kouyy.flink.connector;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

/**
 * @author kouyouyang
 * @date 2019-08-06 17:44
 * kafka中kv格式反序列化类
 * JsonDeserializationSchema 使用jackson反序列化json格式消息，并返回ObjectNode，可以使用.get(“property”)方法来访问相应字段。
 */
public class KvDeserializationSchema<T> implements KeyedDeserializationSchema<T> {
    private Class<T> clazz;

    public KvDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        return JSON.parseObject(new String(message), clazz);
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(clazz);
    }
}
