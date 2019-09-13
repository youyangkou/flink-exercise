package com.kouyy.flink.connector;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

/**
 * @author kouyouyang
 * @date 2019-08-06 17:44
 * Kafka反序列化类, new String(bytes) 是json格式
 */
public class ObjectDeserializationSchema<T> implements DeserializationSchema<T> {
    private Class<T> clazz;

    public ObjectDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        //确保 new String(bytes) 是json 格式，如果不是，请自行解析
        return JSON.parseObject(new String(bytes), clazz);
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
