package com.product.data.publisher.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class KafkaDeserializer<T> implements Deserializer<T> {
    ObjectMapper objectMapper = new ObjectMapper();
    Class<T> typeOfClass;
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public KafkaDeserializer(Class<T> typeOfClass) {
        this.typeOfClass = typeOfClass;
    }

    public KafkaDeserializer() {
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        Object obj=null;
        try {
            obj=objectMapper.readValue(bytes,typeOfClass);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return (T) obj;
    }
}
