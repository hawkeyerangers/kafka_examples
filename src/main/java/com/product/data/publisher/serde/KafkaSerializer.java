package com.product.data.publisher.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaSerializer<T> implements Serializer <T> {

    ObjectMapper objectMapper = new ObjectMapper();
    Class<T> typeOfClass;
    public KafkaSerializer() {
    }

    public KafkaSerializer(Class<T> typeOfClass) {
        this.typeOfClass = typeOfClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, T t) {
        byte[] serialisedVersion= null;
        try {
            serialisedVersion = objectMapper.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return serialisedVersion;
    }



    @Override
    public void close() {

    }
}
