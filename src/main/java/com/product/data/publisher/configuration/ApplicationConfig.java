package com.product.data.publisher.configuration;

import com.product.data.publisher.connector.StreamConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfig {
    StreamConnector streamConnector;

    @Autowired
    public ApplicationConfig(KafkaConfig kafkaConfig) {
        this.streamConnector = new StreamConnector(kafkaConfig);
    }

    public StreamConnector getStreamConnector() {
        return streamConnector;
    }
}
