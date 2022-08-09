package com.product.data.publisher.configuration;

import lombok.Builder;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Builder
@Configuration
public class KafkaConfig {

    private String applicationId;
    private String bootstrapServers;
    private String customerDetailsTopic;
    private String balanceDetailsTopic;
    private String customerBalanceDetailsTopic;
    @Autowired
    public KafkaConfig(@Value("${kafka.applicationId}") String applicationId,
                       @Value("${kafka.bootStrap.Servers}") String bootstrapServers,
                       @Value("${kafka.customerDetailsTopic}") String customerDetailsTopic,
                       @Value("${kafka.balanceDetailsTopic}") String balanceDetailsTopic,
                       @Value("${kafka.customerBalanceDetailsTopic}") String customerBalanceDetailsTopic) {
        this.applicationId = applicationId;
        this.bootstrapServers = bootstrapServers;
        this.customerDetailsTopic = customerDetailsTopic;
        this.balanceDetailsTopic = balanceDetailsTopic;
        this.customerBalanceDetailsTopic = customerBalanceDetailsTopic;
    }
}
