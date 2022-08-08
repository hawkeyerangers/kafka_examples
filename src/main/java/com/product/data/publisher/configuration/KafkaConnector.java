package com.product.data.publisher.configuration;

import com.product.data.publisher.model.BalanceDetails;
import com.product.data.publisher.model.CustomerBalanceDetails;
import com.product.data.publisher.model.CustomerDetails;
import com.product.data.publisher.serde.KafkaDeserializer;
import com.product.data.publisher.serde.KafkaSerializer;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

@Data
public class KafkaConnector {

    private final StreamsBuilder customerDetailsStreamBuilder = new StreamsBuilder();
    private final StreamsBuilder balanceDetailsStreamBuilder = new StreamsBuilder();
    private Topology customerTopology;
    private Topology balanceTopology;
    private KStream<String, CustomerDetails> customerDetailsStream;
    private KStream<String, BalanceDetails> balanceDetailsStream;
    private final ApplicationConfig applicationConfig;

    public KafkaConnector(ApplicationConfig applicationConfig){
        this.applicationConfig = applicationConfig;
    }

    synchronized KStream<String, CustomerDetails> getKStreamCustomerDetails(){
        if( Objects.isNull(customerDetailsStream) ){
            customerDetailsStream = customerDetailsStreamBuilder
                    .stream(getApplicationConfig().getCustomerDetailsTopic(), Consumed.with(Serdes.String(), getKafkaSerdes(CustomerDetails.class)));
        }
        return customerDetailsStream;
    }

    synchronized KStream<String, BalanceDetails> getKStreamBalanceDetails(){
        if( Objects.isNull(balanceDetailsStream) ){
            balanceDetailsStream = balanceDetailsStreamBuilder
                    .stream(getApplicationConfig().getBalanceDetailsTopic(), Consumed.with(Serdes.String(),getKafkaSerdes(BalanceDetails.class)));
        }

        return balanceDetailsStream;
    }
    void doJoinOnStream()
    {
        KStream<String, CustomerBalanceDetails> customerBalanceDetailsKStream = customerDetailsStream.
                join(balanceDetailsStream,(customerDetails, balanceDetails)->new CustomerBalanceDetails(customerDetails.getCustomerId(),customerDetails.getPhoneNumber(),balanceDetails.getAccountId(),balanceDetails.getBalance()),
                        JoinWindows.of(Duration.ofMinutes(5)));
    }
    public Properties getKafkaStreamProperties(Properties newProps,Class valueClassName){

        Properties configurationProperties = new Properties();

        configurationProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationConfig().getApplicationId());
        configurationProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getApplicationConfig().getBootstrapServers());

      /*  configurationProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configurationProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());*/

        configurationProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerializer.class.getName());
        configurationProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, getKafkaSerdes(valueClassName).serializer().getClass().getName());

        if(Objects.nonNull(newProps)){
            configurationProperties.putAll(newProps);
        }

        return configurationProperties;
    }
    public static <T> Serde<T> getKafkaSerdes(Class <T> inputClass)
    {
        return Serdes.serdeFrom(new KafkaSerializer<>(inputClass),new KafkaDeserializer<>(inputClass));
    }
}
