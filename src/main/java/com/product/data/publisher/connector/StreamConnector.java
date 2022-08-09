package com.product.data.publisher.connector;

import com.product.data.publisher.configuration.KafkaConfig;
import com.product.data.publisher.model.BalanceDetails;
import com.product.data.publisher.model.CustomerBalanceDetails;
import com.product.data.publisher.model.CustomerDetails;
import com.product.data.publisher.serde.KafkaDeserializer;
import com.product.data.publisher.serde.KafkaSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

public class StreamConnector {

    private final StreamsBuilder customerDetailsStreamBuilder = new StreamsBuilder();
    private final StreamsBuilder balanceDetailsStreamBuilder = new StreamsBuilder();
    private Topology customerTopology;
    private Topology balanceTopology;
    private KStream<String, CustomerDetails> customerDetailsStream;
    private KStream<String, BalanceDetails> balanceDetailsStream;

    private KStream<String, CustomerBalanceDetails> customerBalanceDetailsKStream;
    private final KafkaConfig kafkaConfig;

    public StreamConnector(KafkaConfig kafkaConfig){
        this.kafkaConfig = kafkaConfig;
        this.buildPipeline();
    }
    public void buildPipeline()
    {
        getKStreamCustomerDetails();
        getKStreamBalanceDetails();

        buildCustomerDetailsTopologyAndStartStream();
        buildBalanceDetailsTopologyAndStartStream();
        doJoinOnStream();
    }
    void buildCustomerDetailsTopologyAndStartStream()
    {
        this.customerTopology=this.customerDetailsStreamBuilder.build(getKafkaStreamProperties(null,CustomerDetails.class));
        KafkaStreams customerKafkaStreams = new KafkaStreams(this.customerTopology,getKafkaStreamProperties(null,CustomerDetails.class));
        customerKafkaStreams.start();
    }

    void buildBalanceDetailsTopologyAndStartStream()
    {
        this.balanceTopology=this.balanceDetailsStreamBuilder.build(getKafkaStreamProperties(null,BalanceDetails.class));
        KafkaStreams balanceKafkaStreams = new KafkaStreams(this.customerTopology,getKafkaStreamProperties(null,BalanceDetails.class));
        balanceKafkaStreams.start();

    }
    synchronized KStream<String, CustomerDetails> getKStreamCustomerDetails(){
        if( Objects.isNull(this.customerDetailsStream) ){
            this.customerDetailsStream = this.customerDetailsStreamBuilder
                    .stream(getKafkaConfig().getCustomerDetailsTopic(), Consumed.with(Serdes.String(), getKafkaSerdes(CustomerDetails.class)));
        }
        return this.customerDetailsStream;
    }

    synchronized KStream<String, BalanceDetails> getKStreamBalanceDetails(){
        if( Objects.isNull(this.balanceDetailsStream) ){
            this.balanceDetailsStream = this.balanceDetailsStreamBuilder
                    .stream(getKafkaConfig().getBalanceDetailsTopic(), Consumed.with(Serdes.String(),getKafkaSerdes(BalanceDetails.class)));
        }

        return this.balanceDetailsStream;
    }
   public void doJoinOnStream()
    {
        this.customerBalanceDetailsKStream = this.customerDetailsStream.
                join(this.balanceDetailsStream,(customerDetails, balanceDetails)->
                        new CustomerBalanceDetails(customerDetails.getCustomerId(),customerDetails.getPhoneNumber(),balanceDetails.getAccountId(),balanceDetails.getBalance()),
                        JoinWindows.of(Duration.ofMinutes(5)));
        this.customerBalanceDetailsKStream.filter((k,v)-> Objects.nonNull(v)).
                to(this.kafkaConfig.getCustomerBalanceDetailsTopic(),
               Produced.with(getKafkaSerdes(String.class),getKafkaSerdes(CustomerBalanceDetails.class)));
    }
    public Properties getKafkaStreamProperties(Properties newProps,Class valueClassName){

        Properties configurationProperties = new Properties();

        configurationProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, getKafkaConfig().getApplicationId()+valueClassName.getName());
        configurationProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConfig().getBootstrapServers());

      /*  configurationProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configurationProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());*/

        configurationProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerializer.class.getName());
        configurationProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, getKafkaSerdes(valueClassName).serializer().getClass().getName());

        if(Objects.nonNull(newProps)){
            configurationProperties.putAll(newProps);
        }

        return configurationProperties;
    }
    public Topology getCustomerTopology() {
        return this.customerTopology;
    }

    public void setCustomerTopology(Topology customerTopology) {
        this.customerTopology = customerTopology;
    }

    public Topology getBalanceTopology() {
        return this.balanceTopology;
    }

    public void setBalanceTopology(Topology balanceTopology) {
        this.balanceTopology = balanceTopology;
    }

    public KafkaConfig getKafkaConfig() {
        return this.kafkaConfig;
    }
    public static <T> Serde<T> getKafkaSerdes(Class <T> inputClass)
    {
        return Serdes.serdeFrom(new KafkaSerializer<>(inputClass),new KafkaDeserializer<>(inputClass));
    }
}
