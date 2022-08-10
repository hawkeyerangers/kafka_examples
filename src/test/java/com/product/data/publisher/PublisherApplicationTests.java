package com.product.data.publisher;

import com.product.data.publisher.configuration.ApplicationConfig;
import com.product.data.publisher.model.BalanceDetails;
import com.product.data.publisher.model.CustomerBalanceDetails;
import com.product.data.publisher.model.CustomerDetails;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Random;

import static com.product.data.publisher.connector.StreamConnector.getKafkaSerdes;


@SpringBootTest
class PublisherApplicationTests {

    @Autowired
    ApplicationConfig applicationConfig;

    @Test
    public void testJoin()
    {

/*        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");*/
        Topology customerTopology = applicationConfig.getStreamConnector().getCustomerTopology();
        TopologyTestDriver testCustomerDriver = new TopologyTestDriver(customerTopology);
        TopologyTestDriver testBalanceDriver = new TopologyTestDriver(applicationConfig.getStreamConnector().getBalanceTopology());
        TestInputTopic<String, CustomerDetails> customerDetailsTestInputTopic =
                testCustomerDriver.createInputTopic(applicationConfig.getStreamConnector().getKafkaConfig().getCustomerDetailsTopic(),
                Serdes.String().serializer(), getKafkaSerdes(CustomerDetails.class).serializer());

        TestInputTopic<String, BalanceDetails> balanceDetailsTestInputTopic =
                testBalanceDriver.createInputTopic(applicationConfig.getStreamConnector().getKafkaConfig().getBalanceDetailsTopic(),
                        Serdes.String().serializer(), getKafkaSerdes(BalanceDetails.class).serializer());
        for(int i=0;i<100;i++)
        {
            CustomerDetails customerDetails = getCustomerDetails("ACT-"+i);
            BalanceDetails balanceDetails = getBalanceDetails("ACT-"+i);
            customerDetailsTestInputTopic.pipeInput(customerDetails.getAccountId(), customerDetails);
            balanceDetailsTestInputTopic.pipeInput(balanceDetails.getAccountId(),balanceDetails);
        }
        TestOutputTopic<String, CustomerBalanceDetails> outputTopic = testCustomerDriver.createOutputTopic(
                applicationConfig.getStreamConnector().getKafkaConfig().getCustomerBalanceDetailsTopic(),
                Serdes.String().deserializer(), getKafkaSerdes(CustomerBalanceDetails.class).deserializer());
        List<TestRecord<String, CustomerBalanceDetails>> outList= outputTopic.readRecordsToList();
        Assertions.assertEquals(100,outList.size());
    }

    public CustomerDetails getCustomerDetails(String accountId)
    {
        return CustomerDetails.builder().
                accountId(accountId).
                name("Finland").
                phoneNumber("9825641201").
                customerId(accountId).
                build();
    }

    public BalanceDetails getBalanceDetails(String accountId)
    {
        return BalanceDetails.builder().accountId(accountId).
                balanceId(accountId).
                balance(new Random().nextFloat()).
                build();
    }

    public void sendKafkaMessage()
    {

    }
}
