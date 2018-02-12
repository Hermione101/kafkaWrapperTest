package com.sample.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import com.sample.model.kafkaMessage;

/*
This Util class  to connect to kafka and consume the messages and serialize to object .
 */
public class kafkaConsumerUtil {
    public Consumer<String, String> createConsumer() {

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "kafkaserver:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "TestConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("qa.testtopic1"));
        return consumer;
    }

    public void runConsumer() throws InterruptedException {
        final Consumer<String, String> consumer = createConsumer();

        final int giveUp = 10;   int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                                if (noRecordsCount > giveUp) break;
                else continue;
            }

            System.out.println("consumer records" + consumerRecords.count());
            consumerRecords.forEach(record -> {

                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
                try{
                    kafkaMessage  kmessg = new kafkaMessage();
                    ObjectMapper obj = new ObjectMapper();
                    kmessg = obj.readValue(record.value(),kafkaMessage.class);
                    System.out.println("kafkamessgID" + kmessg.getName());
                    System.out.println("kafkamessgID" + kmessg.getCity());
                }
                catch(Exception e){
                    System.out.print("Exeception "+ e);
                }
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }


}
