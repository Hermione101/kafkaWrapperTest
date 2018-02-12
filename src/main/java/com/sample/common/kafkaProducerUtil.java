package com.sample.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/*
This Util class is for connectiong to Kafka and producing the messages to kafka
 */
public class kafkaProducerUtil {

    public  KafkaProducer<String,String >  createProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "http://kafkaserver:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                                "TestProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }

    public void sendMesgToTopic(String  msg,String Topic){

        KafkaProducer<String, String> kafkaProducer = createProducer();

        try{
            kafkaProducer.send(new ProducerRecord<String, String>(Topic,"key",msg));
            kafkaProducer.close();

        }catch ( Exception e){
            System.out.println(" exeception for writing the message to Kafka");
        }

        finally {
            kafkaProducer.close();
        }
    }
}
