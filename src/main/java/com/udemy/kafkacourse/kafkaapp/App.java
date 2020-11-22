package com.udemy.kafkacourse.kafkaapp;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    
    {
    	String bootstrapServers = "127.0.0.1:9092";
    	//creting properties
    	Properties prop = new Properties();
    	prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    	prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	//create kafka producer
    	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
    	
    	ProducerRecord<String, String> record = new ProducerRecord<String, String>("abc", "by sabaree to test log-1");
    	//send data -asynchronous
    	producer.send(record);
    	producer.flush();
    	producer.close();
    }
    
}
