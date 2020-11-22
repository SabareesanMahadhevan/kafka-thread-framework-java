package com.udemy.kafkacourse.kafkaapp;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeys {

	
	public static void main(String[] args) {
		
	
	final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class); 
	
	for(int i=0;i<10;i++) {
	String bootstrapServers = "127.0.0.1:9092";
	String topic = "abc";
	String keys = "key_"+Integer.toString(i);
	String message = "value can be different";	
	//creting properties
	Properties prop = new Properties();
	prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	
	//create kafka producer
	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
	
	ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,keys, message+"-"+Integer.toString(i)) ;
	
	log.info("KEYS TESTING WITH PARTIONS"+keys);
	//send data -asynchronous
	producer.send(record,new Callback() {
		
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			// TODO Auto-generated method stub
			if(exception==null) {
				
				log.info("\n"+"topic" + metadata.topic() + "\n" + "partion" + metadata.partition() + "\n" + "offset"
						+ metadata.offset() + "\n" + metadata.timestamp());
				
			}else {
				log.error("producer with error"+exception);
			}
			
		}
	});
	
	
	producer.flush();
	producer.close();
	}
}
}
	

