package com.udemy.kafkacourse.kafkaapp;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

	private static KafkaConsumer<String, String> consumer;

	public static void main(String[] args) {
		
	
	 {
		 
			final Logger log = LoggerFactory.getLogger(Consumer.class);
			String bootstrapServers = "127.0.0.1:9092";
			String topic = "abc";
			String consumerGroup = "consumer_group_five";
	    	//creting properties
	    	Properties prop = new Properties();
	    	prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    	prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    	prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    	prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
	    	prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
	    	

	    	consumer = new KafkaConsumer<String, String>(prop);
	    	
	    	consumer.subscribe(Arrays.asList(topic));
	    	
			while (true) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : consumerRecords) {
					log.info("  "+ "KEY:" + record.key() + "  "+ "VALUE:" + record.value() + "  "+ "partitions:"
							+ record.partition() + "  "+ "offset" + record.offset() + "  "+ record.timestamp());
				}
			}
	    }
	
	}
}
