package com.udemy.kafkacourse.kafkaapp;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerWithThreads {

	final Logger log = LoggerFactory.getLogger(KafkaConsumerWithThreads.class);

	public static void main(String[] args) {

		new KafkaConsumerWithThreads().run();
	}

	private KafkaConsumerWithThreads() {

	}

	private void run() {
		String bootstrapServers = "127.0.0.1:9092";
		String topic = "abc";
		String consumerGroup = "consumer_group_ten";
		CountDownLatch latch = new CountDownLatch(1);

		log.info("creating the consumer threads");
		Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, consumerGroup, topic, latch);
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("caught shutdown hook");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			log.info("Appliction has exited");
		}

		));
		try {
			latch.await();
		} catch (InterruptedException e) {

			log.error("Application Got Interuptted",e);
			e.printStackTrace();
		} finally {
			log.info("Appliction is closing");
		}
	}

	public class ConsumerRunnable implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerRunnable(String bootstrapServers, String consumerGroup, String topic, CountDownLatch latch) {
			this.latch = latch;
			Properties prop = new Properties();
			prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
			prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			consumer = new KafkaConsumer<String, String>(prop);
			consumer.subscribe(Arrays.asList(topic));

		}

		@Override
		public void run() {
			try {

				while (true) {
					ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

					for (ConsumerRecord<String, String> record : consumerRecords) {
						log.info("  " + "KEY:" + record.key() + "  " + "VALUE:" + record.value() + "  " + "partitions:"
								+ record.partition() + "  " + "offset" + record.offset() + "  " + record.timestamp());

					}
				}
			} catch (WakeupException e) {
				// TODO: handle exception
				log.info("Received Shut Down Signal !",e);
			} finally {
				consumer.close();
				// tell main code we are done with consumer
				latch.countDown();
			}
		}

		public void shutdown() {
			consumer.wakeup();

		}

	}

}
