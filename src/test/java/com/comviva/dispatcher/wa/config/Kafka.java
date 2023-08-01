package com.comviva.dispatcher.wa.config;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;

import java.util.Collection;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.SendValues;

public class Kafka {

	private EmbeddedKafkaCluster kafka;

	public void setupKafka() throws InterruptedException {
		kafka = provisionWith(defaultClusterConfig());
		kafka.start();
		System.err.println("******************** Started Kafka Server.......");

	}

	public void tearDownKafka() {
		kafka.stop();
		System.err.println("******************** Stopped Kafka Server.......");
	}

	public void send(String topic, Collection<String> values) throws InterruptedException {
		kafka.send(SendValues.to(topic, values));
	}

	public void observe(String topic) throws InterruptedException {
		kafka.observe(ObserveKeyValues.on(topic, 1));
	}
}
