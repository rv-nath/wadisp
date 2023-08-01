package com.comviva.dispatcher.wa.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.comviva.dispatcher.wa.WADispatcher;

public class TopicPublisher extends TimerTask {
	private static final Logger LOGGER = LogManager.getLogger(TopicPublisher.class);

	private static TopicPublisher topicPublisher = null;
	public static AdminClient adminClient = null;
	public static AdminClient adminClientForDelete = null;
	long lastLogAnalyseTIme = System.currentTimeMillis();

	public TopicPublisher(Properties props) {
		props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
		adminClient = AdminClient.create(props);
	}

	public static TopicPublisher getInstance(Properties props) {
		if (topicPublisher == null) {
			topicPublisher = new TopicPublisher(props);
			final Timer timer = new Timer("WA-MANAGER-TH", true);
			timer.scheduleAtFixedRate(topicPublisher, (1000 - (System.currentTimeMillis() % 1000)), 30L * 1000);
		}
		return topicPublisher;
	}

	@Override
	public void run() {
		try {
			ListTopicsResult topicResults = adminClient.listTopics();
			List<TopicListing> topicList = new ArrayList<>(topicResults.listings().get());

			for (TopicListing topic : topicList) {
				if (topic.name().contains("WHATSAPP_MT_NON_BULK") || topic.name().contains("WHATSAPP_MT_BULK")) {
					WADispatcher.getjRedis().publish(TopicSubscriber.CAMPAIGN_ACTIVE_LIST, topic.name());
				}
			}

			describeGroups();

			if (System.currentTimeMillis() - lastLogAnalyseTIme > 60 * 1000) {
				Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
						.listConsumerGroupOffsets("WA-DISPATCHER-GROUP").partitionsToOffsetAndMetadata().get();
				LOGGER.error("Offsets : {}", offsets);
			}

			analyzeLag("WA-DISPATCHER-GROUP");

		} catch (InterruptedException | ExecutionException e) {
		    LOGGER.error("ERR: IN TOPIC PUBLISHER", e);
		    Thread.currentThread().interrupt();
		  }
	}

	private void describeGroups() throws InterruptedException, ExecutionException {
		if (System.currentTimeMillis() - lastLogAnalyseTIme > 60 * 1000) {
			List<String> groupIds = adminClient.listConsumerGroups().all().get().stream().map(s -> s.groupId())
					.collect(Collectors.toList());
			Map<String, ConsumerGroupDescription> groups;

			groups = adminClient.describeConsumerGroups(groupIds).all().get();

			for (final String groupId : groupIds) {
				ConsumerGroupDescription descr = groups.get(groupId);
				// find if any description is connected to the topic with topicName
				Collection<MemberDescription> tp = descr.members();
				LOGGER.error("member : {}", tp);

			}
			lastLogAnalyseTIme = System.currentTimeMillis();
		}
	}

	public void analyzeLag(String groupId) throws ExecutionException, InterruptedException {
		if (System.currentTimeMillis() - lastLogAnalyseTIme > 60 * 1000) {
			Map<TopicPartition, Long> consumerGrpOffsets = getConsumerGrpOffsets(groupId);
			Map<TopicPartition, Long> producerOffsets = getProducerOffsets(consumerGrpOffsets);
			Map<TopicPartition, Long> lags = computeLags(consumerGrpOffsets, producerOffsets);
			for (Map.Entry<TopicPartition, Long> lagEntry : lags.entrySet()) {
				String topic = lagEntry.getKey().topic();
				int partition = lagEntry.getKey().partition();
				Long lag = lagEntry.getValue();
				LOGGER.error("Time {}| Lag for topic = {}, partition = {} is {}", new Date(), topic, partition, lag);
			}
			lastLogAnalyseTIme = System.currentTimeMillis();
		}
	}

	private Map<TopicPartition, Long> computeLags(Map<TopicPartition, Long> consumerGrpOffsets,
			Map<TopicPartition, Long> producerOffsets) {
		Map<TopicPartition, Long> lags = new HashMap<>();
		if(null != consumerGrpOffsets && null != producerOffsets) {
			for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
				Long producerOffset = producerOffsets.get(entry.getKey());
				Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
				long lag = Math.abs(producerOffset - consumerOffset);
				lags.putIfAbsent(entry.getKey(), lag);
			}
		}
		
		return lags;
	}

	private Map<TopicPartition, Long> getProducerOffsets(Map<TopicPartition, Long> consumerGrpOffset) {
		List<TopicPartition> topicPartitions = new LinkedList<>();
		for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffset.entrySet()) {
			TopicPartition key = entry.getKey();
			topicPartitions.add(new TopicPartition(key.topic(), key.partition()));
		}		
		return null;
	}

	private Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId)
			throws ExecutionException, InterruptedException {
		ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
		Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = info.partitionsToOffsetAndMetadata()
				.get();

		Map<TopicPartition, Long> groupOffset = new HashMap<>();
		for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
			TopicPartition key = entry.getKey();
			OffsetAndMetadata metadata = entry.getValue();
			groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
		}
		return groupOffset;
	}

	public static void deleteTopic(String topicName) {
		try {
			DeleteTopicsResult results = adminClient.deleteTopics(Arrays.asList(topicName));
			LOGGER.error("INFO: DELETED TOPICS ARE : :{}: {}", topicName, results);
			WADispatcher.getjRedis().publish(TopicSubscriber.CAMPAIGN_UNSUBSCRIBE_LIST, topicName);
		} catch (Exception e) {
			LOGGER.error("ERR: IN DELETE TOPIC : {}:{}", e, topicName);
		}
	}

	public void setLastLogAnalyseTIme(long lastLogAnalyseTIme) {
		this.lastLogAnalyseTIme = lastLogAnalyseTIme;
	}

	

}
