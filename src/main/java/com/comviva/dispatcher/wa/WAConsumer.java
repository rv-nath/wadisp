package com.comviva.dispatcher.wa;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.comviva.ngage.kafka.KafkaHelper;
import com.comviva.ngage.kafka.interfaces.KClientCallback;

public class WAConsumer implements KClientCallback {

	private static final Logger LOGGER = LogManager.getLogger(WAConsumer.class);

	private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
	private Map<TopicPartition, WAProcessor> activeTasks = new HashMap<>();
	private  List<String> unSubscribeList = new ArrayList<>();
	
	ExecutorService executorService = null;
	private KafkaHelper kCHelper = null;
	private String instanceId;
	private long lastCommitTime = System.currentTimeMillis();

	public WAConsumer(String instanceId, Properties properties, int noOfThreads) {
		this.kCHelper = KafkaHelper.getKCHelperInstance(instanceId, this, noOfThreads, properties, false, true);
		this.instanceId = instanceId;
		this.executorService=Executors.newFixedThreadPool(8);
	}

	public void onConsumers(Collection<String> topicNames, ConsumerRecords<String, String> records,
			KafkaConsumer<String, String> kafkaConsumer) {
		Long startTime = System.currentTimeMillis();
		LOGGER.trace("INFO : CONSUMER POLLER CALLED : {}", this.instanceId);
		if (!records.isEmpty()) {
			records.partitions().forEach(topicPartition -> {
				List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
				WAProcessor task = new WAProcessor(partitionRecords, topicPartition, instanceId);
				executorService.submit(task);
				activeTasks.put(topicPartition, task);
			});

			kafkaConsumer.pause(records.partitions());
		}
		try {
			checkActiveTasks(kafkaConsumer);
			commitOffsets(kafkaConsumer);
			unSubscribe(kafkaConsumer);
			
		} catch (Exception e) {
			LOGGER.error("ERR IN CONSUMER  : {}:{}", e, kafkaConsumer);
		}finally {
			LOGGER.info("CONSUMER TAKING TIME IS :{}",(System.currentTimeMillis()-startTime));
		}
	}

	public void subscribe(String topic) {
		LOGGER.error("NEW SUBSCRIBED TOPIC IS : {}", topic);
		this.kCHelper.subscribe(topic);
	}
	
	public void setUnSubscribe(String topic) {
		if(this.kCHelper.getTopicList().contains(topic)) {
			unSubscribeList.add(topic);
		}
	}
	
	public void unSubscribe(KafkaConsumer<String, String> kafkaConsumer) {
		if(unSubscribeList.isEmpty()) {
			return;
		}
		
		for(String topic : unSubscribeList) {
			try {
				this.kCHelper.unSubscribe(topic);
			} catch (Exception e) {
				LOGGER.error("ERR: IN UNSUBSCRIBE",e);
			}
		}
	}

	private void checkActiveTasks(KafkaConsumer<String, String> kafkaConsumer) {
		
		List<TopicPartition> resumeTopicPartition = new ArrayList<>();
		if (activeTasks == null || activeTasks.isEmpty()) {
			LOGGER.debug("WARN : activeTasks list is empty");
			return;
		}
		activeTasks.forEach((topicPartition, task) -> {
			boolean isTaskFinished = task.isTaskFinish();
			long offset = task.getCurrentOffset();
			boolean isnextPoll = task.isIsnextPoll();
			long secondEpoch = task.getSecondEpoch();
			if (isTaskFinished) {
				if (isnextPoll) {
					resumeTopicPartition.add(topicPartition);
					addOffset(topicPartition, offset);
				} else {
					seekOffSet(kafkaConsumer, topicPartition, offset);
					if (secondEpoch < (System.currentTimeMillis() / 1000)) {
						resumeTopicPartition.add(topicPartition);
					}
				}
			}
		});

		try {
			if (!resumeTopicPartition.isEmpty()) {
				LOGGER.info("ACTIVE LIST IS : {}", activeTasks);
				for (TopicPartition removeKey : resumeTopicPartition) {
					try {
						kafkaConsumer.resume(Arrays.asList(removeKey));
					} catch (IllegalStateException e) {
						LOGGER.error("ERR: java.lang.IllegalStateException: No current assignment for partition: {}",
								removeKey);
					}
					activeTasks.remove(removeKey);
				}
			}
			if (System.currentTimeMillis() - lastCommitTime > 60 * 1000) {
				Set<TopicPartition> pausedList = kafkaConsumer.paused();
				if (!pausedList.isEmpty()) {
					LOGGER.info("PAUSED TOPIC LIST IS : {}", pausedList);
				}
			}

		} catch (Exception e) {
			LOGGER.error("ERR: in Resume topics:{}", resumeTopicPartition, e);
		}

	}

	/**
	 * @param kafkaConsumer
	 * @param topicPartition
	 * @param offset
	 */
	private void seekOffSet(KafkaConsumer<String, String> kafkaConsumer, TopicPartition topicPartition, long offset) {
		if (offset > 0) {
			kafkaConsumer.seek(topicPartition, new OffsetAndMetadata(offset));
		}
	}

	/**
	 * @param topicPartition
	 * @param offset
	 */
	private void addOffset(TopicPartition topicPartition, long offset) {
		if (offset > 0) {
			offsetsToCommit.put(topicPartition, new OffsetAndMetadata(offset));
		}
	}

	private void commitOffsets(KafkaConsumer<String, String> kafkaConsumer) {
		try {
			if (System.currentTimeMillis() - lastCommitTime > 60*1000) {
				if (offsetsToCommit != null && !offsetsToCommit.isEmpty()) {
					kafkaConsumer.commitSync(offsetsToCommit);
					LOGGER.info("OffSet Commit done .....  {}" , offsetsToCommit);
					offsetsToCommit.clear();
				} else {
					LOGGER.debug("WARN : OFFSET Commit map is empty");
				}
				lastCommitTime = System.currentTimeMillis();
			}
		} catch (Exception e) {
			LOGGER.error("ERR: in commit offset", e);
			lastCommitTime = System.currentTimeMillis();
		}
	}

	@Override
	public void onError(Exception e, Collection<String> topicNames) {
		LOGGER.error("ERR: ON ERROR CALL  in topics {}", topicNames);

	}

	@Override
	public void close(String topicName) {
		LOGGER.error("ERR: CLOSE CALL: {}", topicName);

	}

	@Override
	public void onConsume(String topicName, ConsumerRecord<String, String> record) {
		LOGGER.error("ERR: WRONG METHOD CALL: {}", topicName);

	}

	public void subscribe(Set<String> subscribedTopics) {
		for (String topics : subscribedTopics) {
			subscribe(topics);
		}
	}

	@Override
	public boolean canConsumeMoreRecords(Collection<String> topics) {
		boolean fine = isAllWell();
		return fine;
	}

	private boolean isAllWell() {

		return true;
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void onEmptyList(Collection<String> topics) {
		LOGGER.error("EMPTY DATA IN {}  ", topics);// , this.kCHelper.getpausedList());
	}

	public KafkaHelper getkCHelper() {
		return kCHelper;
	}

	public void setkCHelper(KafkaHelper kCHelper) {
		this.kCHelper = kCHelper;
	}

	public void setLastCommitTime(long lastCommitTime) {
		this.lastCommitTime = lastCommitTime;
	}

}
