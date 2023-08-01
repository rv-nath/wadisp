package com.comviva.dispatcher.wa;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.comviva.dispatcher.wa.config.Kafka;
import com.comviva.dispatcher.wa.config.RedisConfiguration;
import com.comviva.dispatcher.wa.service.PartnerService;
import com.comviva.dispatcher.wa.service.ProducerService;
import com.comviva.dispatcher.wa.utils.AppConstants;
import com.comviva.dispatcher.wa.utils.TopicDelete;
import com.comviva.dispatcher.wa.utils.TopicPublisher;
import com.comviva.dispatcher.wa.utils.TopicSubscriber;
import com.comviva.ngage.base.constants.MsgkeyConstants;
import com.comviva.ngage.base.enumeration.JobType;
import com.comviva.ngage.base.enumeration.WAConversationType;
import com.comviva.ngage.commons.httpPool.HttpResponse;
import com.comviva.ngage.kafka.KProducer;
import com.comviva.ngage.kafka.KafkaHelper;
import com.comviva.ngage.msg.MessageRequest;

public class GeneralTests {

	static Kafka kafka = new Kafka();

	@BeforeClass
	public static void setUp() throws Exception {
		Properties testProperties = new Properties();
		testProperties.load(GeneralTests.class.getClassLoader().getResourceAsStream("application.properties"));
		testProperties.putAll(System.getProperties());
		System.setProperties(testProperties);
		try {
			RedisConfiguration.serverStart();
			kafka.setupKafka();
		} catch (Exception e) {
			e.printStackTrace();
		}
		WADispatcher.main(new String[] {});
	}

	@AfterClass
	public static void tearDown() throws Exception {
		try {
			RedisConfiguration.serverStop();
			kafka.tearDownKafka();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void producerTest() throws Exception {
		ProducerService ps = ProducerService.getInstance();
		MessageRequest messageRequest = new MessageRequest();
		messageRequest.set(MsgkeyConstants.TXN_ID, UUID.randomUUID());
		messageRequest.set(MsgkeyConstants.MSG_ACC_ID, UUID.randomUUID());
		messageRequest.set(MsgkeyConstants.ACC_USER_ID, UUID.randomUUID());
		messageRequest.set(MsgkeyConstants.ACC_USER_ID, UUID.randomUUID());
		messageRequest.set("apiKey", "C77kMGaERYIuE3UJCBfcP68JAK");
		messageRequest.set("message",
				"{\"to\":\"919962009107\",\"type\":\"interactive\",\"interactive\":{\"type\":\"location_request_message\",\"body\":{\"type\":\"text\",\"text\":\"Send your location\"},\"action\":{\"name\":\"send_location\"}},\"from\":\"917795217711\"}");
		ps.sendToKafKa(messageRequest);
		assertNotNull(ps);
	}

	@Test
	public void partnerTest() throws Exception {
		PartnerService partnerService = new PartnerService();
		HttpResponse httpResponse = partnerService.sendMessage("C77kMGaERYIuE3UJCBfcP68JAK",
				"{\"to\":\"919962009107\",\"type\":\"interactive\",\"interactive\":{\"type\":\"location_request_message\",\"body\":{\"type\":\"text\",\"text\":\"Send your location\"},\"action\":{\"name\":\"send_location\"}},\"from\":\"917795217711\"}");
		assertTrue(httpResponse.isOk());
	}
	
	@Test
	public void waConsumerTest() {
		KafkaHelper helper = new KafkaHelper("WHATSAPP_MT_NON_BULK_1", 5, WADispatcher.kProperties);
		WAConsumer consumerCallback1 = new WAConsumer("KAFKA-WA-CONSUMER-1", WADispatcher.kProperties, 1);
		consumerCallback1.setkCHelper(helper);
		consumerCallback1.setLastCommitTime(0);
		assertNotNull(consumerCallback1.getkCHelper());
		Set<String> topicNames = new HashSet<String>();
		topicNames.add("WHATSAPP_MT_NON_BULK_2");
		consumerCallback1.subscribe(topicNames);
		consumerCallback1.close("WHATSAPP_MT_NON_BULK_2");
		consumerCallback1.setUnSubscribe("WHATSAPP_MT_NON_BULK_2");
		consumerCallback1.onEmptyList(topicNames);
		consumerCallback1.onError(new Exception(), topicNames);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(WADispatcher.kProperties);
		consumerCallback1.unSubscribe(consumer);
		ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<String, String>("WHATSAPP_MT_NON_BULK_1", 1, 1000, "sample", "sample");
		consumerCallback1.onConsume("WHATSAPP_MT_NON_BULK_1", consumerRecord);
		
	}
	
	@Test
	public void createMessageTrackerTest() {
		MessageRequest mr = new MessageRequest();
		mr.set(MsgkeyConstants.ACC_USER_ID, UUID.randomUUID());
		mr.set(MsgkeyConstants.MSG_USER_ID, UUID.randomUUID());
		mr.set(MsgkeyConstants.JOB_TYPE, JobType.NON_BULK);
		mr.set(MsgkeyConstants.JOB_COST, "100.00");
		String campaignId = UUID.randomUUID().toString();
		mr.set(MsgkeyConstants.PARENT_MSG_TXN_ID, campaignId);
		mr.set(MsgkeyConstants.TXN_ID, UUID.randomUUID());
		mr.set(MsgkeyConstants.WALLET_ID, UUID.randomUUID());
		mr.set(MsgkeyConstants.PACKAGE_CONSUMPTION, "1");
		mr.set(MsgkeyConstants.CONSUMED_PACKAGE, "sample");
		mr.set(MsgkeyConstants.BOOKING_ID, UUID.randomUUID());
		mr.set("conversation_type", WAConversationType.BUSINESS_INITIATED.name());
		mr.set(MsgkeyConstants.CAMPAIGN_NAME, "sample");
		mr.set(MsgkeyConstants.ORG_ID, UUID.randomUUID());
		mr.set(MsgkeyConstants.RECURRENCE_ID, "1");
		HttpResponse response = new HttpResponse();
		response.responseBody = "success";
		response.statusCode = 200;
		WADispatcher.getjRedis().hset(campaignId + ":" + "1",
                MsgkeyConstants.TOTAL_COUNT,"1");
		List<ConsumerRecord<String, String>> partitionRecords = new ArrayList<ConsumerRecord<String,String>>();
		ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<String, String>("WHATSAPP_MT_NON_BULK_1", 1, 1000, "sample", mr.toJSON());
		partitionRecords.add(consumerRecord);
		TopicPartition topicPartition = new TopicPartition("WHATSAPP_MT_NON_BULK_1", 1);
		WAProcessor waProcessor = new WAProcessor(partitionRecords, topicPartition, "123");
		waProcessor.run();
	}
	
	@Test
	public void waAPPConstants() {
		AppConstants appConstants = new AppConstants();
		assertNotNull(appConstants);
		assertNotNull(AppConstants.TOPIC);
		assertNotNull(AppConstants.MAX_TASKS);
		assertNotNull(AppConstants.MESSAGES_URL);
		assertNotNull(AppConstants.KRAKEND_URL);
		assertNotNull(AppConstants.TEMPLATES_URL);
		assertNotNull(AppConstants.API_KEY);
		assertNotNull(AppConstants.CANCEL_URL);
		assertNotNull(AppConstants.DR_TOPIC);
	}

	@Test
	public void waManagerTest() {
		WAConsumer consumerCallback1 = new WAConsumer("KAFKA-WA-CONSUMER-1", WADispatcher.kProperties, 1);
		WAConsumer consumerCallback2 = new WAConsumer("KAFKA-WA-CONSUMER-2", WADispatcher.kProperties, 1);
		WAConsumer consumerCallback3 = new WAConsumer("KAFKA-WA-CONSUMER-3", WADispatcher.kProperties, 1);
		List<WAConsumer> consumerList = new ArrayList<>();
		consumerList.add(consumerCallback1);
		consumerList.add(consumerCallback2);
		consumerList.add(consumerCallback3);
		WAManager waManager = WAManager.getInstance(consumerList);
		waManager.setActiveTopics("WHATSAPP_MT_BULK_1_1");
		waManager.run();
		TopicDelete topicDelete = new TopicDelete();
		topicDelete.run();
		waManager.setActiveTopics("WHATSAPP_MT_BULK_2_2");
		waManager.run();
		topicDelete.run();
		waManager.setActiveTopics("WHATSAPP_MT_BULK_3_3");
		waManager.run();
		topicDelete.run();
		assertNotNull(waManager.getConsumerList());
		waManager.setUnsubsribeTopics("WHATSAPP_MT_BULK_1_1");
		waManager.run();
		topicDelete.run();
	}

	@Test
	public void sampleTest() throws InterruptedException, ExecutionException {
		KProducer producer = new KProducer(WADispatcher.kProperties);
		assertNotNull(producer);
		WAConsumer consumerCallback = new WAConsumer("KAFKA-WA-CONSUMER-1", WADispatcher.kProperties, 1);
		MessageRequest messageRequest = new MessageRequest();
		messageRequest.set(MsgkeyConstants.TXN_ID, UUID.randomUUID());
		messageRequest.set(MsgkeyConstants.MSG_USER_ID, UUID.randomUUID());
		messageRequest.set(MsgkeyConstants.ACC_USER_ID, UUID.randomUUID());
		messageRequest.set("apiKey", "C77kMGaERYIuE3UJCBfcP68JAK");
		List<WAConsumer> consumerList = new ArrayList<>();
		consumerList.add(consumerCallback);
		WAManager waManager = WAManager.getInstance(consumerList);
		TopicSubscriber ts = TopicSubscriber.getInstance(WADispatcher.getjRedis(), waManager);
		ts.onMessage("CAMPAIGN:UNSUBSCRIBE:LIST", "sample");
		TopicPublisher tp = TopicPublisher.getInstance(WADispatcher.kProperties);
		tp.setLastLogAnalyseTIme(0);
		tp.analyzeLag("WA-DISPATCHER-GROUP");
		TopicPublisher.deleteTopic("WHATSAPP_MT_NON_BULK_1");
		TopicDelete.getInstance();
		waManager.setActiveTopics("WHATSAPP_MT_NON_BULK_" + messageRequest.getString(MsgkeyConstants.MSG_USER_ID) + "_"
				+ messageRequest.getString(MsgkeyConstants.ACC_USER_ID));
		messageRequest.set("message",
				"{\"to\":\"919962009107\",\"type\":\"interactive\",\"interactive\":{\"type\":\"location_request_message\",\"body\":{\"type\":\"text\",\"text\":\"Send your location\"},\"action\":{\"name\":\"send_location\"}},\"from\":\"917795217711\"}");
		producer.sendRecord("WHATSAPP_MT_NON_BULK_" + messageRequest.getString(MsgkeyConstants.MSG_USER_ID) + "_"
				+ messageRequest.getString(MsgkeyConstants.ACC_USER_ID), messageRequest.toJSON());
		tp.analyzeLag("WA-DISPATCHER-GROUP");
		Set<NewTopic> topicNames = new HashSet<>();
		topicNames.add(new NewTopic("WHATSAPP_MT_NON_BULK_1", 1, (short) 2));
		topicNames.add(new NewTopic("WHATSAPP_MT_BULK_1", 1, (short) 2));
		TopicPublisher.adminClient.createTopics(topicNames);
		tp.setLastLogAnalyseTIme(0);
		tp.run();
		assertNotNull(consumerCallback);
	}
}
