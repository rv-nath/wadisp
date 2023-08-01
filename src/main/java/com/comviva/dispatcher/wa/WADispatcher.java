package com.comviva.dispatcher.wa;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.comviva.dispatcher.wa.utils.TopicDelete;
import com.comviva.dispatcher.wa.utils.TopicPublisher;
import com.comviva.dispatcher.wa.utils.TopicSubscriber;
import com.comviva.ngage.base.constants.Constants;
import com.comviva.ngage.base.exceptions.MissingConfigurationException;
import com.comviva.ngage.base.utils.StringUtils;
import com.comviva.ngage.msg.MalformedBufferException;
import com.comviva.ngage.msg.MessageRequest;
import com.comviva.ngage.msg.MessageRequest.MessageBufferType;
import com.comviva.ngage.msg.UnknownMessageBuilderException;
import com.comviva.ngage.redispool.JRedis;

public class WADispatcher {
	private static final Logger LOGGER = LogManager.getLogger(WADispatcher.class);

	public static Properties kProperties = null;
	private static List<WAConsumer> consumerList = new ArrayList<>();
	public static Properties localProperties = null;
	public static WAManager waManager = null;
	private static JRedis jRedis;
	public static Properties redisProperties;
	public static Properties rdbmsProperties;

	/**
	 * @param args
	 * @throws MissingConfigurationException
	 */
	public static void main(String[] args) {
		try {
			LOGGER.info("Starting WA Disaptcher service");
			LOGGER.info("Reading environment variables...");
			init();
			createJedisConnection();
			createConsumers();
			createWAManager();
			createPubSub();
			LOGGER.info("WA Disaptcher service ready");
		} catch (Exception e) {
			LOGGER.error("ERR: In Main thread so progrom exist", e);
			System.exit(0);
		}

	}

	private static void createConsumers() {
		for (int i = 1; i <= 10; i++) {
			LOGGER.error("{} WA CONSUMER CREATED...", i);
			WAConsumer consumerCallback = new WAConsumer("KAFKA-WA-CONSUMER-" + i, kProperties, 1);
			consumerList.add(consumerCallback);
		}
	}

	private static void createWAManager() {
		waManager = WAManager.getInstance(consumerList);
	}

	private static void createPubSub() {
		TopicSubscriber.getInstance(jRedis, waManager);
		TopicPublisher.getInstance(kProperties);
		TopicDelete.getInstance();
	}

	private static void createJedisConnection() throws MissingConfigurationException {
		jRedis = new JRedis(redisProperties);
	}

	/**
	 * @throws MalformedBufferException
	 * @throws UnknownMessageBuilderException
	 * 
	 */
	public static void init() {
		try {
			loadLocalEnv();
			loadEnv();
		} catch (Exception e) {
			LOGGER.error("ERR: IN INIT CALL ", e);
		}
	}

	private static void loadLocalEnv() {

		localProperties = new Properties();
		boolean islocalRun = Boolean.parseBoolean(System.getenv().getOrDefault("IS_LOCAL_RUN", "true"));
		if (!islocalRun) {
			return;
		}
		try {
			localProperties.load(WADispatcher.class.getClassLoader().getResourceAsStream("application.properties"));
			localProperties.putAll(System.getProperties());
			System.setProperties(localProperties);
		} catch (IOException e) {
			LOGGER.error("ERR: TO LOAD LOCAL PROPERTIES ", e);
		}
	}

	public static void loadEnv() throws IOException, UnknownMessageBuilderException, MalformedBufferException {
		loadKafkaProperties();
		loadRedisProperties();
	}

	private static void loadRedisProperties() throws UnknownMessageBuilderException, MalformedBufferException {
		String redis_prop = System.getenv().get(Constants.REDIS_PROPERTIES);
		String redis_sec_prop = System.getenv().getOrDefault(Constants.REDIS_PROPERTIES_SECRET,
				System.getProperty(Constants.REDIS_PROPERTIES_SECRET));
		
		if(StringUtils.isBlank(redis_prop)) {
			redis_prop = System.getProperty(Constants.REDIS_PROPERTIES);
			MessageRequest temp = new MessageRequest(redis_prop, MessageBufferType.json);
			temp.merge(new MessageRequest(redis_sec_prop, MessageBufferType.json));
			redisProperties = temp.getProperties();
			LOGGER.info("Reading Redis properties... Didn't find,so falling back to {}",redisProperties);
		}else {
			MessageRequest temp = new MessageRequest(redis_prop, MessageBufferType.json);
			temp.merge(new MessageRequest(redis_sec_prop, MessageBufferType.json));
			redisProperties = temp.getProperties();
			LOGGER.info("Reading Redis properties... found as {}",redisProperties);
		}
	}

	private static void loadKafkaProperties() throws UnknownMessageBuilderException, MalformedBufferException {
		String kafka_prop = System.getenv().get(Constants.KAFKA_PROPERTIES);		
		if(StringUtils.isBlank(kafka_prop)) {
			kafka_prop = System.getProperty(Constants.KAFKA_PROPERTIES);
			kProperties = new MessageRequest(kafka_prop, MessageBufferType.json).getProperties();
			kProperties.setProperty("max.poll.records", "100");
			kProperties.setProperty("max.partition.fetch.bytes", "52428800");
			kProperties.setProperty("max.poll.interval.ms", "300000");
			LOGGER.info("Reading Kafka properties... Didn't find,so falling back to {}",kProperties);
		}else {
			kProperties = new MessageRequest(kafka_prop, MessageBufferType.json).getProperties();
			kProperties.setProperty("max.poll.records", "100");
			kProperties.setProperty("max.partition.fetch.bytes", "52428800");
			kProperties.setProperty("max.poll.interval.ms", "300000");
			LOGGER.info("Reading Kafka properties... found as {}",kProperties);
		}
	}

	public static JRedis getjRedis() {
		return jRedis;
	}

	public static List<WAConsumer> getConsumerList() {
		return consumerList;
	}

	public static WAManager getWaManager() {
		return waManager;
	}

}
