package com.comviva.dispatcher.wa.service;

import java.util.Properties;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.comviva.dispatcher.wa.WADispatcher;
import com.comviva.dispatcher.wa.utils.AppConstants;
import com.comviva.ngage.kafka.KafkaHelper;
import com.comviva.ngage.msg.MessageRequest;
import com.comviva.ngage.base.constants.MsgkeyConstants;

public class ProducerService {

	private static final Logger LOGGER = LogManager.getLogger("ProducerService");

	private Properties kafka_prop = null;

	public static ProducerService producerService = null;

	public ProducerService() {
		kafka_prop = new Properties();
		kafka_prop.putAll(WADispatcher.kProperties);
	}

	public static ProducerService getInstance() {
		if (producerService == null) {
			producerService = new ProducerService();

		}
		return producerService;
	}

	/**
	 *
	 * @param request
	 * @throws Exception
	 */
	public void sendToKafKa(MessageRequest request) throws Exception {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("Producer received Message request is::{}", request);
		}
		try {
			Properties prop = new Properties();
			prop.putAll(kafka_prop);

		String topic = System.getenv().getOrDefault(AppConstants.DR_TOPIC,
					System.getProperty(AppConstants.DR_TOPIC)) + "-" + request.getString("o_id");
			LOGGER.error("Topic Name :::{}", topic);
			KafkaHelper producer = KafkaHelper.getKProducerInstance(topic, kafka_prop);
			RecordMetadata metaData = producer.publishSync(topic, request.toJSON());
			if (metaData != null) {
				LOGGER.info("Message Sent Successfully");
			}
			if (LOGGER.isInfoEnabled()) {
				LOGGER.info("Sent message: {} with offset: ", request);
			}
		} catch (Exception e) {
			LOGGER.error("Exception inside ProducerService : sendToKafKa messge : {}", e.getMessage());
			throw e;
		}
	}
}
