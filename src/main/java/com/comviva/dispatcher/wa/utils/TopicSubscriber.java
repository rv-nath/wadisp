package com.comviva.dispatcher.wa.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.comviva.dispatcher.wa.WAManager;
import com.comviva.ngage.redispool.JRedis;

import redis.clients.jedis.JedisPubSub;

public class TopicSubscriber extends JedisPubSub implements Runnable {
	private static final Logger LOGGER = LogManager.getLogger(TopicSubscriber.class);

	public static final String CAMPAIGN_ACTIVE_LIST = "CAMPAIGN:ACTIVE:LIST";
	public static final String CAMPAIGN_UNSUBSCRIBE_LIST = "CAMPAIGN:UNSUBSCRIBE:LIST";

	private static TopicSubscriber topicSubscriber = null;
	private boolean isStop = false;
	private JRedis jRedis;
	private WAManager smsManager;
	private String[] channels;

	public TopicSubscriber(JRedis jRedis, WAManager smsManager) {
		this.jRedis = jRedis;
		this.smsManager = smsManager;
		this.channels = new String[] { CAMPAIGN_ACTIVE_LIST };
	}

	/**
	 * 
	 * @param smsManager
	 * @return
	 */
	public static TopicSubscriber getInstance(JRedis jRedis, WAManager smsManager) {
		if (topicSubscriber == null) {
			topicSubscriber = new TopicSubscriber(jRedis, smsManager);
			Thread th = new Thread(topicSubscriber);
			th.start();
		}
		return topicSubscriber;
	}

	@Override
	public void run() {
		while (!isStop) {
			try {
				this.jRedis.subscribe(this, channels);
			} catch (Exception e) {
				LOGGER.error("ERR: IN TOPIC SUBSCRIBER",e);
			}
		}
	}

	@Override
	public void onMessage(final String channel, final String message) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("************ SMS DISPATCHER onMessage ************");
			LOGGER.debug("channel [ " + channel + " ] message [ " + message + " ]");
		}

		switch (channel) {
		case CAMPAIGN_ACTIVE_LIST:
			this.smsManager.setActiveTopics(message);
			break;
		case CAMPAIGN_UNSUBSCRIBE_LIST:
			this.smsManager.setUnsubsribeTopics(message);
			break;

		default:
			break;
		}
	}

}
