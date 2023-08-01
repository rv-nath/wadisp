package com.comviva.dispatcher.wa.utils;

import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import com.comviva.dispatcher.wa.WADispatcher;
import com.comviva.ngage.base.enumeration.CampaignStatus;
import com.comviva.ngage.msg.MessageRequest;

public class TopicDelete extends TimerTask {

	private static TopicDelete topicDelete = null;
	private static final int DELAY_CONSTANT = 1000;
	private static final long PERIOD_MULTIPLIER = 60L;

	public static TopicDelete getInstance() {
		if (topicDelete == null) {
			topicDelete = new TopicDelete();
			final Timer timer = new Timer("WA-DELETE-TOPIC", true);
			timer.scheduleAtFixedRate(topicDelete, (DELAY_CONSTANT - (System.currentTimeMillis() % DELAY_CONSTANT)),
					PERIOD_MULTIPLIER * 1000);
		}

		return topicDelete;
	}

	@Override
	public void run() {

		Set<String> topicList = WADispatcher.getWaManager().getDispatchTopics();

		for (String topic : topicList) {
			if (topic.contains("WHATSAPP_MT_BULK")) {
				String campaignId = topic.split("_")[4];

				Map<String, String> parentInfo = WADispatcher.getjRedis().hgetAll(campaignId);
				MessageRequest parentReq = new MessageRequest();
				parentReq.set(parentInfo);
				int status = parentReq.getInt("status", 0);
				if (status == CampaignStatus.CANCELLED.getStatus() || status == CampaignStatus.COMPLETED.getStatus()
						|| status == CampaignStatus.REJECTED.getStatus()
						|| status == CampaignStatus.FAILED.getStatus()) {
					TopicPublisher.deleteTopic(topic);
				}
			}

		}

	}

}
