package com.comviva.dispatcher.wa;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WAManager extends TimerTask {
	private static final Logger LOGGER = LogManager.getLogger(WAManager.class);

	private static WAManager waManager = null;
	private List<WAConsumer> consumerList = null;

	private Set<String> dispatchTopics = new HashSet<>();
	private Set<String> arriveTopics = new HashSet<>();

	private Iterator<WAConsumer> consumerItr = null;

	private static final int DELAY_CONSTANT = 1000;
	private static final long PERIOD_MULTIPLIER = 3L;

	public WAManager(List<WAConsumer> consumerList) {
		this.consumerList = consumerList;
		this.consumerItr = circularIterator(consumerList);
	}

	public static WAManager getInstance(List<WAConsumer> consumerList) {
		if (waManager == null) {
			waManager = new WAManager(consumerList);

			final Timer timer = new Timer("WA-MANAGER-TH", true);
			timer.scheduleAtFixedRate(waManager, (DELAY_CONSTANT - (System.currentTimeMillis() % DELAY_CONSTANT)),
					PERIOD_MULTIPLIER * 1000);
		}
		return waManager;
	}

	static <T> Iterator<T> circularIterator(List<T> list) {
		int size = list.size();
		return new Iterator<T>() {

			int i = 0;

			@Override
			public T next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				if (i == size * 2) {
					i = 0;
				}
				return list.get(i++ % size);
			}

			@Override
			public boolean hasNext() {
				return true;
			}
		};
	}

	@Override
	public void run() {
		try {

			Set<String> newTopics = getNewTopics();

			if (newTopics.isEmpty()) {
				return;
			}
			LOGGER.info("TOTAL SUBSCRIBE TOPICS ARE: {}", dispatchTopics.size());
			this.dispatchTopics.addAll(newTopics);

			LOGGER.error("SUBSCRIBED NEW TOPICS ARE :{}", newTopics);
			LOGGER.info("TOTAL SUBSCRIBE TOPICS ARE: {}", dispatchTopics.size());
			for (String topic : newTopics) {
				consumerItr.next().subscribe(topic);
			}

		} catch (Exception e) {
			LOGGER.error("ERR: IN WA MANAGER FOR GETTING NEW TOPICS", e);
		}
	}

	private Set<String> getNewTopics() {
		Set<String> difference = arriveTopics.stream().filter(aObj -> !dispatchTopics.contains(aObj))
				.collect(Collectors.toSet());
		return difference;
	}

	public void setActiveTopics(String topic) {
		arriveTopics.add(topic);
	}

	public List<WAConsumer> getConsumerList() {
		return consumerList;
	}

	public Set<String> getDispatchTopics() {
		return dispatchTopics;
	}

	public void setUnsubsribeTopics(String topic) {
		for (WAConsumer consumer : consumerList) {
			consumer.setUnSubscribe(topic);
		}

	}

}
