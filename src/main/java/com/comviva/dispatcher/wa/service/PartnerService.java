package com.comviva.dispatcher.wa.service;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.comviva.dispatcher.wa.utils.AppConstants;
import com.comviva.ngage.commons.httpPool.HttpDispatcherImpl;
import com.comviva.ngage.commons.httpPool.HttpResponse;

public class PartnerService {

	HttpDispatcherImpl dispatcherImpl = null;

	private static final Logger LOGGER = LogManager.getLogger(PartnerService.class);

	public HttpResponse sendMessage(String apiKey, String message) throws Exception {
		String messagesURL = System.getenv().getOrDefault(AppConstants.MESSAGES_URL,
				System.getProperty(AppConstants.MESSAGES_URL));
// 		messagesURL = "https://waba.360dialog.io/v1/messages";
		LOGGER.info("Inside Partner Service : Messages URL : {}", messagesURL);
		HashMap<String, Object> hashApiKey = new HashMap<>();
		hashApiKey.put(AppConstants.API_KEY, apiKey);
		if (dispatcherImpl == null) {
			dispatcherImpl = HttpDispatcherImpl.getInstance("WA_Dispatcher", null);
		}
		HttpResponse httpResponse = dispatcherImpl.httpPostJsonBloking(messagesURL, message, hashApiKey, null);
		LOGGER.trace("Response Body : {} ", httpResponse.responseBody);
		if (httpResponse.isOk()) {
			LOGGER.trace("Message Sending Successful: {}", httpResponse);
		} else {
			LOGGER.trace("Some Probel in Message Sending: {}", httpResponse);
		}
		return httpResponse;
	}

}
