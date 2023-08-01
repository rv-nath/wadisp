package com.comviva.dispatcher.wa;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.comviva.dispatcher.wa.service.PartnerService;
import com.comviva.dispatcher.wa.service.ProducerService;
import com.comviva.dispatcher.wa.utils.AppConstants;
import com.comviva.ngage.auth.service.LoginService;
import com.comviva.ngage.base.constants.MsgkeyConstants;
import com.comviva.ngage.base.enumeration.ErrorCodes;
import com.comviva.ngage.base.enumeration.JobType;
import com.comviva.ngage.base.enumeration.WAConversationType;
import com.comviva.ngage.base.utils.StringUtils;
import com.comviva.ngage.commons.httpPool.HttpDispatcherImpl;
import com.comviva.ngage.commons.httpPool.HttpResponse;
import com.comviva.ngage.msg.MessageRequest;
import com.comviva.ngage.msg.MessageRequest.MessageBufferType;
import com.comviva.ngage.redispool.JRedis;
import com.comviva.ngage.base.enumeration.CampaignStatus;

public class WAProcessor implements Runnable {

	private static final Logger LOGGER = LogManager.getLogger(WAProcessor.class);

	private List<ConsumerRecord<String, String>> partitionRecords;
    private static final String STATUS_UPDATE_1 = ":STATUS_UPDATE:1:";
	private static final String SCHEDULER_TASK_B = "scheduler.task.b";
	private boolean isnextPoll = true;
	private final AtomicLong currentOffset = new AtomicLong(-1);
	private boolean taskFinish = false;
	private long secondEpoch;
	private String topic;
	private int partition;
	private String consumerInstanceId;
	private PartnerService partnerService = new PartnerService();
	private ProducerService producerService = new ProducerService();
	HttpDispatcherImpl dispatcherImpl = null;

	public WAProcessor(List<ConsumerRecord<String, String>> partitionRecords, TopicPartition topicPartition,
			String instanceId) {
		this.partitionRecords = partitionRecords;
		this.topic = topicPartition.topic();
		this.partition = topicPartition.partition();
		this.consumerInstanceId = instanceId;
	}

	@Override
	public void run() {
		int processCount = 0;
		try {
			for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
				if (!isnextPoll) {
					LOGGER.error("TPS THROTLER IS TRUE SO SKIPING THE REMAIN DATA & Last Offset : {}",
							this.currentOffset.get());
					return;
				}
				process(partitionRecord);
				processCount++;
			}
		} catch (Exception e) {
			LOGGER.error("ERR: In SMSProcessor all records topic partition ", e);
		} finally {
			LOGGER.info("INFO: TOPIC PARTITION BATCH IS COMPLETED : {}/{} & offset is : {}:T{}", processCount,
					partitionRecords.size(), this.getCurrentOffset(), this.topic);
			this.taskFinish = true;
		}
	}

	/**
	 * @param partitionRecord
	 */
	private void process(ConsumerRecord<String, String> partitionRecord) {
		int status = -1;
		MessageRequest msgReq = null;
		try {
			LOGGER.info("INFO: Request is  [{}:{}] by {}", partitionRecord.topic(), partitionRecord.partition(),
					partitionRecord.value());
			currentOffset.set(partitionRecord.offset() + 1);

			msgReq = new MessageRequest(partitionRecord.value(), MessageBufferType.json);
			setTopicMetaInfo(partitionRecord, msgReq);
			status = executeResolvers(msgReq);

		} catch (Exception e) {
			LOGGER.error("ERR: In SMSProcessor per single record ", e);
		} finally {
			generateFailureEntry(status, msgReq);
		}
	}

	private int executeResolvers(MessageRequest msgReq) {
		int status = 0;

		// added wa code
		try {
			LOGGER.error("msgReq {} ",msgReq);			
			HttpResponse response = null;
			if (msgReq.getString("message") != null) {
				response = partnerService.sendMessage(msgReq.getString("apiKey"),
						msgReq.get("message").toString());
			}
			MessageRequest dr = getDR(response, msgReq);
			producerService.sendToKafKa(dr);
			createMessageTracker(response, msgReq, dr);
		} catch (Exception e) {
			LOGGER.error("ERR: In while execute resolvers ", e);
		}

		msgReq.set(MsgkeyConstants.STATUS, status);
		msgReq.set(MsgkeyConstants.DELIVERY_CODE, status);

		if (status == ErrorCodes.ERROR_THROTTLE_LIMIT_EXCEEDED.eCode) {
			this.isnextPoll = false;
			this.secondEpoch = System.currentTimeMillis() / 1000;
			LOGGER.info(
					"UPDATE TO KAFKA TOPIC TPS THROTTLE REACHED : {} & par: {}  & SEEK OFFSET IS : {} & nextPoll : {}",
					this.topic, this.partition, this.currentOffset.get(), this.isnextPoll);
		}

		return status;
	}

	private void createMessageTracker(HttpResponse httpResponse, MessageRequest msgReq,MessageRequest dr) throws ParseException {
		JRedis jRedis = WADispatcher.getjRedis();
		JSONParser jsonParser = new JSONParser();
		if (httpResponse.isOk() && httpResponse.responseBody != null) {
			JSONObject responseBody = (JSONObject) jsonParser.parse(httpResponse.responseBody);
			JSONObject messageObj = (JSONObject) ((JSONArray) responseBody.get("messages")).get(0);
			MessageRequest mr = new MessageRequest();
			mr.set(MsgkeyConstants.ACC_USER_ID, msgReq.getString(MsgkeyConstants.ACC_USER_ID));
			mr.set(MsgkeyConstants.MSG_USER_ID, msgReq.getString(MsgkeyConstants.MSG_USER_ID));
			mr.set(MsgkeyConstants.JOB_TYPE, msgReq.getString(MsgkeyConstants.JOB_TYPE));
			mr.set(MsgkeyConstants.JOB_COST, msgReq.getString(MsgkeyConstants.JOB_COST));
			mr.set(MsgkeyConstants.PARENT_MSG_TXN_ID, msgReq.getString(MsgkeyConstants.PARENT_MSG_TXN_ID));
			mr.set(MsgkeyConstants.TXN_ID, msgReq.getString(MsgkeyConstants.TXN_ID));
			mr.set(MsgkeyConstants.WALLET_ID, msgReq.getString(MsgkeyConstants.WALLET_ID));
			mr.set(MsgkeyConstants.PACKAGE_CONSUMPTION, msgReq.getString(MsgkeyConstants.PACKAGE_CONSUMPTION));
			mr.set(MsgkeyConstants.CONSUMED_PACKAGE, msgReq.getString(MsgkeyConstants.CONSUMED_PACKAGE));
			mr.set(MsgkeyConstants.CONVERSATION_TYPE, msgReq.getString(MsgkeyConstants.CONVERSATION_TYPE));
			mr.set(MsgkeyConstants.CAMPAIGN_NAME, msgReq.getString(MsgkeyConstants.NAME));
			mr.set(MsgkeyConstants.ORG_ID, msgReq.getString(MsgkeyConstants.ORG_ID));
			mr.set(MsgkeyConstants.RECURRENCE_ID, msgReq.getString(MsgkeyConstants.RECURRENCE_ID));
			mr.set("c_tm", Long.toString(System.currentTimeMillis()));
			mr.set("u_nm", msgReq.getString(MsgkeyConstants.USER_NAME));
			mr.set("o_id", msgReq.getString(MsgkeyConstants.MSG_ACC_ID));
			mr.set("o_nm", msgReq.getString(MsgkeyConstants.ORG));
			mr.set("i_tp", msgReq.getString(MsgkeyConstants.SOURCE_TYPE));
			mr.set("p_tp", msgReq.getString(MsgkeyConstants.PAYLOAD_TYPE));
			mr.set("m_ctg", msgReq.getString(MsgkeyConstants.MSG_CATEGORY));
			mr.set("country", msgReq.getString(MsgkeyConstants.COUNTRY));
			mr.set("s_tm", Long.toString(System.currentTimeMillis()));
			LOGGER.error("Category{}", msgReq.getString(MsgkeyConstants.MSG_CATEGORY));
			LOGGER.error("Country{}", msgReq.getString(MsgkeyConstants.COUNTRY));
			if(null != dr.get("s_st")) {
				mr.set("s_st", dr.get("s_st").toString());
			}			
			jRedis.hset("wa_mt:" + msgReq.getString(MsgkeyConstants.FROM) + ":" + msgReq.getString(MsgkeyConstants.TO),
					messageObj.get("id").toString(), mr.toJSON());
		}
	}

	private MessageRequest getDR(HttpResponse httpResponse, MessageRequest msgReq) throws Exception {
		MessageRequest dr = new MessageRequest();
		dr.set("t_id", "");
		dr.set("m_id", msgReq.getString(MsgkeyConstants.TXN_ID));
		dr.set("p_id", msgReq.getString(MsgkeyConstants.PARENT_MSG_TXN_ID));
		dr.set("dir", "0");
		dr.set("c_tm", System.currentTimeMillis());
		dr.set("u_id", msgReq.getString(MsgkeyConstants.MSG_USER_ID));
		dr.set("acc_id", msgReq.getString(MsgkeyConstants.MSG_ACC_ID));
		dr.set("u_nm", msgReq.getString(MsgkeyConstants.USER_NAME));
		dr.set("o_id", msgReq.getString(MsgkeyConstants.ORG_ID));
		dr.set("o_nm", msgReq.getString(MsgkeyConstants.ORG));
		dr.set("i_tp", msgReq.getString(MsgkeyConstants.SOURCE_TYPE));
		dr.set("p_tp", msgReq.getString(MsgkeyConstants.PAYLOAD_TYPE));
		dr.set("from", msgReq.getString(MsgkeyConstants.FROM));
		dr.set("to", msgReq.getString(MsgkeyConstants.TO));
		dr.set("m_ctg", msgReq.getString(MsgkeyConstants.MSG_CATEGORY));
		dr.set("s_tm", System.currentTimeMillis());
		dr.set("price", msgReq.getString(MsgkeyConstants.JOB_COST));
		dr.set("country", msgReq.getString(MsgkeyConstants.COUNTRY));

		if (httpResponse != null && httpResponse.isOk()) {
			JSONParser jsonParser = new JSONParser();
			JSONObject responseBody = (JSONObject) jsonParser.parse(httpResponse.responseBody);
			JSONObject messageObj = (JSONObject) ((JSONArray) responseBody.get("messages")).get(0);
			dr.set("cnv_id", messageObj.get("id"));
			dr.set("s_st", ErrorCodes.SUBMITTED.eCode);
		} else {
			cancelFailedMessage(msgReq);
			dr.set("s_st", ErrorCodes.SUBMIT_FAILED.eCode);
			if (httpResponse != null) {
				dr.set("error_status_code", httpResponse.statusCode);
				dr.set("error_message", httpResponse.responseBody);
			}
		}
		return dr;
	}
	
	private void cancelFailedMessage(MessageRequest msgReq) throws Exception {
		String cancelUrl = System.getenv().getOrDefault(AppConstants.CANCEL_URL,
				System.getProperty(AppConstants.CANCEL_URL));
		LOGGER.error("cancel url : {} ", cancelUrl);
		HashMap<String, Object> hashApiKey = new HashMap<>();
		if (dispatcherImpl == null) {
			dispatcherImpl = HttpDispatcherImpl.getInstance("WA_Dispatcher", null);
		}
		String userId = null;
		if (msgReq.getString(MsgkeyConstants.MSG_USER_ID) != null
				&& !msgReq.getString(MsgkeyConstants.MSG_USER_ID).isEmpty()) {
			userId = msgReq.getString(MsgkeyConstants.MSG_USER_ID);
		} else {
			userId = msgReq.getString(MsgkeyConstants.ACC_USER_ID);
		}
		String bookingId = null;
		Map<String, Object> bookingIdMap = getMessageTrackerBookingIdData(
				msgReq.getString(MsgkeyConstants.PARENT_MSG_TXN_ID));
		if (msgReq.getString("conversation_type") != null) {
			if (msgReq.getString("conversation_type").equalsIgnoreCase("BUSINESS_INITIATED")) {
				if (bookingIdMap != null) {
					bookingId = bookingIdMap.get("business_initiated_bookingId").toString();
				}
			} else {
				if (bookingIdMap != null) {
					bookingId = bookingIdMap.get("user_initiated_bookingId").toString();
				}
			}
		}
		LOGGER.error("userId  : {}", userId);
		LOGGER.error("booking Id  : {}", bookingId);
		if (userId != null && bookingId != null && !userId.isEmpty() && !bookingId.isEmpty()) {
			MessageRequest message = new MessageRequest();
			message.set(MsgkeyConstants.MSG_USER_ID, userId);
			message.set("quantity", 1);
			message.set("bookingId", bookingId);
			LOGGER.error("message  : {}", message);
			HttpResponse httpResponse = dispatcherImpl.httpPostJsonBloking(cancelUrl, message.toJSON(), hashApiKey,
					LoginService.getJWTToken());
			LOGGER.error("Response Body : {} ", httpResponse.responseBody);
			if (httpResponse.isOk()) {
				LOGGER.error("Cancel call successful: {}", httpResponse);
			} else {
				LOGGER.error("Some Probelm in cancelling: {}", httpResponse);
			}
		} else {
			LOGGER.error("Booking Id / User Id not available to cancel the booking");
		}
		reduceTotalCountOrUpdateCampaignStatus(msgReq.getString(MsgkeyConstants.PARENT_MSG_TXN_ID), msgReq);
	}

	private Map<String, Object> getMessageTrackerBookingIdData(String parentJobId) {
		Map<String, Object> messageTrackerBookingIdMap = null;
		try {
			String messageTrackerBookingIdJson = WADispatcher.getjRedis().hget("wa_billing", parentJobId);
			LOGGER.error("Message tracker booking id for parent job id {} is {}", parentJobId,
					messageTrackerBookingIdJson);
			if (StringUtils.isNotBlank(messageTrackerBookingIdJson)) {
				MessageRequest messageTrackerBookingIdData = new MessageRequest(messageTrackerBookingIdJson,
						MessageRequest.MessageBufferType.json);
				messageTrackerBookingIdMap = messageTrackerBookingIdData.getMap();
			} else {
				LOGGER.error("Message tracker booking id not exist for parent job id {}", parentJobId);
			}
		} catch (Exception e) {
			LOGGER.error("Error ocured while getting message tracker ", e);
		}
		return messageTrackerBookingIdMap;
	}
    
    private void reduceTotalCountOrUpdateCampaignStatus(String campaignId, MessageRequest msgReq) {
		String count = WADispatcher.getjRedis().hget(campaignId + ":" + msgReq.getString(MsgkeyConstants.RECURRENCE_ID),
				MsgkeyConstants.TOTAL_COUNT);
		if (count != null && !count.isEmpty()) {
			Long totalCount = WADispatcher.getjRedis().hincrBy(
					campaignId + ":" + msgReq.getString(MsgkeyConstants.RECURRENCE_ID), MsgkeyConstants.TOTAL_COUNT,
					-1);
			if (totalCount == 0) {
				// update status as failed
				msgReq.set(MsgkeyConstants.STATUS, CampaignStatus.FAILED.getStatus());
				WADispatcher.getjRedis().zadd(SCHEDULER_TASK_B, System.currentTimeMillis(), campaignId + STATUS_UPDATE_1
						+ ":" + msgReq.getString(MsgkeyConstants.CHANNEL_TYPE) + ":" + msgReq.toJSON());
			} else {
				WADispatcher.getjRedis().hincrBy(campaignId + ":" + msgReq.getString(MsgkeyConstants.RECURRENCE_ID),
						"submit_failed_count", 1);
			}
		}
	}
	
	/**
	 * @param status
	 * @param msgReq
	 */
	private void generateFailureEntry(int status, MessageRequest msgReq) {
		try {
			if (status != 0 && status != ErrorCodes.ERROR_THROTTLE_LIMIT_EXCEEDED.eCode) {
				// TO REPORT FOR FAILURE
				WADispatcher.getjRedis().lpush("INTERNAL_FAILURE_REQ", msgReq.toJSON());
			}
		} catch (Exception e) {
			LOGGER.error("ERR: In while generating the failure entry ", e);
		}
	}

	/**
	 * @param partitionRecord
	 * @param msgReq
	 */
	private void setTopicMetaInfo(ConsumerRecord<String, String> partitionRecord, MessageRequest msgReq) {
		msgReq.set("ktopic", partitionRecord.topic());
		msgReq.set("kpartition", partitionRecord.partition());
		msgReq.set("koffset", partitionRecord.offset());
	}

	public boolean isIsnextPoll() {
		return isnextPoll;
	}

	public void setIsnextPoll(boolean isnextPoll) {
		this.isnextPoll = isnextPoll;
	}

	public String getConsumerInstanceId() {
		return consumerInstanceId;
	}

	public void setConsumerInstanceId(String consumerInstanceId) {
		this.consumerInstanceId = consumerInstanceId;
	}

	public long getCurrentOffset() {
		return currentOffset.get();
	}

	public boolean isTaskFinish() {
		return taskFinish;
	}

	public void setTaskFinish(boolean taskFinish) {
		this.taskFinish = taskFinish;
	}

	public long getSecondEpoch() {
		return secondEpoch;
	}

	public void setSecondEpoch(long secondEpoch) {
		this.secondEpoch = secondEpoch;
	}

}
