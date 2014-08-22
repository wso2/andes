package org.wso2.andes.server.stats;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.ClusterResourceHolder;

import java.util.*;

/**
 * Created by Akalanka on 7/30/14.
 * This class is responsible for keeping track of message rates and ongoing message statuses.
 */
public final class MessageCounter {

    private Log log = LogFactory.getLog(MessageCounter.class);

    MessageStore messageStore;

    private static final MessageCounter messageCounter = new MessageCounter();

    /**
     * Make the constructor private for the singleton class and initialized data structures.
     */
    private MessageCounter() {
        if (ClusterResourceHolder.getInstance().getClusterConfiguration().isInMemoryMode()) { //InMemoryMode
            this.messageStore = MessagingEngine.getInstance().getInMemoryMessageStore();
        } else {
            this.messageStore = MessagingEngine.getInstance().getDurableMessageStore();
        }
    }

    /**
     * Get the singleton instance of the Message Counter.
     * @return The MessageCounter singleton instance.
     */
    public static synchronized MessageCounter getInstance() {
        return messageCounter;
    }

    /**
     * Update the current state of a message and update the counts of the same.
     *
     * @param messageID The message ID in the broker.
     * @param messageCounterType The message status.
     * @param queueName The queue name the message is in.
     */
    public void updateOngoingMessageStatus(long messageID, MessageCounterKey.MessageCounterType messageCounterType, String queueName, long timeMillis) {
        MessageCounterKey messageCounterKey = new MessageCounterKey(queueName, messageCounterType);
        try{
            messageStore.addMessageStatusChange(messageID, timeMillis, messageCounterKey);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
        }

    }

    /**
     * Generate the json key for the rate that is used for json objects which pass graph data.
     *
     * The JSON value keys will be distinct for each message status.
     * They are...
     * RatePublishCounter
     * RateDeliverCounter
     * RateAcknowledgedCounter
     *
     * This is to enable the chart to show all three or selected types at once in a single chart.

     * @param messageCounterType The message status.
     * @return The message rate JSON data
     */
    private String generateRateJsonKey(MessageCounterKey.MessageCounterType messageCounterType) {
        return "Rate" + messageCounterType.getType();
    }

    /**
     * Get the graph data as a JSON String.
     *
     * @param queueName The queue name to get data
     * @return The JSON String of Graph Data
     */
    public String getGraphData(String queueName, Long minDate, Long maxDate) {
        JSONArray graphData = new JSONArray();
        try {
            Map<MessageCounterKey.MessageCounterType, Map<Long, Integer>> total = messageStore.getMessageRates(queueName, minDate, maxDate);

            for (Map.Entry<MessageCounterKey.MessageCounterType, Map<Long, Integer>> currEntry : total.entrySet()) {
                String rateJsonKey = generateRateJsonKey(currEntry.getKey());

                Map<Long, Integer> currValues = currEntry.getValue();

                Iterator<Map.Entry<Long,Integer>> valueItr= currValues.entrySet().iterator();
                Long previousKey = null;

                while(valueItr.hasNext()) {
                    Map.Entry<Long, Integer> currSecondData = valueItr.next();
                    Long currKey = currSecondData.getKey();

                    if (previousKey != null) {
                        long secondsDiff = (currKey - previousKey) / 1000;
                        if ( secondsDiff > 1 ) {
                            for (long i = secondsDiff; i > 1; i--) {
                                previousKey += 1000;
                                JSONObject fillEmpty = new JSONObject();
                                fillEmpty.put("Date", previousKey);
                                fillEmpty.put(rateJsonKey, 0);
                                graphData.add(fillEmpty);
                            }
                        }
                    }

                    JSONObject secondRate = new JSONObject();
                    secondRate.put("Date", currKey);
                    secondRate.put(rateJsonKey, currSecondData.getValue());
                    graphData.add(secondRate);

                    previousKey = currKey;
                }
            }
        } catch (AndesException e) {
            e.printStackTrace();
        }

        return graphData.toJSONString();
    }

    /**
     * Get statuses of on going messages.
     * @return Map of on going message status Map<MessageID, MessageCounterKey>
     */
    public Map<Long, Map<String, String>> getOnGoingMessageStatus(String queueName, Long minDate, Long maxDate) {
        try {
            return messageStore.getMessageStatuses(queueName, minDate, maxDate);
        } catch (AndesException e) {
            e.printStackTrace();
            return null;
        }
    }
}
