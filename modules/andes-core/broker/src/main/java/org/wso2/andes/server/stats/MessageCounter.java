/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.andes.server.stats;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.ClusterResourceHolder;

import java.util.Map;
import java.util.Set;

/**
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
    public static MessageCounter getInstance() {
        return messageCounter;
    }

    /**
     * Update the current state of a message and update the counts of the same.
     *
     * @param messageID The message ID in the broker.
     * @param messageCounterType The message status.
     * @param queueName The queue name the message is in.
     * @param timeMillis The status change occured time.
     */
    public void updateOngoingMessageStatus(long messageID, MessageCounterKey.MessageCounterType messageCounterType, String queueName, long timeMillis) {
        MessageCounterKey messageCounterKey = new MessageCounterKey(queueName, messageCounterType);
        try{
            messageStore.addMessageStatusChange(messageID, timeMillis, messageCounterKey);
        } catch (Exception e) {
            log.error("Error recording message status change.", e);
        }

    }

    /**
     * Get stats of each message published, delivered and acknowledged times.
     *
     * @param queueName The queue name the message is in.
     * @param minDate The min value for the time range to retrieve in timemillis.
     * @param maxDate The max value for the time range to retrieve in timemillis.
     * @param minMessageId The min messageId to retrieve (use for paginated data retrieval. Else null).
     * @param limit Limit of the number of records to retrieve. The messages will be retrieved in ascending messageId order. If null MAX value of long will be set.
     * @param compareAllStatuses Compare all the statuses that are changed within the given period, else only the published time will be compared.
     * @return The message stats. Map<MessageId, Map{Published:time, Delivered:time, Aknowledged:time, queue_name:name}>
     */
    public Set<MessageStatus> getOnGoingMessageStatus(String queueName, Long minDate, Long maxDate, Long minMessageId, Long limit, Boolean compareAllStatuses) {
        Set<MessageStatus> messageStatuses = null;

        try {
            messageStatuses =  messageStore.getMessageStatuses(queueName, minDate, maxDate, minMessageId, limit, compareAllStatuses);
        } catch (AndesException e) {
            log.error("Error retrieving message statuses.", e);
        }
        return messageStatuses;
    }

    /**
     * Get message count within the given time range and given queue.
     *
     * @param queueName The queue name the message is in.
     * @param minDate The min value for the time range to retrieve in timemillis.
     * @param maxDate The max value for the time range to retrieve in timemillis.
     * @return The message count.
     */
    public Long getMessageStatusCounts(String queueName, Long minDate, Long maxDate) {
        Long count = 0L;
        try {
            count =  messageStore.getMessageStatusCount(queueName, minDate, maxDate);
        } catch (AndesException e) {
            log.error("Error retrieving message status counts.", e);
        }

        return count;
    }

    /**
     * Get message status change times for a given message status.
     *
     * @param queueName The queue name the message is in.
     * @param minDate The min value for the time range to retrieve in timemillis.
     * @param maxDate The max value for the time range to retrieve in timemillis.
     * @param minMessageId The min messageId to retrieve (used for paginated data retrieval. Else null).
     * @param limit Limit of the number of records to retrieve. The messages will be retrieved in ascending messageId order. If null MAX value of long will be set.
     * @param messageCounterType The message status.
     * @return Map<MessageId MessageStatusChangeTimeMillis>
     */
    public Map<Long, Long> getMessageStatusChangeTimes(String queueName, Long minDate, Long maxDate, Long minMessageId, Long limit, MessageCounterKey.MessageCounterType messageCounterType) {
        Map<Long, Long> messageStatuses = null;

        try {
            messageStatuses =  messageStore.getMessageStatusChangeTimes(queueName, minDate, maxDate, minMessageId, limit, messageCounterType);
        } catch (AndesException e) {
            log.error("Error retrieving message statuses.", e);
        }
        return messageStatuses;
    }
}
