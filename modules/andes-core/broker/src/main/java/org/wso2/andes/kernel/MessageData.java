/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.slot.Slot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class represents a bean where message delivery statistics are kept
 */
public class MessageData {

    private static Log log = LogFactory.getLog(MessageData.class);

    public final long msgID;

    public final String destination;
    /**
     * timestamp at which the message was taken from store for processing.
     */
    public long timestamp;
    /**
     * Timestamp after which the message should expire.
     */
    public long expirationTime;
    /**
     * timestamp at which the message entered the first gates of the broker.
     */
    public long arrivalTime;
    /**
     * Number of scheduled deliveries. concurrently modified whenever the message is scheduled to be delivered.
     */
    public AtomicInteger numberOfScheduledDeliveries;
    /**
     * Number of deliveries done of this message in each amq channel.
     */
    private Map<UUID, Integer> channelToNumOfDeliveries;
    /**
     * State transition of the message
     */
    public List<MessageStatus> messageStatus;
    /**
     * Parent slot of message.
     */
    public Slot slot;

    /**
     * Number of acks pending for the message
     */
    private AtomicInteger pendingAckCount;

    /**
     * Indicate if the metadata should not be used.
     */
    private boolean stale;

    /**
     * Check if metadata is stale
     * @return true if message is stale
     */
    public boolean isStale() {
        return stale;
    }

    public void markAsStale() {
        stale = true;
    }

    public MessageData(long msgID, Slot slot, String destination, long timestamp,
                    long expirationTime, MessageStatus messageStatus,
                    long arrivalTime) {
        this.msgID = msgID;
        this.slot = slot;
        this.destination = destination;
        this.timestamp = timestamp;
        this.expirationTime = expirationTime;
        this.channelToNumOfDeliveries = new ConcurrentHashMap<UUID, Integer>();
        this.messageStatus = Collections.synchronizedList(new ArrayList<MessageStatus>());
        this.messageStatus.add(messageStatus);
        this.numberOfScheduledDeliveries = new AtomicInteger(0);
        this.pendingAckCount = new AtomicInteger(0);
        this.arrivalTime = arrivalTime;
        this.stale = false;
    }

    public boolean isExpired() {
        if (expirationTime != 0L) {
            long now = System.currentTimeMillis();
            return (now > expirationTime);
        } else {
            return false;
        }
    }

    public void addMessageStatus(MessageStatus status) {
        messageStatus.add(status);
    }

    public String getStatusHistory() {
        String history = "";
        for (MessageStatus status : messageStatus) {
            history = history + status + ">>";
        }
        return history;
    }

    public MessageStatus getLatestState() {
        MessageStatus latest = null;
        if (messageStatus.size() > 0) {
            latest = messageStatus.get(messageStatus.size() - 1);
        }
        return latest;
    }

    public boolean isRedelivered(UUID channelID) {
        Integer numOfDeliveries = channelToNumOfDeliveries.get(channelID);
        return numOfDeliveries > 1;
    }

    public int incrementDeliveryCount(UUID channelID) {
        Integer numOfDeliveries = channelToNumOfDeliveries.get(channelID);
        if (numOfDeliveries == null) {
            numOfDeliveries = 0;
        }
        numOfDeliveries++;
        channelToNumOfDeliveries.put(channelID, numOfDeliveries);
        return numOfDeliveries;
    }

    public Set<UUID> getAllDeliveredChannels() {
        return channelToNumOfDeliveries.keySet();
    }

    public int decrementDeliveryCount(UUID channelID) {
        decrementPendingAckCount();
        Integer numOfDeliveries = channelToNumOfDeliveries.get(channelID);
        numOfDeliveries--;
        if (numOfDeliveries > 0) {
            channelToNumOfDeliveries.put(channelID, numOfDeliveries);
        } else {
            channelToNumOfDeliveries.remove(channelID);
        }
        return numOfDeliveries;
    }

    public int getNumOfDeliveires4Channel(UUID channelID) {
         /* Since sometimes Broker tries to send stored messages when it initialised a subscription
            so then it returns null value for that subscription's channel's amount of deliveries,
            Since we need to the evaluate the rules before we send message, therefore we have to ignore the null value,
            then we have to check the number of deliveries for the particular channel */
        if (null != channelToNumOfDeliveries.get(channelID)) {
            return channelToNumOfDeliveries.get(channelID);
        } else {
            return 0;
        }
    }

    /**
     * Increment pending ack count
     *
     * @param count
     *         number of acks pending
     */
    public void incrementPendingAckCount(int count) {
        pendingAckCount.addAndGet(count);
    }

    /**
     * Decrement pending ack count
     */
    public void decrementPendingAckCount() {
        pendingAckCount.decrementAndGet();
    }

    /**
     * Check if  all acks were received for the message
     *
     * @return True if all the acks were received or else False
     */
    public boolean allAcksReceived() {
        return pendingAckCount.get() == 0;
    }
}
