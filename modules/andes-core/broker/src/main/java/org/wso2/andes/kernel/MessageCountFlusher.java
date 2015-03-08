/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Updates message count in batches. Can be used as a scheduled task as well to update message counts
 */
public class MessageCountFlusher implements Runnable {

    private static Log log = LogFactory.getLog(MessageCountFlusher.class);

    /**
     * Map to keep message count difference not flushed to disk of each queue
     */
    private Map<String, AtomicInteger> messageCountDifferenceMap;

    /**
     * message count will be flushed to DB when count difference reach this val
     */
    private int messageCountFlushNumberGap;

    /**
     * Used to increment and decrement message counts
     */
    private MessageStore messageStore;

    /**
     * Creates a message count flusher object to store message counts in store
     * @param messageStore MessageStore
     * @param messageCountFlushNumberGap batch size of the count update for a given queue.
     */
    public MessageCountFlusher(MessageStore messageStore, int messageCountFlushNumberGap) {
        this.messageStore = messageStore;
        this.messageCountFlushNumberGap = messageCountFlushNumberGap;
        messageCountDifferenceMap = new ConcurrentHashMap<String, AtomicInteger>();
    }

    /**
     * Running this task as a scheduled task will flush all pending message count update request to DB at given
     * time intervals
     */
    @Override
    public void run() {
        for (Map.Entry<String, AtomicInteger> entry : messageCountDifferenceMap.entrySet()) {
            flushToStore(entry.getKey(), entry.getValue());
        }
    }

    /**
     * increment message count of queue. Flush if difference is in tab
     *
     * @param queueName   name of the queue to increment count
     * @param incrementBy increment count by this value
     */
    public void incrementQueueCount(String queueName, int incrementBy) {

        if(log.isDebugEnabled()) {
            log.debug("Increment count by " + incrementBy + " for queue " + queueName);
        }
        updateQueueCount(queueName, incrementBy);
    }

    /**
     * decrement queue count. Flush if difference is in tab
     *
     * @param queueName   name of the queue to decrement count
     * @param decrementBy decrement count by this value, This should be a positive value
     */
    public void decrementQueueCount(String queueName, int decrementBy) {
        if(log.isDebugEnabled()) {
            log.debug("Decrement count by " + decrementBy + " for queue " + queueName);
        }
        updateQueueCount(queueName, -decrementBy);
    }

    /**
     * Increment or decrement the queue count by given delta. Count update is reflected in store when either the count
     * update batch size (messageCountFlushNumberGap) is reached or scheduled message count update task is triggered
     *
     * @param queueName name of the queue to update the queue count
     * @param delta     value to be decrement or increment. Positive value to increment and vice versa
     */
    private void updateQueueCount(String queueName, int delta) {
        AtomicInteger difference = messageCountDifferenceMap.get(queueName);
        if (null == difference) {
            difference = new AtomicInteger(0);
            messageCountDifferenceMap.put(queueName, difference);
        }

        // Decrement value and get the current value
        int newDifference = difference.addAndGet(delta);
        // We use the new value at the time we decrement and make decisions on that value.
        // we flush this value to store in 100 message tabs.
        if (newDifference % messageCountFlushNumberGap == 0) {
            // "newDifference" is already updated in "difference"  as well
            flushToStore(queueName, difference);
        }
    }

    /**
     * Update the store with new message counts
     *
     * @param queueName  count is updated in this queue
     * @param difference count is updated by this value in store
     */
    private void flushToStore(String queueName, AtomicInteger difference) {
        int count = 0;
        try {
            // Get the current count and make decisions from that value.
            // If not, within the execution of method some other thread might update the value and subsequent reads
            // of the value will be different leading to unpredictable behaviour.
            // We get the current count and reset the value at the same time so that this call is going to
            // take care of the current count only. Any update happen during the method execution will be reflected
            // in subsequent calls to this method.
            count = difference.getAndSet(0);

            if (count > 0) {
                if(log.isDebugEnabled()) {
                    log.debug("Increment store count by " + count + " for queue " + queueName);
                }
                messageStore.incrementMessageCountForQueue(queueName, count);
            } else if (count < 0) {
                if(log.isDebugEnabled()) {
                    log.debug("Decrement store count by " + count + " for queue " + queueName);
                }
                messageStore.incrementMessageCountForQueue(queueName, count);
            }
        } catch (AndesException e) {
            // On error add back the count. Since the operation didn't run correctly. Next call to this method might
            // get the chance to update the value properly.
            difference.addAndGet(count);
            log.error("Error while updating message counts for queue " + queueName, e);
        }
    }
}
