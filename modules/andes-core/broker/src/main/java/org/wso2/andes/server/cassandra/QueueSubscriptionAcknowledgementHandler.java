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
package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQStoreException;
import org.wso2.andes.amqp.QpidAMQPBridge;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.queue.QueueEntry;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * TODO handle message timeouts
 */
public class QueueSubscriptionAcknowledgementHandler {
    /** 
     * this is a delivery performance counter
     */
    private MessageStore messageStore;

    private Map<Long, QueueMessageTag> deliveryTagMessageMap = new ConcurrentHashMap<Long, QueueMessageTag>();

    private Map<Long, QueueMessageTag> sentMessagesMap = new ConcurrentHashMap<Long, QueueMessageTag>();

    private SortedMap<Long, Long> timeStampAckedMessageIdMap = new ConcurrentSkipListMap<Long, Long>();

    private SortedMap<Long, Long> timeStampMessageIdMap = new ConcurrentSkipListMap<Long, Long>();

    private QueueMessageTagCleanupJob cleanupJob;

    private Map<Long, Long> messageDeliveryTimeRecorderMap = new ConcurrentHashMap<Long, Long>();

    private long timeOutInMills = 10000;

    private long ackedMessageTimeOut = 3 * timeOutInMills;

    private static Log log = LogFactory.getLog(QueueSubscriptionAcknowledgementHandler.class);

    private OnflightMessageTracker messageTracker = OnflightMessageTracker.getInstance();


    public QueueSubscriptionAcknowledgementHandler(MessageStore messageStore, String queue) {
        this.messageStore = messageStore;
    }

    /**
     * Check the eligibility of message to be sent to the client
     * @param messageMetadata
     * @param deliveryTag
     * @param channel
     * @return  boolean eligible to be sent
     * @throws AMQException
     */
    public boolean checkAndRegisterSent(AndesMessageMetadata messageMetadata, long deliveryTag, AMQChannel channel) throws AMQException {

        return messageTracker.testAndAddMessage(messageMetadata, deliveryTag, channel);

    }

    public void handleAcknowledgement(AMQChannel channel, QueueEntry queueEntry) {
        try {
            try {
                /**
                 * When the message is acknowledged it is informed to Andes Kernel
                 */
                QpidAMQPBridge.getInstance().ackReceived(queueEntry.getMessage().getMessageNumber(),
                                                         queueEntry.getQueue().getName(),
                                                         false);
                // We first delete the message so even this fails in tracker, no harm done. Also decrement message count.
                messageTracker.ackReceived(channel.getId(), queueEntry.getMessage().getMessageNumber());
                channel.decrementNonAckedMessageCount();

            } catch (AMQStoreException e) {
                log.error("Error while handling the ack for " + queueEntry.getMessage().getMessageNumber(), e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }        
    }

    private class QueueMessageTag {

        private long deliveryTag;

        private long messageId;

        private String queue;

        public QueueMessageTag(String queue, long deliveryTag, long msgId) {
            this.queue = queue;
            this.deliveryTag = deliveryTag;
            this.messageId = msgId;
        }

        public long getDeliveryTag() {
            return deliveryTag;
        }

        public long getMessageId() {
            return messageId;
        }

        public String getQueue() {
            return queue;
        }
    }

    /**
     * This will clean up TimedOut QueueMessageTags from the Maps
     */
    private class QueueMessageTagCleanupJob implements Runnable {

        private boolean running = true;

        @Override
        public void run() {

            long currentTime = System.currentTimeMillis();

            while (running) {
                try {
                    synchronized (messageStore) {
                        // Here timeStampMessageIdMap.firstKey() is the oldest
                        if (timeStampMessageIdMap.firstKey() + timeOutInMills <= currentTime) {
                            // we should handle timeout
                            SortedMap<Long, Long> headMap = timeStampMessageIdMap.headMap(currentTime - timeOutInMills);
                            if (headMap.size() > 0) {
                                for (Long l : headMap.keySet()) {
                                    long mid = headMap.get(l);
                                    QueueMessageTag mtag = sentMessagesMap.get(mid);

                                    if (mtag != null) {

                                        long deliveryTag = mtag.getDeliveryTag();
                                        if (deliveryTagMessageMap.containsKey(deliveryTag)) {
                                            QueueMessageTag tag = deliveryTagMessageMap.get(deliveryTag);

                                            if (tag != null) {

                                                if (sentMessagesMap.containsKey(tag.getMessageId())) {
                                                    sentMessagesMap.remove(tag.getMessageId());
                                                }
                                                deliveryTagMessageMap.remove(deliveryTag);
                                            }

                                        }
                                    }
                                }

                                for (Long key : headMap.keySet()) {
                                    timeStampMessageIdMap.remove(key);
                                }
                            }

                            if (timeStampAckedMessageIdMap.firstKey() + ackedMessageTimeOut < currentTime) {
                                SortedMap<Long, Long> headAckedMessagesMap = timeStampAckedMessageIdMap
                                        .headMap(currentTime - ackedMessageTimeOut);

                                for (long key : headAckedMessagesMap.keySet()) {
                                    timeStampAckedMessageIdMap.remove(key);
                                }

                            }

                        }

                    }
                } catch (Exception e) {
                    log.error("Error while running Queue Message Tag Cleanup Task", e);
                } finally {
                    try {
                        Thread.sleep(60 * 1000);
                    } catch (InterruptedException e) {
                        // Ignore
                    }
                }
            }

        }

        public void stop() {

        }
    }

}
