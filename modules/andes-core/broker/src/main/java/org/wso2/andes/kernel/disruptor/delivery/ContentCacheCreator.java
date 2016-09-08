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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.delivery;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.gs.collections.api.iterator.MutableLongIterator;
import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import com.gs.collections.impl.set.mutable.primitive.LongHashSet;
import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DisruptorCachedContent;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolMessage;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This is used to load message content to memory.
 */
public class ContentCacheCreator {
    /**
     * Class Logger for logging information, error and warning.
     */
    private static final Logger log = Logger.getLogger(ContentCacheCreator.class);

    /**
     * Maximum content chunk size stored in DB
     */
    private final int maxChunkSize;

    /**
     * Keeps track of ids of messages which this handler couldn't load payload
     * content (from message store)
     */
    private final LongArrayList failedContentRetrivals;

    /**
     * Guava based cache used to avoid fetching content for same message id in non-durable topics
     */
    private final Cache<Long, DisruptorCachedContent> contentCache;

    /**
     * Creates a {@link org.wso2.andes.kernel.disruptor.delivery.ContentCacheCreator} object
     *
     * @param maxContentChunkSize maximum content chunk size stored in DB
     */
    public ContentCacheCreator(int maxContentChunkSize) {
        this.maxChunkSize = maxContentChunkSize;

        Integer maximumSize = AndesConfigurationManager
                .readValue(AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_CONTENT_CACHE_MAXIMUM_SIZE);
        Integer expiryTime = AndesConfigurationManager
                .readValue(AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_CONTENT_CACHE_EXPIRY_TIME);

        contentCache = CacheBuilder.newBuilder().expireAfterWrite(expiryTime, TimeUnit.SECONDS).maximumSize(maximumSize)
                .concurrencyLevel(1).build();

        failedContentRetrivals = new LongArrayList();

    }


    /**
     * Load content for a message in to the memory.
     *
     * @param eventDataList List of delivery event data
     * @throws AndesException Thrown when getting content from the message store.
     */

    public void onEvent(List<DeliveryEventData> eventDataList) throws AndesException {

        LongHashSet messagesToFetch = new LongHashSet();
        List<DeliveryEventData> messagesWithoutCachedContent = new ArrayList<>();
        String storageQueueName = null;

       // HashMap messageMap = new HashMap();
        HashMap<String, ArrayList<Long>> messageMap = new HashMap<>();
        
        for (DeliveryEventData deliveryEventData : eventDataList) {
            ProtocolMessage metadata = deliveryEventData.getMetadata();
            long messageID = metadata.getMessageID();
            int contentLength = metadata.getMessage().getMessageContentLength();
            storageQueueName = metadata.getMessage().getSlot().getStorageQueueName();

            ArrayList<Long> messageList = messageMap.get(storageQueueName);

            if (null == messageList) {

                messageList = new ArrayList<>();
                messageMap.put(storageQueueName, messageList);
            }
                messageList.add(messageID);
                //messageList.get(contentLength);
 //               messageList.add(messageID);
//                messageList = messageMap.get(storageQueueName);
//                messageMap.put(storageQueueName, messageList);


            //messageMap.put(storageQueueName, messageID);
            if (contentLength > 0) {

               // DisruptorCachedContent content = contentCache.getIfPresent(messageID);
                DisruptorCachedContent content = null;

                if (null != content) {
                    deliveryEventData.setAndesContent(content);

                    if (log.isTraceEnabled()) {
                        log.trace("Content read from cache for message " + messageID);
                    }

                } else {
                    // Add to the list to fetch later
                    messagesToFetch.add(messageID);
                    messagesWithoutCachedContent.add(deliveryEventData);
                }

            } else {
                // user has sent a message with out content. trying to read from
                // the message storage is not required.
                continue;
            }

        }

        LongArrayList containMessagesToFetch = new LongArrayList();
        containMessagesToFetch.addAll(messagesToFetch);

        LongObjectHashMap<List<AndesMessagePart>> contentListMap = MessagingEngine.getInstance()
                .getContent( messageMap );

        for (DeliveryEventData deliveryEventData : messagesWithoutCachedContent) {

            ProtocolMessage metadata = deliveryEventData.getMetadata();
            long messageID = metadata.getMessageID();

            // We check again for content put in cache in the previous iteration
            DisruptorCachedContent content;

//            if (null != content) {
//                deliveryEventData.setAndesContent(content);
//
//                if (log.isTraceEnabled()) {
//                    log.trace("Content read from cache for message " + messageID);
//                }
//
//                continue;
//            }

            int contentSize = metadata.getMessage().getMessageContentLength();
            List<AndesMessagePart> contentList = contentListMap.get(messageID);

            if (null != contentList) {
                Map<Integer, AndesMessagePart> messagePartMap = new HashMap<>(contentList.size());

                for (AndesMessagePart messagePart : contentList) {
                    messagePartMap.put(messagePart.getOffset(), messagePart);
                }

                content = new DisruptorCachedContent(messagePartMap, contentSize, maxChunkSize);
//                contentCache.put(messageID, content);
                deliveryEventData.setAndesContent(content);

                if (log.isTraceEnabled()) {
                    log.trace("All content read for message " + messageID);
                }
            } else {
                // potential scenario this could happen is when there is a split
                // brain scenario (with two coordinator working with same set of
                // messages
                // in parallel. e.g. when this node tries to send messages,
                // another node (in other network partition) sends these
                // messages and then deletes content and metadata from message
                // store
                recordFailedMessageContentRetrievalError(deliveryEventData);
            }

            //Tracing message
            MessageTracer.trace(metadata.getMessage(), MessageTracer.CONTENT_READ);
            logFailedMessageContentRetreivalErrors();
        }
    }

    /**
     * Keeps track of message for which this handle couldn't get message contents.
     *
     * @param deliveryEventData information about the message.
     */
    private void recordFailedMessageContentRetrievalError(DeliveryEventData deliveryEventData) {
        deliveryEventData.reportExceptionOccurred();
        failedContentRetrivals.add(deliveryEventData.getMetadata().getMessageID());
    }

    /**
     * Print a error log message which this handler couldn't find payloads in
     * database.
     * This will not throw an error since disruptor batch event handler will not
     * give deliveryEventData (in the list being processed to next handler)
     */
    private void logFailedMessageContentRetreivalErrors() {

        if (!failedContentRetrivals.isEmpty()) {

            StringBuilder errorMsg = new StringBuilder("message content not found for message ids : ");

            MutableLongIterator iterator = failedContentRetrivals.longIterator();
            while (iterator.hasNext()) {
                long messageId = iterator.next();
                errorMsg.append(messageId).append(',');
            }

            failedContentRetrivals.clear();
            log.error(errorMsg.toString());
        }

    }

}
