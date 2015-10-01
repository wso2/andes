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
import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.*;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Disruptor handler used to load message content to memory.
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
     * Guava based cache used to avoid fetching content for same message id in non-durable topics
     */
    private final Cache<Long, DisruptorCachedContent> contentCache;

    /**
     * Creates a {@link org.wso2.andes.kernel.disruptor.delivery.ContentCacheCreator} object
     * @param maxContentChunkSize maximum content chunk size stored in DB
     */
    public ContentCacheCreator(int maxContentChunkSize) {
        this.maxChunkSize = maxContentChunkSize;

        Integer maximumSize = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_CONTENT_CACHE_MAXIMUM_SIZE);
        Integer expiryTime = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_CONTENT_CACHE_EXPIRY_TIME);

        contentCache = CacheBuilder.newBuilder().expireAfterWrite(expiryTime, TimeUnit.SECONDS).maximumSize(maximumSize)
                                   .concurrencyLevel(1).build();
    }

    /**
     * Load content for a message in to the memory.
     *
     * @param eventDataList
     *      List of delivery event data
     * @throws AndesException
     *         Thrown when getting content from the message store.
     */
    public void onEvent(List<DeliveryEventData> eventDataList) throws AndesException {

        Set<Long> messagesToFetch = new HashSet<>();
        List<DeliveryEventData> messagesWithoutContent = new ArrayList<>();

        for (DeliveryEventData deliveryEventData: eventDataList) {
            ProtocolMessage metadata = deliveryEventData.getMetadata();
            long messageID =  metadata.getMessageID();

            DisruptorCachedContent content = contentCache.getIfPresent(messageID);

            if (null != content) {
                deliveryEventData.setAndesContent(content);

                if (log.isTraceEnabled()) {
                    log.trace("Content read from cache for message " + messageID);
                }

            } else {
                // Add to the list to fetch later
                messagesToFetch.add(messageID);
                messagesWithoutContent.add(deliveryEventData);
            }
        }

        Map<Long, List<AndesMessagePart>> contentListMap = MessagingEngine.getInstance().getContent(new ArrayList<>(messagesToFetch));

        for (DeliveryEventData deliveryEventData : messagesWithoutContent) {

            ProtocolMessage metadata = deliveryEventData.getMetadata();
            long messageID =  metadata.getMessageID();

            // We check again for content put in cache in the previous iteration
            DisruptorCachedContent content = contentCache.getIfPresent(messageID);

            if (null != content) {
                deliveryEventData.setAndesContent(content);

                if (log.isTraceEnabled()) {
                    log.trace("Content read from cache for message " + messageID);
                }

                continue;
            }

            int contentSize = metadata.getMessage().getMessageContentLength();
            List<AndesMessagePart> contentList = contentListMap.get(messageID);

            if (null != contentList) {
                Map<Integer, AndesMessagePart> messagePartMap = new HashMap<>(contentList.size());

                for (AndesMessagePart messagePart : contentList) {
                    messagePartMap.put(messagePart.getOffset(), messagePart);
                }

                content = new DisruptorCachedContent(messagePartMap, contentSize, maxChunkSize);
                contentCache.put(messageID, content);
                deliveryEventData.setAndesContent(content);

                if (log.isTraceEnabled()) {
                    log.trace("All content read for message " + messageID);
                }
            } else if (log.isDebugEnabled()) {
                throw new AndesException(
                        "Empty message parts received while retrieving message content for message id " + messageID);
            }

            //Tracing message
            MessageTracer.trace(metadata.getMessage(), MessageTracer.CONTENT_READ);
        }
    }
}
