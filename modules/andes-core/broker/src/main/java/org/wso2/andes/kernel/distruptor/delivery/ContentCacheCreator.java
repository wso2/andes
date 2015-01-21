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

package org.wso2.andes.kernel.distruptor.delivery;

import com.lmax.disruptor.EventHandler;
import org.apache.log4j.Logger;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DisruptorCachedContent;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.store.StorableMessageMetaData;

/**
 * Disruptor handler used to load message content to memory
 */
public class ContentCacheCreator implements EventHandler<DeliveryEventData> {
    /**
     * Class Logger
     */
    private static final Logger log = Logger.getLogger(ContentCacheCreator.class);

    /**
     * Used to identify the sequence IDs that need to be processed by this handler
     */
    private final long ordinal;

    /**
     * Total number of ContentCacheCreators
     */
    private final long numberOfConsumers;

    public ContentCacheCreator(long ordinal, long numberOfConsumers) {
        this.ordinal = ordinal;
        this.numberOfConsumers = numberOfConsumers;
    }

    /**
     * Load content for a message in to the memory
     *
     * @param deliveryEventData
     *         Event data holder
     * @param sequence
     *         Sequence number of the disruptor event
     * @param endOfBatch
     *         Indicate end of batch
     * @throws Exception
     */
    @Override
    public void onEvent(DeliveryEventData deliveryEventData, long sequence, boolean endOfBatch) throws Exception {
        // Filter tasks assigned to this handler
        if ((sequence % numberOfConsumers) == ordinal) {
            AndesMessageMetadata message = deliveryEventData.getMetadata();
            long messageID =  message.getMessageID();

            StorableMessageMetaData metaData = AMQPUtils.convertAndesMetadataToAMQMetadata(message);
            int contentSize = metaData.getContentSize();
            int writtenSize = 0;

            AndesMessagePart messagePart = MessagingEngine.getInstance()
                                                          .getMessageContentChunk(messageID, writtenSize);
            if(null == messagePart ) {
                throw new AndesException("Empty message part received while retrieving message content.");
            }

            // Load data to memory
            deliveryEventData.addMessagePart(writtenSize,messagePart);
            writtenSize = writtenSize + messagePart.getDataLength();

            // Continue until all content for the message is retrieved
            while (writtenSize < contentSize) {
                messagePart = MessagingEngine.getInstance()
                                             .getMessageContentChunk(messageID, writtenSize);
                if(null == messagePart) {
                    throw new AndesException("Empty message part received while retrieving message content.");
                }

                deliveryEventData.addMessagePart(writtenSize,messagePart);
                writtenSize = writtenSize + messagePart.getDataLength();
            }

            deliveryEventData.setAndesContent(new DisruptorCachedContent(deliveryEventData, contentSize));

            if (log.isTraceEnabled()) {
                log.trace("All content read for message " + messageID);
            }
        }
    }
}
