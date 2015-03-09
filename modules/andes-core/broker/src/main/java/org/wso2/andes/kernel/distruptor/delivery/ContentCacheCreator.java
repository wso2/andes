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

import java.util.List;
import java.util.Map;

/**
 * Disruptor handler used to load message content to memory
 */
public class ContentCacheCreator {
    /**
     * Class Logger
     */
    private static final Logger log = Logger.getLogger(ContentCacheCreator.class);

    /**
     * Load content for a message in to the memory
     *
     * @param eventDataList Event data holder
     * @param messageIdList messageIDs of content to be retrieved from
     * @throws Exception
     */
    public void onEvent(List<DeliveryEventData> eventDataList, List<Long> messageIdList) throws Exception {

        Map<Long, List<AndesMessagePart>> contentListMap = MessagingEngine.getInstance().getContent(messageIdList);

        for (DeliveryEventData deliveryEventData : eventDataList) {

            AndesMessageMetadata message = deliveryEventData.getMetadata();
            long messageID =  message.getMessageID();

            StorableMessageMetaData metaData = AMQPUtils.convertAndesMetadataToAMQMetadata(message);
            int contentSize = metaData.getContentSize();
            List<AndesMessagePart> contentList = contentListMap.get(messageID);
            if(null == contentList ) {
                throw new AndesException("Empty message parts received while retrieving message content for" +
                        "message id " + messageID);
            }

            for (AndesMessagePart messagePart : contentList) {
                deliveryEventData.addMessagePart(messagePart.getOffSet(), messagePart);
            }

            deliveryEventData.setAndesContent(new DisruptorCachedContent(deliveryEventData, contentSize));

            if (log.isTraceEnabled()) {
                log.trace("All content read for message " + messageID);
            }
        }

    }
}
