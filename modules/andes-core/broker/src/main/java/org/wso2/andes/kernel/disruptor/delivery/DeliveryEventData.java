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

import com.lmax.disruptor.EventFactory;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.LocalSubscription;

import java.util.HashMap;
import java.util.Map;

/**
 * Delivery event data holder. This is used to store and retrieve data between different handlers
 */
public class DeliveryEventData {
    /**
     * Recipient of the message
     */
    private LocalSubscription localSubscription;

    /**
     * Metadata of the message
     */
    private AndesMessageMetadata metadata;

    /**
     * Used to store message content in memory
     */
    private Map<Integer, AndesMessagePart> messagePartCache;

    /**
     * Indicate if any error occurred during processing handlers
     */
    private boolean errorOccurred;

    /**
     * Provide access to message content
     */
    private AndesContent andesContent;

    public DeliveryEventData() {
        this.messagePartCache = new HashMap<Integer, AndesMessagePart>();
        this.errorOccurred = false;
    }

    /**
     * Factory used by the Disruptor to create delivery event data
     *
     * @return Delivery event data holder factory
     */
    public static EventFactory<DeliveryEventData> getFactory() {
        return new DeliveryEventDataFactory();
    }

    /**
     * Add message part to memory map. This is done by the ContentCacheCreator
     *
     * @param index
     *         Byte index of the message content part
     * @param messagePart
     *         Content chunk
     */
    public void addMessagePart(int index, AndesMessagePart messagePart) {
        messagePartCache.put(index, messagePart);
    }

    /**
     * Clear state data for current instance. This should be called by the last event handler for the ring-buffer
     */
    public void clearData() {
        messagePartCache.clear();
        errorOccurred = false;
    }

    /**
     * Get the content message object
     *
     * @return content object
     */
    public AndesContent getAndesContent() {
        return andesContent;
    }

    /**
     * Set message content object
     *
     * @param andesContent
     *         content object
     */
    public void setAndesContent(AndesContent andesContent) {
        this.andesContent = andesContent;
    }

    /**
     * Get local subscription for current event
     *
     * @return Local subscription
     */
    public LocalSubscription getLocalSubscription() {
        return localSubscription;
    }

    /**
     * Set AMQP local subscription for current event
     *
     * @param localSubscription
     *         Local subscription
     */
    public void setLocalSubscription(LocalSubscription localSubscription) {
        this.localSubscription = localSubscription;
    }

    /**
     * Get Message part for byte index
     *
     * @param indexToQuery
     *         Byte index of the content
     * @return Content chunk
     */
    public AndesMessagePart getMessagePart(int indexToQuery) throws AndesException {
        AndesMessagePart messagePart = messagePartCache.get(indexToQuery);

        if (null == messagePart) {
            throw new AndesException(
                    "Content not cached for chunk index " + indexToQuery + " for message ID " + metadata
                            .getMessageID());
        }
        return messagePart;
    }

    /**
     * Get metadata for current event
     *
     * @return Metadata
     */
    public AndesMessageMetadata getMetadata() {
        return metadata;
    }

    /**
     * Set metadata for current event
     *
     * @param metadata
     *         Metadata
     */
    public void setMetadata(AndesMessageMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Used to indicate errors by handlers
     */
    public void reportExceptionOccurred() {
        errorOccurred = true;
    }

    @Override
    public String toString() {
        return "Message ID: " + metadata.getMessageID() + ", Cache size: " + messagePartCache.size()
               + ", Error occurred : " + isErrorOccurred();
    }

    /**
     * Check if any errors reported by previous handlers
     *
     * @return true if error occurred
     */
    public boolean isErrorOccurred() {
        return errorOccurred;
    }

    /**
     * Factory class for delivery event data
     */
    public static class DeliveryEventDataFactory implements EventFactory<DeliveryEventData> {
        @Override
        public DeliveryEventData newInstance() {
            return new DeliveryEventData();
        }
    }
}
