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

package org.wso2.andes.kernel;

import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.disruptor.delivery.DeliveryEventData;

import java.nio.ByteBuffer;

/**
 * DisruptorCachedContent has access to content cache built by the disruptor
 */
public class DisruptorCachedContent implements AndesContent {
    /**
     * event data holder is used to access cached content
     */
    private final DeliveryEventData eventDataHolder;

    /**
     * Content length of the message
     */
    private final int contentLength;

    /**
     * Maximum chunk size allowed within Andes core
     */
    private final int maxChunkSize;

    /**
     * Create a {@link org.wso2.andes.kernel.DisruptorCachedContent} object
     * @param eventDataHolder {@link org.wso2.andes.kernel.distruptor.delivery.DeliveryEventData}
     * @param contentLength length of the content to be cached
     * @param maxChunkSize maximum chunk size of the stored content
     */
    public DisruptorCachedContent(DeliveryEventData eventDataHolder, int contentLength, int maxChunkSize) {
        this.eventDataHolder = eventDataHolder;
        this.contentLength = contentLength;
        this.maxChunkSize = maxChunkSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int putContent(int offset, ByteBuffer destinationBuffer) throws AndesException {
        int written = 0;
        int remainingBufferSpace = destinationBuffer.remaining();
        int remainingContent = contentLength - offset;
        int maxRemaining = Math.min(remainingBufferSpace, remainingContent);

        int currentBytePosition = offset;

        while (maxRemaining > written) {
            // This is an integer division
            int chunkNumber = currentBytePosition / maxChunkSize;
            int chunkStartByteIndex = chunkNumber * maxChunkSize;
            int positionToReadFromChunk = currentBytePosition - chunkStartByteIndex;

            AndesMessagePart messagePart = eventDataHolder.getMessagePart(chunkStartByteIndex);

            int messagePartSize = messagePart.getDataLength();
            int numOfBytesAvailableToRead = messagePartSize - positionToReadFromChunk;
            int remaining = maxRemaining - written;
            int numOfBytesToRead;

            if (remaining > numOfBytesAvailableToRead) {
                numOfBytesToRead = numOfBytesAvailableToRead;
            } else {
                numOfBytesToRead = remaining;
            }

            destinationBuffer.put(messagePart.getData(), positionToReadFromChunk, numOfBytesToRead);

            written = written + numOfBytesToRead;
            currentBytePosition = currentBytePosition + numOfBytesToRead;
        }

        return written;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getContentLength() {
        return contentLength;
    }
}
