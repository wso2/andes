/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * DisruptorCachedContent has access to content cache built by the disruptor
 */
public class RetainedContent implements AndesContent {
    /**
     * Content length of the message
     */
    private final int contentLength;

    /**
     * Used to store message content in memory
     */
    private Map<Integer, AndesMessagePart> messagePartCache;
    private long messageID;

    public RetainedContent(Map<Integer, AndesMessagePart> messageParts, int contentLength, long messageID) {
        this.messagePartCache = messageParts;
        this.contentLength = contentLength;
        this.messageID = messageID;
    }

    /**
     * This method gives access to content of the retained message.
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
            int chunkNumber = currentBytePosition / AMQPUtils.DEFAULT_CONTENT_CHUNK_SIZE;
            int chunkStartByteIndex = chunkNumber * AMQPUtils.DEFAULT_CONTENT_CHUNK_SIZE;
            int positionToReadFromChunk = currentBytePosition - chunkStartByteIndex;

            AndesMessagePart messagePart = getMessagePart(chunkStartByteIndex);

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
     * Get Message part for byte index
     *
     * @param indexToQuery
     *         Byte index of the content
     * @return Content chunk
     */
    private AndesMessagePart getMessagePart(int indexToQuery) throws AndesException {
        AndesMessagePart messagePart = messagePartCache.get(indexToQuery);

        if (null == messagePart) {
            throw new AndesException(
                    "Content not cached for chunk index " + indexToQuery + " for message ID " + messageID);
        }
        return messagePart;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getContentLength() {
        return contentLength;
    }
}