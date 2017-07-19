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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.inbound;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessagePart;

import java.util.ArrayList;
import java.util.List;

/**
 * This class will convert incoming message content chunks into content chunks
 * that can managed by Andes core. That is, this will change the chunk size.
 */
public class ContentChunkHandler implements EventHandler<InboundEventContainer> {

    private static Log log = LogFactory.getLog(ContentChunkHandler.class);

    private final int maxChunkSize;

    private ContentCompressionStrategy chunkStrategy;

    /**
     * Creates a {@link ContentChunkHandler} object
     * @param maxChunkSize maximum allowed chunk size to be stored in DB
     */
    ContentChunkHandler(int maxChunkSize, ContentCompressionStrategy chunkStrategy) {
        this.maxChunkSize = maxChunkSize;
        this.chunkStrategy = chunkStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onEvent(InboundEventContainer event, long sequence, boolean endOfBatch) throws Exception {

        // If the content is already taken there is no point in further processing
        // therefore ignore
        if (!event.availableForContentProcessing()) {
            return;
        }

        switch (event.getEventType()) {
            case MESSAGE_EVENT:
                // Since there's only one message in inbound message list it can be retrieved without iterate through
                // the list.
                AndesMessage message = resizeContentChunks(event.popMessage(), sequence);
                // Modified andes message(with chunk resize) will be added back to inbound message list.
                event.addMessage(message);
                break;
            case TRANSACTION_ENQUEUE_EVENT:
                handleTransaction(event, sequence);
                break;
            case DTX_ONE_PHASE_COMMIT_EVENT:
            case DTX_COMMIT_EVENT:
                handleDtxEvent(event, sequence);
                break;
            default:
                if (log.isDebugEnabled()) {
                    log.debug("Message chunk ignored for event type " + event.getEventType());
                }
                break;
        }
    }

    /**
     * Resize content chunks for transactional messages
     * @param event {@link InboundEventContainer}
     * @param sequence Disruptor sequence number
     */
    private void handleDtxEvent(InboundEventContainer event, long sequence) {
        ArrayList<AndesMessage> enqueueList = event.getDtxBranch().getEnqueueList();
        for (AndesMessage andesMessage: enqueueList) {
            resizeContentChunks(andesMessage, sequence);
        }
    }

    /**
     * Transaction related message content chunks are re-sized by this method and added
     * to the {@link org.wso2.andes.kernel.disruptor.inbound.InboundTransactionEvent} message queue
     *
     * @param event    {@link org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer}
     * @param sequence Disruptor sequence number
     */
    private void handleTransaction(InboundEventContainer event, long sequence) {
        AndesMessage message = resizeContentChunks(event.popMessage(), sequence);
        event.getTransactionEvent().enqueueMessage(message);
    }

    /**
     * Resize the content of the messages provided into chunks that can be stored in DB
     *
     * @param message messages to be processed
     * @param sequence sequence number of the ring buffer
     * @return andes message
     */
    private AndesMessage resizeContentChunks(AndesMessage message, long sequence) {
        if (log.isDebugEnabled()) {
            log.debug("[ " + sequence + " ] Content chunk resize for " + message.getContentChunkList().size() +
                    " message ");
        }

        // andes message chunks will be handled based on configured chunk strategy.
        ContentPartHolder contentChunkPair = chunkStrategy.ContentChunkStrategy(message);
        message.setChunkList(
                resizeChunks(contentChunkPair.getAndesMessagePartList(),contentChunkPair.getContentLength()));

        return message;
    }

    /**
     * This resize content chunks of the provided messages. Resized chunks will have a maximum length of
     * maxChunkSize
     * <p/>
     * Algorithm
     * <p/>
     * While iterating through each original content chunk it copies the content to a new data chunk that will
     * have a maximum length of maxChunkSize.
     * <p/>
     * This can handle content with maximum chunk size equal to, less than or greater than the maxChunkSize
     *
     * @param partList      Original content chunk list
     * @param contentLength total content length
     * @return list of resized content chunks
     */
    List<AndesMessagePart> resizeChunks(List<AndesMessagePart> partList, int contentLength) {

        List<AndesMessagePart> chunkList = new ArrayList<>();
        int written = 0;    // Written bytes to new content chunks
        int totalRemainingLength = contentLength;
        byte[] data = null;
        int startPos = 0;   // Start position of destination data array. (for copying)

        for (AndesMessagePart chunk : partList) {

            // Chunk can be added directly to the new chunk list if there is no remaining data array left
            // and the following conditions are met
            //
            // Chunk is either equal to the the maxChunkSize or the last chunk of the original that is
            // less than the maxChunkSize.
            if (data == null && (chunk.getDataLength() == maxChunkSize ||
                    (chunk.getDataLength() < maxChunkSize && chunk.getDataLength() == totalRemainingLength))) {

                chunk.setOffSet(written);
                chunkList.add(chunk);
                written = written + chunk.getDataLength();
                totalRemainingLength = contentLength - written;
                continue; // Whole chunk is written. Move to next iteration
            }

            // When a larger chunk is found, split it to smaller chunks.
            int chunkStartPos = 0;
            int chunkRemainingLength = chunk.getDataLength();
            while (chunkRemainingLength >= maxChunkSize) {

                if (null == data) {
                    data = new byte[maxChunkSize];
                    startPos = 0;
                }

                System.arraycopy(chunk.getData(), chunkStartPos, data, startPos, maxChunkSize - startPos);

                AndesMessagePart newChunk = new AndesMessagePart();
                newChunk.setMessageID(chunk.getMessageID());
                newChunk.setOffSet(written);
                newChunk.setData(data);
                chunkList.add(newChunk);

                written = written + data.length;
                data = null;
                chunkStartPos = chunkStartPos + (maxChunkSize - startPos);
                chunkRemainingLength = chunkRemainingLength - (maxChunkSize - startPos);
                startPos = 0;
                totalRemainingLength = contentLength - written;
            }

            // This is either original chunks left over part is less than maxChunkSize
            // or a original chunk it self is less than maxChunkSize
            while (chunkRemainingLength > 0) {
                if (null == data) {
                    int arrayLength;
                    if (chunkRemainingLength == totalRemainingLength) {
                        arrayLength = chunkRemainingLength;
                    } else if (totalRemainingLength >= maxChunkSize) {
                        arrayLength = maxChunkSize;
                    } else {
                        arrayLength = totalRemainingLength;
                    }
                    data = new byte[arrayLength];
                }

                int writeSize;
                if (chunkRemainingLength <= (data.length - startPos)) {
                    writeSize = chunkRemainingLength;
                } else {
                    writeSize = data.length - startPos;
                }
                System.arraycopy(chunk.getData(), chunkStartPos, data, startPos, writeSize);
                startPos = startPos + writeSize;
                chunkStartPos = chunkStartPos + writeSize;
                chunkRemainingLength = chunkRemainingLength - writeSize;
                if (startPos == data.length) {
                    AndesMessagePart newChunk = new AndesMessagePart();
                    newChunk.setMessageID(chunk.getMessageID());
                    newChunk.setOffSet(written);
                    newChunk.setData(data);
                    chunkList.add(newChunk);
                    written = written + data.length;
                    totalRemainingLength = contentLength - written;
                    data = null;
                    startPos = 0;
                }
            }
        }
        return chunkList;
    }

}
