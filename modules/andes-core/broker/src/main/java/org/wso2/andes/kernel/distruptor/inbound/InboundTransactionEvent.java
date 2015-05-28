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

package org.wso2.andes.kernel.distruptor.inbound;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.InboundEventManager;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.slot.SlotMessageCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the Andes transaction event related class. This event object handles
 * the life cycle of a single transaction coming from the protocol to Andes.
 */
public class InboundTransactionEvent {

    private static Log log = LogFactory.getLog(InboundTransactionEvent.class);

    private final MessageStore.AndesTransaction transaction;
    private final InboundEventManager eventManager;
    private EventType eventType;
    private SettableFuture<Boolean> taskCompleted;
    private final List<AndesMessageMetadata> metadataList;

    /**
     * Maximum batch size for a transaction. Limit is set for content size of the batch.
     * Exceeding this limit will lead to a failure in the subsequent commit request.
     */
    private final int maxBatchSize;

    /**
     * maximum connections reserved for transactional  tasks
     */
    private final int maxConnections;

    /**
     * content batch cached size at a given point in time.
     */
    private int currentBatchSize;

    /**
     * Total number of active transactions at a given time (That is transactions that have
     * started enqueue messages but not yet committed or rollback)
     */
    private static AtomicInteger currentConnectionCount;

    /**
     * Reference to the channel of the publisher
     */
    private final AndesChannel channel;

    /**
     * Supported state events
     */
    private enum EventType {

        /** Transaction commit related event type */
        TX_COMMIT_EVENT,

        /** Transaction rollback related event type */
        TX_ROLLBACK_EVENT,

        /** Transaction message enqueue related event type */
        TX_ENQUEUE_EVENT,

        /** close the current transaction and release all resources */
        TX_CLOSE_EVENT
    }

    /**
     * Transaction object to do a transaction
     * @param transaction AndesTransaction
     * @param eventManager InboundEventManager
     */
    public InboundTransactionEvent(MessageStore.AndesTransaction transaction, InboundEventManager eventManager,
                                   int maxBatchSize, int maxConnections, AndesChannel channel ) {
        this.transaction = transaction;
        this.eventManager = eventManager;
        metadataList = new ArrayList<AndesMessageMetadata>();
        taskCompleted = SettableFuture.create();
        this.maxBatchSize = maxBatchSize;
        this.maxConnections = maxConnections;
        this.channel = channel;
    }

    /**
     * This will commit the batched transacted message to the persistence storage using Andes
     * underlying event manager.
     *
     * This is a blocking call
     * @throws AndesException
     */
    public void commit() throws AndesException {

        if(currentBatchSize > maxBatchSize) {
            currentBatchSize = 0;
            throw new AndesException("Current enqueued batch size exceeds maximum transactional batch size of " +
            maxBatchSize + " bytes." );
        }

        if (log.isDebugEnabled()) {
            log.debug("Prepare for commit. Channel id: " + channel.getId());
        }

        eventType = EventType.TX_COMMIT_EVENT;
        taskCompleted = SettableFuture.create();

        // Publish to event manager for processing
        eventManager.requestTransactionOperation(this);
        // Make the call blocking
        waitForCompletion();
        currentBatchSize = 0;
    }

    /**
     * This will rollback the transaction. This is done using Andes underlying event manager
     * This is a blocking call.
     *
     * @throws AndesException
     */
    public void rollback() throws AndesException {
        if (log.isDebugEnabled()) {
            log.debug("Prepare for rollback. Channel: " + channel.getId());
        }

        eventType = EventType.TX_ROLLBACK_EVENT;
        taskCompleted = SettableFuture.create();

        // Publish to event manager for processing
        eventManager.requestTransactionOperation(this);
        // Make the call blocking
        waitForCompletion();
        currentBatchSize = 0;
    }

    /**
     * Add a message to a transaction. Added messages will be persisted in DB only when
     * commit is invoked. Underlying event manager will add the message to the the transaction
     *
     * This is a asynchronous call
     * @param message AndesMessage
     * @param channel AndesChannel
     */
    public void enqueue(AndesMessage message, AndesChannel channel) {
        eventType = EventType.TX_ENQUEUE_EVENT;
        eventManager.processTransactionEnqueue(this, message, channel);
    }

    /**
     * Release all resources used by transaction object. This should be called when the transactional session is
     * closed. This is to prevent unwanted resource usage (DB connections etc) after closing
     * a transactional session.
     *
     * @throws AndesException
     */
    public void close() throws AndesException {
        eventType = EventType.TX_CLOSE_EVENT;
        taskCompleted = SettableFuture.create();
        eventManager.requestTransactionOperation(this);
        waitForCompletion();
    }

    /**
     * Update internal state of the transaction according to the prepared event of the transaction
     * This method is call by the state event handler.
     */
    void updateState() {
        switch (eventType) {
            case TX_COMMIT_EVENT:
                commitTransactionToDB();
                break;
            case TX_ROLLBACK_EVENT:
                rollbackTransactionFromDB();
                break;
            case TX_CLOSE_EVENT:
                closeTransactionFromDB();
                break;
            default:
                log.debug("Event " + eventType + " ignored.");
                break;
        }
    }

    private void closeTransactionFromDB() {
        try {
            transaction.close();
            metadataList.clear();
            taskCompleted.set(true);
        } catch (Throwable t) {
            // Exception is passed to the the caller of get method of settable future
            taskCompleted.setException(t);
            metadataList.clear();
        }
    }

    /**
     * This is only a package specific method only called from Andes underlying event manager.
     *
     * @param message AndesMessage
     * @throws AndesException
     */
    void enqueuePreProcessedMessage(AndesMessage message) throws AndesException {
        for (AndesMessagePart messagePart: message.getContentChunkList()) {
            currentBatchSize = currentBatchSize + messagePart.getDataLength();
        }

        if (currentBatchSize > maxBatchSize) {
            metadataList.clear(); // if max batch size exceeds invalidate commit.
        } else {
            metadataList.add(message.getMetadata());
            transaction.enqueue(message);

            if (log.isDebugEnabled()) {
                log.debug("Enqueue message with message id " + message.getMetadata().getMessageID() + " for transaction ");
            }
        }
    }

    private void commitTransactionToDB() {
        try {
            if(currentBatchSize > maxBatchSize) {
                throw new AndesException("Commit batch size exceeds maximum commit batch size of " +
                        maxBatchSize + " bytes");
            }

            transaction.commit();

            // update slot information for transaction related messages
            for(AndesMessageMetadata metadata: metadataList){
                SlotMessageCounter.getInstance().recordMetadataCountInSlot(metadata);
            }
            metadataList.clear();
            taskCompleted.set(true);
        } catch (Throwable t) {
            // Exception is passed to the the caller of get method of settable future
            taskCompleted.setException(t);
            metadataList.clear();
        }
    }

    private void rollbackTransactionFromDB() {
        try {
            transaction.rollback();
            metadataList.clear();
            taskCompleted.set(true);
        } catch (Throwable t) {
            taskCompleted.setException(t);
        }
    }

    private Boolean waitForCompletion() throws AndesException {
        try {
            return taskCompleted.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new AndesException("Error occurred while processing transaction event " + eventType, e);
        }
        return false;
    }
}
