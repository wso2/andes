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

package org.wso2.andes.store;

import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.apache.log4j.Logger;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.slot.RecoverySlotCreator;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.transaction.xa.Xid;

/**
 * Implementation of {@link MessageStore} which observes failures such is
 * connection errors. Any {@link MessageStore} implementation specified in
 * broker.xml will be wrapped by this class.
 */
public class FailureObservingMessageStore implements MessageStore {

    /**
     * {@link MessageStore} specified in broker.xml
     */
    private MessageStore wrappedInstance;

    /**
     * Future referring to a scheduled task which check the connectivity to the
     * store.
     * Used to cancel the periodic task after store becomes operational.
     */
    private ScheduledFuture<?> storeHealthDetectingFuture;

    /**
     * Variable that holds the operational status of the context store. True if store is operational.
     */
    private AtomicBoolean isStoreAvailable;

    /**
     * {@link FailureObservingStoreManager} to notify about store's operational status.
     */
    private FailureObservingStoreManager failureObservingStoreManager;

    private static final Logger log = Logger.getLogger(FailureObservingMessageStore.class);

    /**
     * {@inheritDoc}
     */
    public FailureObservingMessageStore(MessageStore messageStore, FailureObservingStoreManager manager) {
        this.wrappedInstance = messageStore;
        this.storeHealthDetectingFuture = null;
        isStoreAvailable = new AtomicBoolean(true);
        failureObservingStoreManager = manager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore,
            ConfigurationProperties connectionProperties) throws AndesException {
        try {
            return wrappedInstance.initializeMessageStore(contextStore, connectionProperties);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        try {
            wrappedInstance.storeMessagePart(partList);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        try {
            return wrappedInstance.getContent(messageId, offsetValue);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongObjectHashMap<List<AndesMessagePart>> getContent(LongArrayList messageIDList) throws AndesException {
        try {
            return wrappedInstance.getContent(messageIDList);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    @Override
    public void storeMessages(List<AndesMessage> messageList) throws AndesException {
        try {
            wrappedInstance.storeMessages(messageList);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToQueue(long messageId, String currentQueueName, String targetQueueName)
            throws AndesException {
        try {
            wrappedInstance.moveMetadataToQueue(messageId, currentQueueName, targetQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException {
        try {
            wrappedInstance.moveMetadataToDLC(messageId, dlcQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(List<AndesMessageMetadata> messages, String dlcQueueName)
            throws AndesException {
        try {
            wrappedInstance.moveMetadataToDLC(messages, dlcQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMetadataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {
        try {
            wrappedInstance.updateMetadataInformation(currentQueueName, metadataList);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getMetadata(long messageId) throws AndesException {
        try {
            return wrappedInstance.getMetadata(messageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DeliverableAndesMetadata> getMetadataList(Slot slot, String storageQueueName, long firstMsgId,
            long lastMsgID) throws AndesException {
        try {
            return wrappedInstance.getMetadataList(slot, storageQueueName, firstMsgId, lastMsgID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    public long getMessageCountForQueueInRange(final String storageQueueName, long firstMessageId, long lastMessageId)
            throws AndesException {
        try {
            return wrappedInstance.getMessageCountForQueueInRange(storageQueueName, firstMessageId, lastMessageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    public int recoverSlotsForQueue(final String storageQueueName, long firstMsgId, int count,
                                    RecoverySlotCreator.CallBack callBack) throws AndesException {
        try {
            return wrappedInstance.recoverSlotsForQueue(storageQueueName,firstMsgId,count,callBack);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String storageQueueName, long firstMsgId,
            int count) throws AndesException {
        try {
            return wrappedInstance.getNextNMessageMetadataFromQueue(storageQueueName, firstMsgId, count);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataForQueueFromDLC(String storageQueueName,
            String dlcQueueName, long firstMsgId, int count) throws AndesException {
        try {
            return wrappedInstance
                    .getNextNMessageMetadataForQueueFromDLC(storageQueueName, dlcQueueName, firstMsgId, count);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromDLC(String dlcQueueName, long firstMsgId, int count)
            throws AndesException {
        try {
            return wrappedInstance.getNextNMessageMetadataFromDLC(dlcQueueName, firstMsgId, count);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(String storageQueueName, List<AndesMessageMetadata> messagesToRemove)
            throws AndesException {
        try {
            wrappedInstance.deleteMessageMetadataFromQueue(storageQueueName, messagesToRemove);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(final String storageQueueName, List<AndesMessageMetadata> messagesToRemove)
            throws AndesException {
        try {
            wrappedInstance.deleteMessages(storageQueueName, messagesToRemove);

            //Tracing message activity
            if (MessageTracer.isEnabled()) {
                for (AndesMessageMetadata message : messagesToRemove) {
                    MessageTracer.trace(message.getMessageID(), storageQueueName, MessageTracer.MESSAGE_DELETED);
                }
            }

        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }

    }

    /**
     *{@inheritDoc}
     */
    @Override
    public void deleteMessages(List<Long> messagesToRemove) throws AndesException {
        try {
            wrappedInstance.deleteMessages(messagesToRemove);

            //Tracing message activity
            if (MessageTracer.isEnabled()) {
                for (Long messageId : messagesToRemove) {
                    MessageTracer.trace(messageId, "", MessageTracer.MESSAGE_DELETED);
                }
            }

        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDLCMessages(List<AndesMessageMetadata> messagesToRemove) throws AndesException {
        try {
            wrappedInstance.deleteDLCMessages(messagesToRemove);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getExpiredMessages(long lowerBoundMessageID, String queueName) throws AndesException {
        try {
            return wrappedInstance.getExpiredMessages(lowerBoundMessageID,queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getExpiredMessagesFromDLC(long messageCount) throws AndesException {
        try {
            return wrappedInstance.getExpiredMessagesFromDLC(messageCount);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic,
            String destination) throws AndesException {
        try {
            wrappedInstance.addMessageToExpiryQueue(messageId, expirationTime, isMessageForTopic, destination);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deleteAllMessageMetadata(String storageQueueName) throws AndesException {
        try {
            return wrappedInstance.deleteAllMessageMetadata(storageQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int clearDLCQueue(String dlcQueueName) throws AndesException {
        try {
            return wrappedInstance.clearDLCQueue(dlcQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongArrayList getMessageIDsAddressedToQueue(String storageQueueName, Long startMessageID)
            throws AndesException {
        try {
            return wrappedInstance.getMessageIDsAddressedToQueue(storageQueueName, startMessageID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(String storageQueueName) throws AndesException {
        try {
            wrappedInstance.addQueue(storageQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Integer> getMessageCountForAllQueues(List<String> queueNames) throws AndesException {
        try {
            return wrappedInstance.getMessageCountForAllQueues(queueNames);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String storageQueueName) throws AndesException {
        try {
            return wrappedInstance.getMessageCountForQueue(storageQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueueInDLC(String storageQueueName, String dlcQueueName) throws AndesException {
        try {
            return wrappedInstance.getMessageCountForQueueInDLC(storageQueueName, dlcQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForDLCQueue(String dlcQueueName) throws AndesException {
        try {
            return wrappedInstance.getMessageCountForDLCQueue(dlcQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        try {
            wrappedInstance.resetMessageCounterForQueue(storageQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeQueue(String storageQueueName) throws AndesException {
        try {
            wrappedInstance.removeQueue(storageQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeLocalQueueData(String storageQueueName) {
        wrappedInstance.removeLocalQueueData(storageQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String storageQueueName, long incrementBy) throws AndesException {
        try {
            wrappedInstance.incrementMessageCountForQueue(storageQueueName, incrementBy);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String storageQueueName, long decrementBy) throws AndesException {
        try {
            wrappedInstance.decrementMessageCountForQueue(storageQueueName, decrementBy);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeRetainedMessages(Map<String, AndesMessage> retainMap) throws AndesException {
        try {
            wrappedInstance.storeRetainedMessages(retainMap);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllRetainedTopics() throws AndesException {
        try {
            return wrappedInstance.getAllRetainedTopics();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {
        try {
            return wrappedInstance.getRetainedContentParts(messageID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DeliverableAndesMetadata getRetainedMetadata(String destination) throws AndesException {
        try {
            return wrappedInstance.getRetainedMetadata(destination);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        wrappedInstance.close();
        failureObservingStoreManager.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeDtxRecords(Xid xid, List<AndesMessage> enqueueRecords, List<AndesAckData> dequeueRecords)
            throws AndesException {
        try {
            wrappedInstance.storeDtxRecords(xid, enqueueRecords, dequeueRecords);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}.
     * <p>
     * Alters the behavior where
     * <ol>
     * <li>checks the operational status of the wrapped context store</li>
     * <li>if context store is operational it will cancel the periodic task</li>
     * </ol>
     */
    @Override
    public boolean isOperational(String testString, long testTime) {

        if (wrappedInstance.isOperational(testString, testTime)) {
            isStoreAvailable.set(true);
            if (storeHealthDetectingFuture != null) {
                // we have detected that store is operational therefore
                // we don't need to run the periodic task to check weather store
                // is available.
                storeHealthDetectingFuture.cancel(false);
                storeHealthDetectingFuture = null;
            }
            failureObservingStoreManager.notifyMessageStoreOperational(this);
            return true;
        }
        return false;
    }

    /**
     * A convenient method to notify all {@link StoreHealthListener}s that
     * context store became offline
     *
     * @param e the exception occurred.
     */
    private void notifyFailures(AndesStoreUnavailableException e) {

        if (isStoreAvailable.compareAndSet(true,false)){
            log.warn("Message store became non-operational");
            failureObservingStoreManager.notifyMessageStoreNonOperational(e, wrappedInstance);
            storeHealthDetectingFuture = failureObservingStoreManager.scheduleHealthCheckTask(this);
        }
    }

}
