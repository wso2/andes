/*
 * Copyright (c) 2014-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotCoordinator;
import org.wso2.andes.kernel.slot.SlotCoordinatorCluster;
import org.wso2.andes.kernel.slot.SlotCoordinatorStandalone;
import org.wso2.andes.kernel.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.kernel.slot.SlotManagerStandalone;
import org.wso2.andes.kernel.slot.SlotMessageCounter;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.MessageIdGenerator;
import org.wso2.andes.server.cluster.coordination.TimeStampBasedMessageIdGenerator;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.thrift.MBThriftClient;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class will handle all message related functions of WSO2 Message Broker
 */
public class MessagingEngine {

    /**
     * Logger for MessagingEngine
     */
    private static final Logger log;

    /**
     * Static instance of MessagingEngine
     */
    private static MessagingEngine messagingEngine;

    /**
     * Cluster wide unique message id generator
     */
    private MessageIdGenerator messageIdGenerator;

    /**
     * Reference to MessageStore. This holds the messages received by andes
     */
    private MessageStore messageStore;


    /**
     * Slot coordinator who is responsible of coordinating with the SlotManager
     */
    private SlotCoordinator slotCoordinator;

    /**
     * Expiry manager which is responsible for update DLC info in db tables
     */
    private MessageExpiryManager messageExpiryManager;

    /**
     * private constructor for singleton pattern
     */
    private MessagingEngine() {
    }


    static {
        log = Logger.getLogger(MessagingEngine.class);
        messagingEngine = new MessagingEngine();
    }

    /**
     * MessagingEngine is used for executing operations related to messages. (Storing,
     * retrieving etc) This works as an API for different transports implemented by MB
     *
     * @return MessagingEngine
     */
    public static MessagingEngine getInstance() {
        return messagingEngine;
    }

    /**
     * Initialises the MessagingEngine with a given durableMessageStore. Message retrieval and
     * storing strategy will be set according to the configurations by calling this.
     *
     * @param messageStore MessageStore
     * @throws AndesException
     */
    public void initialise(MessageStore messageStore, MessageExpiryManager messageExpiryManager)
            throws AndesException {

        configureMessageIDGenerator();

        this.messageStore = messageStore;
        this.messageExpiryManager = messageExpiryManager;


        /*
        Initialize the SlotCoordinator
         */
        if (AndesContext.getInstance().isClusteringEnabled()) {
            slotCoordinator = new SlotCoordinatorCluster();
        } else {
            slotCoordinator = new SlotCoordinatorStandalone();
        }
    }

    /**
     * Return the requested chunk of a message's content.
     *
     * @param messageID       Unique ID of the Message
     * @param offsetInMessage The offset of the required chunk in the Message content.
     * @return AndesMessagePart
     * @throws AndesException
     */
    public AndesMessagePart getMessageContentChunk(long messageID, int offsetInMessage) throws AndesException {
        return messageStore.getContent(messageID, offsetInMessage);
    }

    /**
     * Read content for given message metadata list
     *
     * @param messageIdList message id list for the content to be retrieved
     * @return <code>Map<Long, List<AndesMessagePart>></code> Message id and its corresponding message part list
     * @throws AndesException
     */
    public LongObjectHashMap<List<AndesMessagePart>> getContent(LongArrayList messageIdList) throws AndesException {
        return messageStore.getContent(messageIdList);

    }

    /**
     * Persist received messages. Implemented {@link org.wso2.andes.kernel.MessageStore} will be used to
     * persist the messages
     *
     * @param messageList List of {@link org.wso2.andes.kernel.AndesMessage} to persist
     * @throws AndesException
     */
    public void messagesReceived(List<AndesMessage> messageList) throws AndesException {
        messageStore.storeMessages(messageList);
    }

    /**
     * Get a single metadata object
     *
     * @param messageID id of the message
     * @return AndesMessageMetadata
     * @throws AndesException
     */
    public AndesMessageMetadata getMessageMetaData(long messageID) throws AndesException {
        return messageStore.getMetadata(messageID);
    }

    /**
     * Move the messages meta data in the given message to the Dead Letter Channel and
     * remove those meta data from the original queue.
     *
     * @param messageToRemove      Message to be removed
     * @param destinationQueueName The original destination queue of the message
     * @throws AndesException
     */
    public void moveMessageToDeadLetterChannel(DeliverableAndesMetadata messageToRemove, String destinationQueueName)
            throws AndesException {
        String deadLetterQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(destinationQueueName);

        messageExpiryManager.moveMetadataToDLC(messageToRemove.getMessageID(), deadLetterQueueName);

        // Increment count by 1 in DLC and decrement by 1 in original queue

        messageToRemove.markAsDLCMessage();
        messageToRemove.getSlot().decrementPendingMessageCount();

        //Tracing message activity
        MessageTracer.trace(messageToRemove.getMessageID(), destinationQueueName, MessageTracer.MOVED_TO_DLC);
    }

    /**
     * Delete messages from store. No message state changes are involved here.
     *
     * @param messagesToRemove list of messages to remove
     * @throws AndesException
     */
    public void deleteMessages(Collection<AndesMessageMetadata> messagesToRemove) throws AndesException {
        Map<String, List<AndesMessageMetadata>> storageSeparatedMessages = new HashMap<>();

        for (AndesMessageMetadata message : messagesToRemove) {
            List<AndesMessageMetadata> messagesOfStorageQueue = storageSeparatedMessages
                    .get(message.getStorageQueueName());
            if (null == messagesOfStorageQueue) {
                messagesOfStorageQueue = new ArrayList<>();
            }
            messagesOfStorageQueue.add(message);
            storageSeparatedMessages.put(message.getStorageQueueName(), messagesOfStorageQueue);
        }

        //delete message content along with metadata
        for (Map.Entry<String, List<AndesMessageMetadata>> entry : storageSeparatedMessages.entrySet()) {
            messageStore.deleteMessages(entry.getKey(), entry.getValue());
        }

        //TODO:message can be in delivery path. If so we need to decrement slot message count
    }

    /**
     * Delete a list of messages from the dead letter channel.
     *
     * @param messagesToRemove list of messages to remove
     * @throws AndesException
     */
    public void deleteDLCMessages(List<AndesMessageMetadata> messagesToRemove) throws AndesException {
        messageStore.deleteDLCMessages(messagesToRemove);
    }

    /**
     * Delete messages from store. Optionally move to dead letter channel.  Delete
     * call is blocking and then slot message count is dropped in order. Message state
     * is updated.
     *
     * @param messagesToRemove List of messages to remove
     * @throws AndesException
     */
    public void deleteMessages(List<DeliverableAndesMetadata> messagesToRemove) throws AndesException {

        Map<String, List<AndesMessageMetadata>> storageSeparatedMessages = new HashMap<>();

        for (DeliverableAndesMetadata message : messagesToRemove) {
            List<AndesMessageMetadata> messagesOfStorageQueue = storageSeparatedMessages
                    .get(message.getStorageQueueName());
            if (null == messagesOfStorageQueue) {
                messagesOfStorageQueue = new ArrayList<>();
            }
            messagesOfStorageQueue.add(message);
            storageSeparatedMessages.put(message.getStorageQueueName(), messagesOfStorageQueue);
        }

        //delete message content along with metadata
        for (Map.Entry<String, List<AndesMessageMetadata>> entry : storageSeparatedMessages.entrySet()) {
            messageStore.deleteMessages(entry.getKey(), entry.getValue());
        }
        for (DeliverableAndesMetadata message : messagesToRemove) {
            //mark messages as deleted
            message.markAsDeletedMessage();
        }

    }

    /**
     * Delete messages from store. Optionally move to dead letter channel.  Delete
     * call is blocking and then slot message count is dropped in order. Message state
     * is updated.
     *
     * @param messagesToRemove List of messages ids to remove
     * @throws AndesException
     */
    public void deleteMessagesById(List<Long> messagesToRemove) throws AndesException {
            messageStore.deleteMessages(messagesToRemove);
    }


    public void moveMessageToDeadLetterChannel(List<DeliverableAndesMetadata> messagesToMove) throws AndesException {
        Map<String, List<AndesMessageMetadata>> storageSeparatedMessages = new HashMap<>();
        for (DeliverableAndesMetadata message : messagesToMove) {
            List<AndesMessageMetadata> messagesOfStorageQueue = storageSeparatedMessages
                    .get(message.getStorageQueueName());
            if (null == messagesOfStorageQueue) {
                messagesOfStorageQueue = new ArrayList<>();
            }
            messagesOfStorageQueue.add(message);
            storageSeparatedMessages.put(message.getStorageQueueName(), messagesOfStorageQueue);
        }

        for (Map.Entry<String, List<AndesMessageMetadata>> entry : storageSeparatedMessages.entrySet()) {
            //move messages to dead letter channel
            String dlcQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(entry.getKey());
            messageExpiryManager.moveMetadataToDLC(entry.getValue(), dlcQueueName);
        }

        //mark the messages as DLC messages
        for (DeliverableAndesMetadata message : messagesToMove) {
            message.markAsDLCMessage();
            message.getSlot().decrementPendingMessageCount();
        }
    }

    public void moveMessageToDeadLetterChannel(Collection<AndesMessageMetadata> messagesToMove) throws AndesException {
        Map<String, List<AndesMessageMetadata>> storageSeparatedMessages = new HashMap<>();

        for (AndesMessageMetadata message : messagesToMove) {
            List<AndesMessageMetadata> messagesOfStorageQueue = storageSeparatedMessages
                    .get(message.getStorageQueueName());
            if (null == messagesOfStorageQueue) {
                messagesOfStorageQueue = new ArrayList<>();
            }
            messagesOfStorageQueue.add(message);
            storageSeparatedMessages.put(message.getStorageQueueName(), messagesOfStorageQueue);
        }
        for (Map.Entry<String, List<AndesMessageMetadata>> entry : storageSeparatedMessages.entrySet()) {
            //move messages to dead letter channel
            String dlcQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(entry.getKey());
            messageExpiryManager.moveMetadataToDLC(entry.getValue(), dlcQueueName);
        }

        //TODO:message can be in delivery path. If so we need to decrement slot message count
    }

    /**
     * Get content chunk from store
     *
     * @param messageId   id of the message
     * @param offsetValue chunk id
     * @return message content
     * @throws AndesException
     */
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        return messageStore.getContent(messageId, offsetValue);
    }

    /**
     * Get a map of queue names and the message count for each queue from the message store.
     *
     * @param queueNames list of queue names of which the message count should be retrieved
     * @return Map of queue names and the message count for each queue
     */
    public Map<String, Integer> getMessageCountForAllQueues(List<String> queueNames) throws AndesException {
        return messageStore.getMessageCountForAllQueues(queueNames);
    }

    /**
     * Get message count for queue
     *
     * @param queueName name of the queue
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCountOfQueue(String queueName) throws AndesException {
        return messageStore.getMessageCountForQueue(queueName);
    }

    /**
     * Get number of messages in the queue within the message id range
     *
     * @param storageQueueName name of the queue
     * @param firstMessageId   starting message id of the range
     * @param lastMessageId    end message id of the range
     * @return number of messages for the queue within the provided message id range
     * @throws AndesException
     */
    public long getMessageCountForQueueInRange(final String storageQueueName, long firstMessageId, long lastMessageId)
            throws AndesException {
        return messageStore.getMessageCountForQueueInRange(storageQueueName, firstMessageId, lastMessageId);
    }

    /**
     * Get message count in DLC for a specific queue.
     *
     * @param queueName    name of the storage queue
     * @param dlcQueueName name of the dlc queue
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCountInDLCForQueue(String queueName, String dlcQueueName) throws AndesException {
        return messageStore.getMessageCountForQueueInDLC(queueName, dlcQueueName);
    }

    /**
     * Get message count in DLC.
     *
     * @param dlcQueueName name of the dlc queue
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCountInDLC(String dlcQueueName) throws AndesException {
        return messageStore.getMessageCountForDLCQueue(dlcQueueName);
    }

    /**
     * Get message metadata from queue between two message id values
     *
     * @param queueName  queue name
     * @param firstMsgId id of the starting id
     * @param lastMsgID  id of the last id
     * @return List of message metadata
     * @throws AndesException
     */
    public List<DeliverableAndesMetadata> getMetaDataList(final Slot slot, final String queueName, long firstMsgId,
            long lastMsgID) throws AndesException {
        return messageStore.getMetadataList(slot, queueName, firstMsgId, lastMsgID);
    }

    /**
     * Get message metadata from queue starting from given id up a given
     * message count
     *
     * @param queueName  name of the queue
     * @param firstMsgId id of the starting id
     * @param count      maximum num of messages to return
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId,
            int count) throws AndesException {
        return messageStore.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
    }

    /**
     * Get message metadata from queue starting from given id up a given message count.
     *
     * @param queueName    name of the queue
     * @param dlcQueueName name of the dead letter channel queue
     * @param firstMsgId   id of the starting id
     * @param count        maximum num of messages to return
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataInDLCForQueue(final String queueName,
            final String dlcQueueName, long firstMsgId, int count) throws AndesException {
        return messageStore.getNextNMessageMetadataForQueueFromDLC(queueName, dlcQueueName, firstMsgId, count);
    }

    /**
     * Get message metadata from queue starting from given id up a given message count
     *
     * @param dlcQueueName name of the dead letter channel queue name
     * @param firstMsgId   id of the starting id
     * @param count        maximum num of messages to return
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataFromDLC(final String dlcQueueName, long firstMsgId,
            int count) throws AndesException {
        return messageStore.getNextNMessageMetadataFromDLC(dlcQueueName, firstMsgId, count);
    }

    /**
     * Get expired but not yet deleted messages from message store
     *
     * @param lowerBoundMessageID lower bound message Id of the safe zone for delete
     * @param  queueName Queue name
     * @return AndesRemovableMetadata
     * @throws AndesException
     */
    public List<Long> getExpiredMessages(long lowerBoundMessageID, String queueName) throws AndesException {
        return messageStore.getExpiredMessages(lowerBoundMessageID, queueName);
    }

    /**
     * Get expired but not yet deleted messages from DLC.
     *
     * @return list of expired message Ids
     * @throws AndesException
     */
    public List<Long> getExpiredMessagesFromDLC(long messageCount) throws AndesException {
        return messageStore.getExpiredMessagesFromDLC(messageCount);
    }

    /**
     * Update the meta data for the given message with the given information in the AndesMetaData. Update destination
     * and meta data bytes.
     *
     * @param currentQueueName The queue the Meta Data currently in
     * @param metadataList     The updated meta data list.
     * @throws AndesException
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {
        messageStore.updateMetadataInformation(currentQueueName, metadataList);
    }

    /**
     * Generate a new message ID. The return id will be always unique
     * even for different message broker nodes
     *
     * @return id generated
     */
    public long generateUniqueId() {
        long messageId = messageIdGenerator.getNextId();
        if (log.isTraceEnabled()) {
            log.trace("MessageID generated: " + messageId);
        }
        return messageId;
    }

    private void configureMessageIDGenerator() {
        // Configure message ID generator
        String idGeneratorImpl = AndesConfigurationManager.readValue(AndesConfiguration.PERSISTENCE_ID_GENERATOR);
        if (idGeneratorImpl != null && !"".equals(idGeneratorImpl)) {
            try {
                Class clz = Class.forName(idGeneratorImpl);

                Object o = clz.newInstance();
                messageIdGenerator = (MessageIdGenerator) o;
            } catch (Exception e) {
                log.error("Error while loading Message id generator implementation : " +
                        idGeneratorImpl +
                        " adding TimeStamp based implementation as the default", e);
                messageIdGenerator = new TimeStampBasedMessageIdGenerator();
            }
        } else {
            messageIdGenerator = new TimeStampBasedMessageIdGenerator();
        }
    }

    /**
     * Start message delivery. Start threads. If not created create.
     */
    public void startMessageDelivery() {
        log.info("Starting SlotDelivery Workers.");
        //Start all slotDeliveryWorkers
        SlotDeliveryWorkerManager.getInstance().startMessageDelivery();
        //Start thrift reconnecting thread if started
        if (MBThriftClient.isReconnectingStarted()) {
            MBThriftClient.setReconnectingFlag(true);
        }
        log.info("Start Disruptor writing messages to store.");
    }

    /**
     * Stop message delivery threads
     */
    public void stopMessageDelivery() {

        log.info("Stopping SlotDelivery Worker.");
        //Stop all slotDeliveryWorkers
        SlotDeliveryWorkerManager.getInstance().stopMessageDelivery();
        //Stop thrift reconnecting thread if started
        if (MBThriftClient.isReconnectingStarted()) {
            MBThriftClient.setReconnectingFlag(false);
        }
        SlotMessageCounter.getInstance().stop();
        //Stop delivery disruptor
        MessageFlusher.getInstance().stopMessageFlusher();
    }

    /**
     * Properly shutdown all messaging related operations / tasks
     *
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {

        stopMessageDelivery();

        completePendingStoreOperations();
    }

    public void completePendingStoreOperations() {
        messageStore.close();
    }

    public SlotCoordinator getSlotCoordinator() {
        return slotCoordinator;
    }

    /**
     * Store retained messages in the message store.
     *
     * @param retainMap Retained message Map
     * @see org.wso2.andes.kernel.AndesMessageMetadata#retain
     */
    public void storeRetainedMessages(Map<String, AndesMessage> retainMap) throws AndesException {
        messageStore.storeRetainedMessages(retainMap);
    }

    /**
     * Return matching retained message metadata for the given subscription topic name. An empty list is returned if no
     * match is found.
     *
     * @param subscriptionTopicName Destination string provided by the subscriber
     * @return AndesMessageMetadata
     * @throws AndesException
     */
    public List<DeliverableAndesMetadata> getRetainedMessageByTopic(String subscriptionTopicName)
            throws AndesException {
        List<DeliverableAndesMetadata> retainMessageList = new ArrayList<>();
        List<String> topicList = messageStore.getAllRetainedTopics();

        for (String topicName : topicList) {
            if (TopicParserUtil.isMatching(topicName, subscriptionTopicName)) {
                retainMessageList.add(messageStore.getRetainedMetadata(topicName));
            }
        }

        return retainMessageList;
    }

    /**
     * Return message content for the given retained message metadata.
     *
     * @param metadata Message metadata
     * @return AndesContent
     * @throws AndesException
     */
    public AndesContent getRetainedMessageContent(AndesMessageMetadata metadata) throws AndesException {
        long messageID = metadata.getMessageID();
        int contentSize = metadata.getMessageContentLength();

        Map<Integer, AndesMessagePart> retainedContentParts = messageStore.getRetainedContentParts(messageID);

        return new RetainedContent(retainedContentParts, contentSize, messageID);
    }

    /**
     * Return last assign message id of slot for given queue
     *
     * @param queueName name of destination queue
     * @return last assign message id
     */
    public long getLastAssignedSlotMessageId(String queueName) throws AndesException {
        long lastMessageId = 0;
        long messageIdDifference = 1024 * 256 * 5000;
        Long lastAssignedSlotMessageId;
        if (ClusterResourceHolder.getInstance().getClusterManager().isClusteringEnabled()) {
            lastAssignedSlotMessageId = SlotManagerClusterMode.getInstance()
                    .getLastAssignedSlotMessageIdInClusterMode(queueName);
        } else {
            lastAssignedSlotMessageId = SlotManagerStandalone.getInstance()
                    .getLastAssignedSlotMessageIdInStandaloneMode(queueName);
        }
        if (lastAssignedSlotMessageId != null) {
            lastMessageId = lastAssignedSlotMessageId - messageIdDifference;
        }
        return lastMessageId;
    }
}
