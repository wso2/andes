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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.ConnectionException;
import org.wso2.andes.kernel.slot.SlotCoordinator;
import org.wso2.andes.kernel.slot.SlotCoordinatorCluster;
import org.wso2.andes.kernel.slot.SlotCoordinatorStandalone;
import org.wso2.andes.kernel.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.kernel.slot.SlotManagerStandalone;
import org.wso2.andes.kernel.slot.SlotMessageCounter;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.MessageIdGenerator;
import org.wso2.andes.server.cluster.coordination.TimeStampBasedMessageIdGenerator;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.andes.thrift.MBThriftClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
     * Updates the message counts in batches if batch size exceeds or the scheduled time elapses
     */
    private MessageCountFlusher messageCountFlusher;

    /**
     * Executor service thread pool to execute content remover task
     */
    private ScheduledExecutorService asyncStoreTasksScheduler;

    /**
     * reference to subscription store
     */
    private SubscriptionStore subscriptionStore;

    /**
     * Reference to MessageStore. This holds the messages received by andes
     */
    private MessageStore messageStore;

    /**
     * This listener is primarily added so that messaging engine and communicate a queue purge situation to the cluster.
     * Addition/Deletion of queues are done through AndesContextInformationManager
     */
    private QueueListener queueListener;

    /**
     * Slot coordinator who is responsible of coordinating with the SlotManager
     */
    private SlotCoordinator slotCoordinator;

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
     * @param subscriptionStore SubscriptionStore
     * @throws AndesException
     */
    public void initialise(MessageStore messageStore,
                           SubscriptionStore subscriptionStore) throws AndesException {

        configureMessageIDGenerator();

        // message count will be flushed to DB in these interval in seconds
        Integer messageCountFlushInterval = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_MESSAGE_COUNTER_TASK_INTERVAL);
        Integer messageCountFlushNumberGap = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_MESSAGE_COUNTER_UPDATE_BATCH_SIZE);
        Integer schedulerPeriod = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_DELETION_CONTENT_REMOVAL_TASK_INTERVAL);

        this.messageStore = messageStore;
        this.subscriptionStore = subscriptionStore;

        //register listeners for queue changes
        queueListener = new ClusterCoordinationHandler(HazelcastAgent.getInstance());

        // Only two scheduled tasks running (content removal task and message count update task).
        // And each scheduled tasks run with fixed delay. Hence at a given time
        // maximum needed threads would be 2.
        int threadPoolCount = 2;
        ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("MessagingEngine-AsyncStoreTasksSchedulerPool")
                                          .build();
        asyncStoreTasksScheduler = Executors.newScheduledThreadPool(threadPoolCount , namedThreadFactory);

        // This task will periodically flush message count value to the store
        messageCountFlusher = new MessageCountFlusher(messageStore, messageCountFlushNumberGap);

        asyncStoreTasksScheduler.scheduleWithFixedDelay(messageCountFlusher,
                messageCountFlushInterval,
                messageCountFlushInterval,
                TimeUnit.SECONDS);

        /*
        Initialize the SlotCoordinator
         */
        if(AndesContext.getInstance().isClusteringEnabled()){
          slotCoordinator = new SlotCoordinatorCluster();
        }else {
          slotCoordinator = new SlotCoordinatorStandalone();
        }
    }

    /**
     * Return the requested chunk of a message's content.
     * @param messageID Unique ID of the Message
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
    public Map<Long, List<AndesMessagePart>> getContent(List<Long> messageIdList) throws AndesException {
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

        //Separate the messages that are received as new messaged and messages that are being restored
        //Messages in the restoringMessages list will only be updated but not rewritten
        List<AndesMessage> restoringMessages = new ArrayList<>();
        List<AndesMessage> newMessages = new ArrayList<>();

        for (AndesMessage message : messageList) {
            //If a previous message id is not set, then it is a message to be written
            //else, it is a message to be updated
            if (0L == message.getPreviousMessageID()) {
                newMessages.add(message);
            } else {
                restoringMessages.add(message);
            }
        }

        //Write and update all messages recceived
        if (!newMessages.isEmpty()) {
            messageStore.storeMessages(newMessages);
        }
        if (!restoringMessages.isEmpty()) {
            messageStore.updateMessage(restoringMessages);
        }
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
     * Message is rejected
     *
     * @param metadata message that is rejected. It must bare id of channel reject came from
     * @throws AndesException
     */
    public void messageRejected(AndesMessageMetadata metadata) throws AndesException {

        OnflightMessageTracker.getInstance().handleFailure(metadata);
        LocalSubscription subToResend = subscriptionStore.getLocalSubscriptionForChannelId(metadata.getChannelId());
        if (subToResend != null) {
            reQueueMessage(metadata, subToResend);
        } else {
            log.warn("Cannot handle reject. Subscription not found for channel " + metadata.getChannelId()
                    + "Dropping message id= " + metadata.getMessageID());
        }
    }

    /**
     * Schedule message for subscription
     *
     * @param messageMetadata message to be scheduled
     * @param subscription    subscription to send
     * @throws AndesException
     */
    public void reQueueMessage(AndesMessageMetadata messageMetadata, LocalSubscription subscription)
            throws AndesException {
        MessageFlusher.getInstance().scheduleMessageForSubscription(subscription, messageMetadata);
    }

    /**
     * Add message metadata to dead letter channel (DLC)
     *
     * @param messageMetadata      The message metadata {@link org.wso2.andes.kernel.AndesMessageMetadata} to be
     *                             added to DLC
     * @param destinationQueueName The original destination queue of the message
     * @throws AndesException
     */
    public void moveMessageToDeadLetterChannel(AndesMessageMetadata messageMetadata, String destinationQueueName)
            throws AndesException {
        String deadLetterQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(destinationQueueName);
        // TODO We can either create an AndesRemovableMetadata object here, or pass the values
        messageStore.moveMetaDataToDLC(messageMetadata.getMessageID(), messageMetadata.getStorageQueueName(),
                messageMetadata.getDestination());
        // Increment count by 1 in DLC and decrement by 1 in original queue
        incrementQueueCount(deadLetterQueueName, 1);
        decrementQueueCount(destinationQueueName, 1);

        //remove tracking of the message
        OnflightMessageTracker.getInstance()
                .stampMessageAsDLCAndRemoveFromTacking(messageMetadata.messageID);
    }

    /**
     * Method to move a list of metadata to dead letter channel
     *
     * @param metadata the list of metadata
     */
    public void moveMessagesToDeadLetterChannel(List<AndesRemovableMetadata> metadata) throws AndesException {
        for (AndesRemovableMetadata messageMetadata : metadata) {
            String dlcQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(messageMetadata
                    .getMessageDestination());

            messageStore.moveMetaDataToDLC(messageMetadata.getMessageID(), messageMetadata.getStorageDestination(),
                    messageMetadata.getMessageDestination());
            // Increment queue count of DLC
            // Cannot increment whole count at once since there are separate DLC queues for each tenant
            incrementQueueCount(dlcQueueName, 1);
            decrementQueueCount(messageMetadata.getMessageDestination(), 1);
        }
    }

    /**
     * Remove in-memory message buffers of the destination matching to given destination in this
     * node. This is called from the Hazelcast Agent when it receives a queue purged event.
     *
     * @param destination queue or topic name (subscribed routing key) whose messages should be removed
     * @return number of messages removed
     * @throws AndesException
     */
    public int clearMessagesFromQueueInMemory(String destination,
                                              Long purgedTimestamp) throws AndesException {

        MessageFlusher messageFlusher = MessageFlusher.getInstance();
        messageFlusher.getMessageDeliveryInfo(destination).setLastPurgedTimestamp(purgedTimestamp);
        return messageFlusher.getMessageDeliveryInfo(destination).clearReadButUndeliveredMessages();
    }

    /**
     * This is the andes-specific purge method and can be called from AMQPBridge,
     * MQTTBridge or UI MBeans (QueueManagementInformationMBean)
     * Remove messages of the queue matching to given destination queue (cassandra / h2 / mysql etc. )
     *
     * @param destination queue or topic name (subscribed routing key) whose messages should be removed
     * @param ownerName The user who initiated the purge request
     * @param isTopic weather purging happens for a topic
     * @param startMessageID starting message id for the purge operation, optional parameter
     * @return number of messages removed (in memory message count may not be 100% accurate
     * since we cannot guarantee that we caught all messages in delivery threads.)
     * @throws AndesException
     */
    public int purgeMessages(String destination, String ownerName, boolean isTopic,Long startMessageID) throws AndesException {

        // The timestamp is recorded to track messages that came before the purge event.
        // Refer OnflightMessageTracker:evaluateDeliveryRules method for more details.
        Long purgedTimestamp = System.currentTimeMillis();
        String nodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        String storageQueueName = AndesUtils.getStorageQueueForDestination(destination, nodeID, isTopic);

        try {
            // Clear all slots assigned to the Queue. This should ideally stop any messages being buffered during the
            // purge.
            // This call clears all slot associations for the queue in all nodes. (could take time)
            //Slot relations should be cleared through the storage queue name
            slotCoordinator.clearAllActiveSlotRelationsToQueue(storageQueueName);
        } catch (ConnectionException e) {
            String message = "Error while establishing a connection with the thrift server to delete active slots " +
                    "assigned for the purged queue : " + destination;
            log.error(message);
            throw new AndesException(message, e);
        }

        // Clear in memory messages of self (node)
        clearMessagesFromQueueInMemory(destination, purgedTimestamp);

        //Notify the cluster if queues
        if(!isTopic) {
            AndesQueue purgedQueue = new AndesQueue(destination, ownerName, false, true);
            purgedQueue.setLastPurgedTimestamp(purgedTimestamp);

            queueListener.handleLocalQueuesChanged(purgedQueue, QueueListener.QueueEvent.PURGED);
        }

        // Clear any and all message references addressed to the queue from the persistent store.
        // We can measure the message count in store, but cannot exactly infer the message count
        // in memory within all nodes at the time of purging. (Adding that could unnecessarily
        // block critical pub sub flows.)
        // queues destination = storage queue. But for topics it is different
        int purgedNumOfMessages =  purgeQueueFromStore(storageQueueName,startMessageID,isTopic);
        log.info("Purged messages of destination " + destination);
        return purgedNumOfMessages;
    }

    /**
     * Clear all references to all message metadata / content addressed to a specific queue. Used when purging.
     *
     * @param storageQueueName name of storage queue
     * @param startMessageID   id of the message the query should start from
     * @param isTopic          whether the storage queue is a topic
     * @throws AndesException
     */
    public int purgeQueueFromStore(String storageQueueName, Long startMessageID, boolean isTopic) throws
            AndesException {

        try {
            // Retrieve message IDs addressed to the queue and keep track of message count for
            // the queue in the store
            // Messages should be retrieved from both storage and DLC, therefore pass value 2
            List<Long> messageIDsAddressedToQueue = messageStore.getMessageIDsAddressedToQueue(storageQueueName,
                    startMessageID, 2);

            // Clear all message expiry data.
            // Data should be removed from expiry data tables before deleting data from meta data table since expiry
            // data table holds a foreign key constraint to meta data table.
            messageStore.deleteMessagesFromExpiryQueue(messageIDsAddressedToQueue);

            // delete messages for the queue
            messageStore.deleteMessages(storageQueueName, messageIDsAddressedToQueue, true);
            // Reset message count for the specific queue
            messageStore.resetMessageCounterForQueue(storageQueueName);

            return messageIDsAddressedToQueue.size();

        } catch (AndesException e) {
            // This will be a store-specific error. We could make all 5 operations into one atomic transaction so
            // that in case of an error data won't be obsolete, but we must do it in a proper generic manner (to
            // allow any collection of store methods to be executed in a single transaction.). To be done as a
            // separate task.
            throw new AndesException("Error occurred when purging queue from store : " + storageQueueName, e);
        }
    }

    /**
     * A utility class to hold a list of message ids and mesage counts.
     * With use of this class implementation of {@link #deleteMessages} was made simple.
     */
    private class AndesRemovableMetadataDTO{
        
        public AndesRemovableMetadataDTO(){
            messagesToRemove = new ArrayList<Long>();
        }
        List<Long> messagesToRemove;
        int msgCount;
    }

    /**
     * Delete messages from store. Optionally move to dead letter channel
     *
     * @param messagesToRemove        List of messages to remove
     * @param moveToDeadLetterChannel if to move to DLC
     * @throws AndesException
     */
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove,
                               boolean moveToDeadLetterChannel) throws AndesException {
        if (moveToDeadLetterChannel) {
            moveMessagesToDeadLetterChannel(messagesToRemove);
        } else {

            List<Long> idsOfMessagesToRemove = new ArrayList<>(messagesToRemove.size());
            Map<String, AndesRemovableMetadataDTO> storageSeperatedAndesRemovableMetadataDTOs =
                    new HashMap<>(messagesToRemove.size());
            ArrayList<AndesRemovableMetadata> messagesInDLC = new ArrayList<>();

            for (AndesRemovableMetadata message : messagesToRemove) {
                // Add the messages in dead letter channel to a separate list
                if (DLCQueueUtils.isDeadLetterQueue(message.getMessageDestination())) {
                    messagesInDLC.add(message);
                } else {
                    idsOfMessagesToRemove.add(message.getMessageID());
                    //update <storageQueue, dtos> map
                    AndesRemovableMetadataDTO dto = storageSeperatedAndesRemovableMetadataDTOs.get(message
                            .getStorageDestination());

                    if (dto == null) {
                        dto = new AndesRemovableMetadataDTO();
                    }
                    if (message.getStorageDestination() != null) {
                        storageSeperatedAndesRemovableMetadataDTOs.put(message.getStorageDestination(), dto);
                    }
                    dto.messagesToRemove.add(message.getMessageID());
                    dto.msgCount = dto.msgCount + 1;
                }
            }
            if (!messagesInDLC.isEmpty()) {
                messageStore.deleteMessageMetadataFromDLC(messagesInDLC);
            }

            //delete message content along with metadata
            for (Map.Entry<String, AndesRemovableMetadataDTO> entry : storageSeperatedAndesRemovableMetadataDTOs
                    .entrySet()) {
                messageStore.deleteMessages(entry.getKey(), entry.getValue().messagesToRemove, false);
                decrementQueueCount(entry.getKey(), entry.getValue().msgCount);
            }

        }

    }

    /**
     * Decrement queue count. Flush to store in batches. Count update will take time to reflect
     * @param queueName name of the queue to decrement count
     * @param decrementBy decrement count by this value, This should be a positive value
     */
    public void decrementQueueCount(String queueName, int decrementBy) {
        messageCountFlusher.decrementQueueCount(queueName, decrementBy);
    }

    /**
     * Increment message count of queue. Flush to store in batches. Count update will take time to reflect
     * @param queueName name of the queue to increment count
     * @param incrementBy increment count by this value
     */
    public void incrementQueueCount(String queueName, int incrementBy) {
        messageCountFlusher.incrementQueueCount(queueName, incrementBy);
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
     * Get message count of a queue. If a DLC queue, retrive from the dead letter channels
     * If storage queue, retrieve from storage
     *
     * @param queueName name of the queue
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCountOfQueue(String queueName) throws AndesException {
        if (!DLCQueueUtils.isDeadLetterQueue(queueName)){
            return messageStore.getMessageCountForQueue(queueName, true);
        }
        else{
            return messageStore.getMessageCountForQueue(queueName, true);
        }
    }

    /**
     * Get message count for queue a from dead letter channel
     *
     * @param queueName name of the queue
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCountInDLCForQueue(String queueName) throws AndesException {
        return messageStore.getMessageCountForQueue(queueName, false);
    }

    /**
     * Get message metadata from queue between two message id values
     * Retrieve the message list from storage
     *
     * @param queueName  queue name
     * @param firstMsgId id of the starting id
     * @param lastMsgID  id of the last id
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId, long lastMsgID) throws AndesException {
        return messageStore.getMetadataList(queueName, firstMsgId, lastMsgID, false);
    }

    /**
     * Get message metadata from queue between two message id values
     *
     * @param queueName  queue name
     * @param firstMsgId id of the starting id
     * @param lastMsgID  id of the last id
     * @param fromDLC    whether to retirve messages from storage or DLC
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId, long lastMsgID,
                                                      boolean fromDLC) throws AndesException {
        return messageStore.getMetadataList(queueName, firstMsgId, lastMsgID, fromDLC);
    }

    /**
     * Get message metadata from queue in the storage starting from given id up a given message count
     *
     * @param queueName  name of the queue
     * @param firstMsgId id of the starting id
     * @param count      maximum num of messages to return
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId, int
            count) throws AndesException {
        return messageStore.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count, true);
    }

    /**
     * Get message metadata for queue in DLC starting from given id up a given message count
     *
     * @param queueName  name of the queue
     * @param firstMsgId id of the starting id
     * @param count      maximum num of messages to return
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataFromDLCForQueue(final String queueName, long firstMsgId,
                                                                             int count) throws AndesException {
        return messageStore.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count, false);
    }

    /**
     * Get expired but not yet deleted messages from message store
     * @param limit upper bound for number of messages to be returned
     * @return AndesRemovableMetadata
     * @throws AndesException
     */
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        return messageStore.getExpiredMessages(limit);
    }

    /**
     * Update the meta data for the given message with the given information in the AndesMetaData. Update destination
     * and meta data bytes.
     *
     * @param currentQueueName The queue the Meta Data currently in
     * @param metadataList     The updated meta data list.
     * @throws AndesException
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException {
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
                log.error(
                        "Error while loading Message id generator implementation : " +
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
        SlotDeliveryWorkerManager.getInstance().startAllSlotDeliveryWorkers();
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
        SlotDeliveryWorkerManager.getInstance().stopSlotDeliveryWorkers();
        //Stop thrift reconnecting thread if started
        if (MBThriftClient.isReconnectingStarted()) {
            MBThriftClient.setReconnectingFlag(false);
        }
        SlotMessageCounter.getInstance().stop();
        //Stop delivery disruptor
        MessageFlusher.getInstance().getFlusherExecutor().stop();
    }

    /**
     * Properly shutdown all messaging related operations / tasks
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {

        stopMessageDelivery();
        stopMessageExpirationWorker();

        completePendingStoreOperations();
    }

    public void completePendingStoreOperations() throws InterruptedException {
        try {
            asyncStoreTasksScheduler.shutdown();
            asyncStoreTasksScheduler.awaitTermination(5, TimeUnit.SECONDS);
            messageStore.close();
        } catch (InterruptedException e) {
            asyncStoreTasksScheduler.shutdownNow();
            messageStore.close();
            log.warn("Content remover task forcefully shutdown.");
            throw e;
        }
    }

    /**
     * Start Checking for Expired Messages (JMS Expiration)
     */
    public void startMessageExpirationWorker() {

        MessageExpirationWorker messageExpirationWorker = ClusterResourceHolder.getInstance().getMessageExpirationWorker();

        if (messageExpirationWorker == null) {
            ClusterResourceHolder.getInstance().setMessageExpirationWorker(new MessageExpirationWorker());

        } else {
            if (!messageExpirationWorker.isWorking()) {
                messageExpirationWorker.startWorking();
            }
        }
    }

    /**
     * Stop Checking for Expired Messages (JMS Expiration)
     */
    public void stopMessageExpirationWorker() {

        MessageExpirationWorker messageExpirationWorker = ClusterResourceHolder.getInstance()
                .getMessageExpirationWorker();

        if (messageExpirationWorker != null && messageExpirationWorker.isWorking()) {
            messageExpirationWorker.stopWorking();
        }
    }


    public SlotCoordinator getSlotCoordinator() {
        return slotCoordinator;
    }

    /**
     * Store retained messages in the message store.
     *
     * @see org.wso2.andes.kernel.AndesMessageMetadata#retain
     * @param retainMap Retained message Map
     */
    public void storeRetainedMessages(Map<String,AndesMessage> retainMap) throws AndesException {
        messageStore.storeRetainedMessages(retainMap);
    }


    /**
     * Return matching retained message metadata for the given subscription topic name. An empty list is returned if no
     * match is found.
     *
     * @param subscriptionTopicName
     *         Destination string provided by the subscriber
     * @return AndesMessageMetadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getRetainedMessageByTopic(String subscriptionTopicName) throws AndesException {
        List<AndesMessageMetadata> retainMessageList = new ArrayList<AndesMessageMetadata>();
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
     * @param metadata
     *         Message metadata
     * @return AndesContent
     * @throws AndesException
     */
    public AndesContent getRetainedMessageContent(AndesMessageMetadata metadata) throws AndesException {
        long messageID = metadata.getMessageID();
        int contentSize = AMQPUtils.convertAndesMetadataToAMQMetadata(metadata).getContentSize();

        Map<Integer, AndesMessagePart> retainedContentParts = messageStore.getRetainedContentParts(messageID);

        return new RetainedContent(retainedContentParts, contentSize, messageID);
    }

    /**
     * Return last assign message id of slot for given queue
     *
     * @param queueName name of destination queue
     * @return last assign message id
     */
    public long getLastAssignedSlotMessageId(String queueName) throws AndesException{
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
        if(lastAssignedSlotMessageId != null) {
            lastMessageId = lastAssignedSlotMessageId - messageIdDifference;
        }
        return lastMessageId;
    }
}
