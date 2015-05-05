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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.ConnectionException;
import org.wso2.andes.kernel.slot.SlotCoordinator;
import org.wso2.andes.kernel.slot.SlotCoordinatorCluster;
import org.wso2.andes.kernel.slot.SlotCoordinatorStandalone;
import org.wso2.andes.kernel.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.kernel.slot.SlotMessageCounter;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.MessageIdGenerator;
import org.wso2.andes.server.cluster.coordination.TimeStampBasedMessageIdGenerator;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.andes.thrift.MBThriftClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
     * This task will asynchronously remove message content
     */
    private MessageContentRemoverTask messageContentRemoverTask;

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
        asyncStoreTasksScheduler = Executors.newScheduledThreadPool(threadPoolCount);

        //this task will periodically remove message contents from store
        messageContentRemoverTask = new MessageContentRemoverTask(messageStore);

        asyncStoreTasksScheduler.scheduleWithFixedDelay(messageContentRemoverTask,
                schedulerPeriod, schedulerPeriod, TimeUnit.SECONDS);

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

    public void messagesReceived(List<AndesMessage> messageList) throws AndesException{
        List<AndesMessageMetadata> metadataList = new ArrayList<AndesMessageMetadata>(messageList.size());
        // parts can be more than message list size
        List<AndesMessagePart> messageParts = new ArrayList<AndesMessagePart>(messageList.size());

        for (AndesMessage message: messageList) {
            metadataList.add(message.getMetadata());
            messageParts.addAll(message.getContentChunkList());
        }

        messageStore.storeMessagePart(messageParts);
        messageStore.addMetaData(metadataList);
    }

    /**
     * Get a single metadata object
     *
     * @param messageID id of the message
     * @return AndesMessageMetadata
     * @throws AndesException
     */
    public AndesMessageMetadata getMessageMetaData(long messageID) throws AndesException {
        return messageStore.getMetaData(messageID);
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
     * Move the messages meta data in the given message to the Dead Letter Channel and
     * remove those meta data from the original queue.
     *
     * @param messageId            The message Id to be removed
     * @param destinationQueueName The original destination queue of the message
     * @throws AndesException
     */
    public void moveMessageToDeadLetterChannel(long messageId, String destinationQueueName)
            throws AndesException {
        String deadLetterQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(destinationQueueName);

        long start = System.currentTimeMillis();
        messageStore.moveMetaDataToQueue(messageId, destinationQueueName, deadLetterQueueName);
        PerformanceCounter.warnIfTookMoreTime("Move Metadata ", start, 10);

        // Increment count by 1 in DLC and decrement by 1 in original queue
        incrementQueueCount(deadLetterQueueName, 1);
        decrementQueueCount(destinationQueueName, 1);

        //remove tracking of the message
        OnflightMessageTracker.getInstance()
                .stampMessageAsDLCAndRemoveFromTacking(messageId);
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

    /***
     * Clear all references to all message metadata / content addressed to a specific queue. Used when purging.
     * @param storageQueueName name of storage queue
     * @param startMessageID id of the message the query should start from
     * @throws AndesException
     */
    public int purgeQueueFromStore(String storageQueueName, Long startMessageID, boolean isTopic) throws AndesException{

        try {
            // Retrieve message IDs addressed to the queue and keep track of message count for
            // the queue in the store
            List<Long> messageIDsAddressedToQueue = messageStore.getMessageIDsAddressedToQueue(storageQueueName,
                    startMessageID);

            Integer messageCountInStore = messageIDsAddressedToQueue.size();

            // Clear all message expiry data.
            // Data should be removed from expiry data tables before deleting data from meta data table since expiry
            // data table holds a foreign key constraint to meta data table.
            messageStore.deleteMessagesFromExpiryQueue(messageIDsAddressedToQueue);

            //  Clear message metadata from queues
            messageStore.deleteAllMessageMetadata(storageQueueName);

            // Reset message count for the specific queue
            messageStore.resetMessageCounterForQueue(storageQueueName);

            // There is only 1 DLC queue per tenant. So we have to read and parse the message
            // metadata and filter messages specific to a given queue.
            // This is pretty exhaustive. If there are 1000 DLC messages and only 10 are relevant
            // to the given queue, We still have to get all 1000
            // into memory. Options are to delete dlc messages leisurely with another thread,
            // or to break from original DLC pattern and maintain multiple DLC queues per each queue.

            //For durable topics and queues the purge would not return true for a topic, non durable topics would not
            //send the message to the DLC therefor we don't need to purge message for non-durable case
            Integer messageCountFromDLC = 0;
            if(!isTopic) {
                //We would consider removal of messages from the DLC for queues and durable topics
                messageCountFromDLC = messageStore.deleteAllMessageMetadataFromDLC(DLCQueueUtils
                        .identifyTenantInformationAndGenerateDLCString(storageQueueName), storageQueueName);

            }

            // Clear message content leisurely / asynchronously using retrieved message IDs
            messageStore.deleteMessageParts(messageIDsAddressedToQueue);

            // If any other places in the store keep track of messages in future,
            // they should also be cleared here.

            return messageCountInStore + messageCountFromDLC;

        } catch (AndesException e) {
            // This will be a store-specific error. We could make all 5 operations into one atomic transaction so
            // that in case of an error data won't be obsolete, but we must do it in a proper generic manner (to
            // allow any collection of store methods to be executed in a single transaction.). To be done as a
            // separate task.
            throw new AndesException("Error occurred when purging queue from store : " + storageQueueName, e);
        }
    }

    /**
     * Delete messages from store. Optionally move to dead letter channel
     *
     * @param messagesToRemove        List of messages to remove
     * @param moveToDeadLetterChannel if to move to DLC
     * @throws AndesException
     */
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel) throws AndesException {
        List<Long> idsOfMessagesToRemove = new ArrayList<Long>(messagesToRemove.size());
        Map<String, List<AndesRemovableMetadata>> storageQueueSeparatedRemoveMessages
                = new HashMap<String, List<AndesRemovableMetadata>>(messagesToRemove.size());
        Map<String, Integer> destinationSeparatedMsgCounts = new HashMap<String, Integer>(messagesToRemove.size());

        for (AndesRemovableMetadata message : messagesToRemove) {
            idsOfMessagesToRemove.add(message.getMessageID());

            //update <storageQueue, metadata> map
            List<AndesRemovableMetadata> messages = storageQueueSeparatedRemoveMessages
                    .get(message.getStorageDestination());
            if (messages == null) {
                messages = new ArrayList<AndesRemovableMetadata>();
            }
            messages.add(message);
            storageQueueSeparatedRemoveMessages.put(message.getStorageDestination(), messages);

            //update <destination, Msgcount> map
            Integer count = destinationSeparatedMsgCounts.get(message.getMessageDestination());
            if(count == null) {
                count = 0;
            }
            count = count + 1;
            destinationSeparatedMsgCounts.put(message.getMessageDestination(), count);

            //if to move, move to DLC. This is costy. Involves per message read and writes
            if (moveToDeadLetterChannel) {
                AndesMessageMetadata metadata = messageStore.getMetaData(message.getMessageID());
                String dlcQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(message
                                .getMessageDestination());
                messageStore.addMetaDataToQueue(dlcQueueName, metadata);

                // Increment queue count of DLC
                // Cannot increment whole count at once since there are separate DLC queues for each tenant
                incrementQueueCount(dlcQueueName, 1);
            }
        }

        //remove metadata
        for (Map.Entry<String, List<AndesRemovableMetadata>> entry : storageQueueSeparatedRemoveMessages.entrySet()) {
            messageStore.deleteMessageMetadataFromQueue(entry.getKey(), entry.getValue());
        }
        //decrement message counts
        for(Map.Entry<String, Integer> entry: destinationSeparatedMsgCounts.entrySet()) {
            decrementQueueCount(entry.getKey(), entry.getValue());
        }

        if (!moveToDeadLetterChannel) {
            //remove content
            //TODO: - hasitha if a topic message be careful as it is shared
            deleteMessageParts(idsOfMessagesToRemove);
        }

    }

    /**
     * schedule to remove message content chunks of messages
     *
     * @param messageIdList list of message ids of content to be removed
     * @throws AndesException
     */
    private void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        for (Long messageId : messageIdList) {
            messageContentRemoverTask.put(messageId);
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
     * Get message metadata from queue between two message id values
     *
     * @param queueName  queue name
     * @param firstMsgId id of the starting id
     * @param lastMsgID  id of the last id
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId, long lastMsgID) throws AndesException {
        return messageStore.getMetaDataList(queueName, firstMsgId, lastMsgID);
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
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId, int count) throws AndesException {
        return messageStore.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
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
        messageStore.updateMetaDataInformation(currentQueueName, metadataList);
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
        //Stop delivery disruptor
        MessageFlusher.getInstance().getFlusherExecutor().stop();
        //Stop thrift reconnecting thread if started
        if (MBThriftClient.isReconnectingStarted()) {
            MBThriftClient.setReconnectingFlag(false);
        }
        SlotMessageCounter.getInstance().stop();
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
}
