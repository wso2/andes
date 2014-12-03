/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.storemanager.MessageStoreManagerFactory;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.MessageExpirationWorker;
import org.wso2.andes.server.cassandra.MessageFlusher;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.MessageIdGenerator;
import org.wso2.andes.server.cluster.coordination.TimeStampBasedMessageIdGenerator;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.slot.SlotManager;
import org.wso2.andes.server.slot.thrift.MBThriftClient;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.store.MessageContentRemoverTask;
import org.wso2.andes.subscription.SubscriptionStore;

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
     * Executor service thread pool to execute content remover task
     */
    private ScheduledExecutorService asyncStoreTasksScheduler;

    /**
     * reference to subscription store
     */
    private SubscriptionStore subscriptionStore;

    /**
     * Map to keep message count difference not flushed to disk of each queue
     */
    private Map<String, AtomicInteger> messageCountDifferenceMap;

    /**
     * message count will be flushed to DB when count difference reach this val
     */
    private int messageCountFlushNumberGap;

    /**
     * Manages how the message content is persisted. Eg in async mode or stored in memory etc
     * Underlying implementation of this interface handles the logic
     */
    private MessageStoreManager messageStoreManager;

    private MessageStore messageStore;

    /**
     * This listener is primarily added so that messaging engine and communicate a queue purge situation to the cluster.
     * Addition/Deletion of queues are done through AndesContextInformationManager
     */
    private QueueListener queueListener;

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
    public void initialise(MessageStore messageStore) throws AndesException {
        configureMessageIDGenerator();

        messageStoreManager = MessageStoreManagerFactory.createDirectMessageStoreManager(messageStore);
        this.messageStore = messageStore;
        subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        messageCountDifferenceMap = new HashMap<String, AtomicInteger>();
        //register listeners for queue changes
        queueListener = new ClusterCoordinationHandler(HazelcastAgent.getInstance());

        int threadPoolCount = 2;
        asyncStoreTasksScheduler = Executors.newScheduledThreadPool(threadPoolCount);

        //this task will periodically remove message contents from store
        messageContentRemoverTask = new MessageContentRemoverTask(messageStore);
        Integer schedulerPeriod = AndesConfigurationManager.getInstance().readConfigurationValue
                (AndesConfiguration.PERFORMANCE_TUNING_DELETION_CONTENT_REMOVAL_TASK_INTERVAL);
        asyncStoreTasksScheduler.scheduleAtFixedRate(messageContentRemoverTask,
                schedulerPeriod,
                schedulerPeriod,
                TimeUnit.SECONDS);


        // message count will be flushed to DB in these interval in seconds
        int messageCountFlushInterval = 15;
        messageCountFlushNumberGap = 100;

        //this task will periodically flush message count value to the store
        Thread messageCountFlusher = new Thread(new Runnable() {
            @Override
            public void run() {
                for (Map.Entry<String, AtomicInteger> entry : messageCountDifferenceMap.entrySet()) {
                    try {
                        if (entry.getValue().get() > 0) {
                            AndesContext.getInstance().getAndesContextStore()
                                    .incrementMessageCountForQueue(entry.getKey(),
                                            entry.getValue().get());
                        } else if (entry.getValue().get() < 0) {
                            AndesContext.getInstance().getAndesContextStore()
                                    .incrementMessageCountForQueue(
                                            entry.getKey(),
                                            entry.getValue().get());
                        }
                        entry.getValue().set(0);
                    } catch (AndesException e) {
                        log.error("Error while updating message counts for queue " + entry.getKey());
                    }
                }
            }
        });

        asyncStoreTasksScheduler.scheduleAtFixedRate(messageCountFlusher,
                10,
                messageCountFlushInterval,
                TimeUnit.SECONDS);

    }

    /**
     * Return the requested chunk of a message's content.
     * @param messageID Unique ID of the Message
     * @param offsetInMessage The offset of the required chunk in the Message content.
     * @return AndesMessagePart
     * @throws AndesException
     */
    public AndesMessagePart getMessageContentChunk(long messageID, int offsetInMessage) throws AndesException {
        return messageStoreManager.getMessagePart(messageID,offsetInMessage);
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
        return messageStoreManager.getMetadataOfMessage(messageID);
    }

    /**
     * Ack received.
     *
     * @param ackData information on Ack
     * @throws AndesException
     */
    public void ackReceived(AndesAckData ackData) throws AndesException {
        messageStoreManager.ackReceived(ackData);
    }

    /**
     * Message is rejected
     *
     * @param metadata message that is rejected. It must bare id of channel reject came from
     * @throws AndesException
     */
    public void messageRejected(AndesMessageMetadata metadata) throws AndesException {

        OnflightMessageTracker.getInstance().handleFailure(metadata);
        LocalSubscription subToResend = subscriptionStore.getLocalSubscriptionForChannelId(metadata.getChannelId(), metadata.getDestination(), metadata.isTopic());
        if (subToResend != null) {
            reQueueMessage(metadata, subToResend);
        } else {
            log.warn("Cannot handle reject. Subscription not found for channel " + metadata.getChannelId() + "Dropping message id= " + metadata.getMessageID());
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
        String deadLetterQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString
                (destinationQueueName, AndesConstants.DEAD_LETTER_QUEUE_NAME);


        messageStoreManager.moveMetaDataToQueue(messageId, destinationQueueName, deadLetterQueueName);

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
     * @return number of messages removed (in memory message count may not be 100% accurate
     * since we cannot guarantee that we caught all messages in delivery threads.)
     * @throws AndesException
     */
    public int purgeMessages(String destination, String ownerName, boolean isTopic) throws AndesException {

        // The timestamp is recorded to track messages that came before the purge event.
        // Refer OnflightMessageTracker:evaluateDeliveryRules method for more details.
        Long purgedTimestamp = System.currentTimeMillis();

        // Clear all slots assigned to the Queue. This should ideally stop any messages being buffered during the purge.
        // This call clears all slot associations for the queue in all nodes. (could take time)
        SlotManager.getInstance().clearAllActiveSlotRelationsToQueue(destination);

        // Clear in memory messages of self (node)
        clearMessagesFromQueueInMemory(destination, purgedTimestamp);

        //Notify the cluster if queues
        if(!isTopic) {
            // The queueOwner here is passed as null since its not used from the notification object.
            // The user permissions are validated at the andes feature component level.
            AndesQueue purgedQueue = new AndesQueue(destination, ownerName, false, true);
            purgedQueue.setLastPurgedTimestamp(purgedTimestamp);

            queueListener.handleLocalQueuesChanged(purgedQueue, QueueListener.QueueEvent.PURGED);
        }

        // Clear any and all message references addressed to the queue from the persistent store.
        // We can measure the message count in store, but cannot exactly infer the message count
        // in memory within all nodes at the time of purging. (Adding that could unnecessarily
        // block critical pub sub flows.)
        // queues destination = storage queue. But for topics it is different
        String nodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        String storageQueueName = AndesUtils.getStorageQueueForDestination(destination, nodeID, isTopic);
        int purgedNumOfMessages =  messageStoreManager.purgeQueueFromStore(storageQueueName);
        log.info("Purged messages of destination " + destination);
        return purgedNumOfMessages;
    }

    /**
     * Delete messages from store. Optionally move to dead letter channel
     *
     * @param messagesToRemove        List of messages to remove
     * @param moveToDeadLetterChannel if to move to DLC
     * @throws AndesException
     */
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel) throws AndesException {
        List<Long> idsOfMessagesToRemove = new ArrayList<Long>();
        Map<String, List<AndesRemovableMetadata>> storageQueueSeparatedRemoveMessages = new HashMap<String, List<AndesRemovableMetadata>>();
        Map<String, Integer> destinationSeparatedMsgCounts = new HashMap<String, Integer>();

        for (AndesRemovableMetadata message : messagesToRemove) {
            idsOfMessagesToRemove.add(message.getMessageID());

            //update <storageQueue, metadata> map
            List<AndesRemovableMetadata> messages = storageQueueSeparatedRemoveMessages
                    .get(message.getStorageDestination());
            if (messages == null) {
                messages = new ArrayList
                        <AndesRemovableMetadata>();
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
                messageStore
                        .addMetaDataToQueue(AndesConstants.DEAD_LETTER_QUEUE_NAME, metadata);
            }
        }

        //remove metadata
        for (String storageQueueName : storageQueueSeparatedRemoveMessages.keySet()) {
            messageStore.deleteMessageMetadataFromQueue(storageQueueName,
                    storageQueueSeparatedRemoveMessages
                            .get(storageQueueName));
        }
        //decrement message counts
        for(String destination: destinationSeparatedMsgCounts.keySet()) {
            decrementQueueCount(destination, destinationSeparatedMsgCounts.get(destination));
        }

        if (!moveToDeadLetterChannel) {
            //remove content
            //TODO: - hasitha if a topic message be careful as it is shared
            deleteMessageParts(idsOfMessagesToRemove);
        }

        if(moveToDeadLetterChannel) {
            //increment message count of DLC
            incrementQueueCount(AndesConstants.DEAD_LETTER_QUEUE_NAME, messagesToRemove.size());
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
     * decrement queue count by 1. Flush if difference is in tab
     * @param queueName
     *         name of the queue to decrement count
     * @param decrementBy
     *         decrement count by this value
     * @throws AndesException
     */
    private void decrementQueueCount(String queueName, int decrementBy) throws AndesException {
        AtomicInteger msgCount = messageCountDifferenceMap.get(queueName);
        if (msgCount == null) {
            msgCount = new AtomicInteger(0);
            messageCountDifferenceMap.put(queueName, msgCount);
        }

        synchronized (this) {
            int currentVal = msgCount.get();
            int newVal = currentVal - decrementBy;
            msgCount.set(newVal);
        }

        //we flush this value to store in 100 message tabs
        if (msgCount.get() % messageCountFlushNumberGap == 0) {
            if (msgCount.get() > 0) {
                AndesContext.getInstance().getAndesContextStore()
                        .incrementMessageCountForQueue(queueName, msgCount.get());
            } else {
                AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(
                        queueName, msgCount.get());
            }
            msgCount.set(0);
        }

    }

    /**
     * increment message count of queue. Flush if difference is in tab
     * @param queueName name of the queue to increment count
     * @param incrementBy increment count by this value
     * @throws AndesException
     */
    private void incrementQueueCount(String queueName, int incrementBy) throws AndesException {
        AtomicInteger msgCount = messageCountDifferenceMap.get(queueName);
        if (msgCount == null) {
            msgCount = new AtomicInteger(0);
            messageCountDifferenceMap.put(queueName, msgCount);
        }

        synchronized (this) {
            int currentVal = msgCount.get();
            int newVal = currentVal + incrementBy;
            msgCount.set(newVal);
        }

        //we flush this value to store in 100 message tabs
        if (msgCount.get() % messageCountFlushNumberGap == 0) {
            if (msgCount.get() > 0) {
                AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(
                        queueName, msgCount.get());
            } else {
                AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(
                        queueName, msgCount.get());
            }
            msgCount.set(0);
        }
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
        return messageStoreManager.getMessagePart(messageId, offsetValue);
    }

    /**
     * Get message count for queue
     *
     * @param queueName name of the queue
     * @return message count of the queue
     * @throws AndesException
     */
    public int getMessageCountOfQueue(String queueName) throws AndesException {
        return (int) AndesContext.getInstance().getAndesContextStore().getMessageCountForQueue(
                queueName);
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
        return messageStoreManager.getMetaDataList(queueName, firstMsgId, lastMsgID);
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
        return messageStoreManager.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
    }

    /**
     * Get expired but not yet deleted messages from message store
     * @param limit upper bound for number of messages to be returned
     * @return AndesRemovableMetadata
     * @throws AndesException
     */
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        return messageStoreManager.getExpiredMessages(limit);
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
        messageStoreManager.updateMetaDataInformation(currentQueueName, metadataList);
    }

    /**
     * Generate a new message ID. The return id will be always unique
     * even for different message broker nodes
     *
     * @return id generated
     */
    public long generateNewMessageId() {
        long messageId = messageIdGenerator.getNextId();
        if (log.isTraceEnabled()) {
            log.trace("MessageID generated: " + messageId);
        }
        return messageId;
    }

    private void configureMessageIDGenerator() throws AndesException {
        // Configure message ID generator
        String idGeneratorImpl = AndesConfigurationManager.getInstance().readConfigurationValue
                (AndesConfiguration.PERSISTENCE_ID_GENERATOR);
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
        log.info("Starting SlotDelivery Workers...");
        //Start all slotDeliveryWorkers
        SlotDeliveryWorkerManager.getInstance().startAllSlotDeliveryWorkers();
        //Start thrift reconnecting thread if started
        if (MBThriftClient.isReconnectingStarted()) {
            MBThriftClient.setReconnectingFlag(true);
        }
        log.info("Start Disruptor writing messages to store...");
    }

    /**
     * Stop message delivery threads
     */
    public void stopMessageDelivery() {

        log.info("Stopping SlotDelivery Worker...");
        //Stop all slotDeliveryWorkers
        SlotDeliveryWorkerManager.getInstance().stopSlotDeliveryWorkers();
        //Stop thrift reconnecting thread if started
        if (MBThriftClient.isReconnectingStarted()) {
            MBThriftClient.setReconnectingFlag(false);
        }
        log.info("Stopping Disruptor writing messages to store...");
    }

    public void close() throws InterruptedException {

        stopMessageDelivery();
        stopMessageExpirationWorker();
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
    public void startMessageExpirationWorker() throws AndesException {

        MessageExpirationWorker mew = ClusterResourceHolder.getInstance().getMessageExpirationWorker();

        if (mew == null) {
            MessageExpirationWorker messageExpirationWorker = new MessageExpirationWorker();
            ClusterResourceHolder.getInstance().setMessageExpirationWorker(messageExpirationWorker);

        } else {
            if (!mew.isWorking()) {
                mew.startWorking();
            }
        }
    }

    /**
     * Stop Checking for Expired Messages (JMS Expiration)
     */
    public void stopMessageExpirationWorker() {

        MessageExpirationWorker mew = ClusterResourceHolder.getInstance()
                .getMessageExpirationWorker();

        if (mew != null && mew.isWorking()) {
            mew.stopWorking();
        }

    }
}
