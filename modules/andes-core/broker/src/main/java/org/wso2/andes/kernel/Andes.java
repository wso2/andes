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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.kernel.distruptor.inbound.InboundAndesChannelEvent;
import org.wso2.andes.kernel.distruptor.inbound.InboundBindingEvent;
import org.wso2.andes.kernel.distruptor.inbound.InboundDeleteMessagesEvent;
import org.wso2.andes.kernel.distruptor.inbound.InboundExchangeEvent;
import org.wso2.andes.kernel.distruptor.inbound.InboundKernelOpsEvent;
import org.wso2.andes.kernel.distruptor.inbound.InboundQueueEvent;
import org.wso2.andes.kernel.distruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_PURGED_COUNT_TIMEOUT;

/**
 * API for all the tasks done by Andes.
 */
public class Andes {

    private static Log log = LogFactory.getLog(Andes.class);

    private static Andes instance = new Andes();

    /**
     * Max purge timeout to return the value of purge message count 
     */
    private final int PURGE_TIMEOUT_SECONDS;

    /**
     * Use to manage channel according to flow control rules
     */
    private final FlowControlManager flowControlManager;

    /**
     * Event manager handling incoming events.
     * Eg: open channel, publish message, process acknowledgments
     */
    private InboundEventManager inboundEventManager;

    /**
     *  Andes context related information manager. Exchanges, Queues and Bindings
     */
    private AndesContextInformationManager contextInformationManager;
    /**
     * handle all message related functions.
     */
    private MessagingEngine messagingEngine;

    /**
     * Manages all subscription related events 
     */
    private AndesSubscriptionManager subscriptionManager;

    /**
     * Scheduler for periodically trigger Slot Deletion Safe Zone
     * update events
     */
    private final ScheduledExecutorService safeZoneUpdateScheduler = Executors.newScheduledThreadPool(1);

    /**
     * Interval in milliseconds above update call should trigger
     */
    private static final int safeZoneUpdateTriggerInterval = 3000;

    /**
     * Instance of AndesAPI returned
     *
     * @return AndesAPI
     */
    public static Andes getInstance() {
        return instance;
    }

    /**
     * Singleton class. Hence private constructor.
     */
    private Andes() {

        PURGE_TIMEOUT_SECONDS = (Integer) AndesConfigurationManager.readValue(PERFORMANCE_TUNING_PURGED_COUNT_TIMEOUT);
        flowControlManager = new FlowControlManager();
    }

    /**
     * Initialise is package specific. We don't need outsiders initialising the API
     */
    void initialise(SubscriptionStore subscriptionStore,
                    MessagingEngine messagingEngine,
                    AndesContextInformationManager contextInformationManager,
                    AndesSubscriptionManager subscriptionManager) {
        
        this.contextInformationManager = contextInformationManager;
        this.messagingEngine = messagingEngine;
        this.subscriptionManager = subscriptionManager;

        inboundEventManager = InboundEventManagerFactory.createEventManager(subscriptionStore, messagingEngine);

        log.info("Andes API initialised.");
    }

    /**
     * Start the safe zone calculation worker. The safe zone is used to decide if a slot can be safely deleted,
     * assuming all messages in the slot range has been delivered.
     */
    public void startSafeZoneAnalysisWorker() {
        SafeZoneUpdateEventTriggeringTask safeZoneUpdateTask =
                new SafeZoneUpdateEventTriggeringTask(inboundEventManager);

        log.info("Starting Safe Zone Calculator for slots.");
        safeZoneUpdateScheduler.scheduleAtFixedRate(safeZoneUpdateTask,
                5, safeZoneUpdateTriggerInterval, TimeUnit.MILLISECONDS);

    }

    /**
     * When a message is received from a transport it should be converted to an AndesMessage and handed over to Andes
     * for delivery through this method.
     * @param message AndesMessage
     * @param andesChannel AndesChannel
     * @param pubAckHandler PubAckHandler
     */
    public void messageReceived(AndesMessage message, AndesChannel andesChannel, PubAckHandler pubAckHandler) {
        inboundEventManager.messageReceived(message, andesChannel, pubAckHandler);
    }

    /**
     * Acknowledgement received from clients for sent messages should be notified to Andes using this method
     * @param ackData AndesAckData
     * @throws AndesException
     */
    public void ackReceived(AndesAckData ackData) throws AndesException {
        inboundEventManager.ackReceived(ackData);
    }

    /**
     * Connection Client to client is closed.
     *
     * @param channelID id of the closed connection
     */
    public void clientConnectionClosed(UUID channelID) {
        InboundAndesChannelEvent channelEvent = new InboundAndesChannelEvent(channelID);
        channelEvent.prepareForChannelClose();
        inboundEventManager.publishStateEvent(channelEvent);
    }

    /**
     * notify client connection is opened. This is for message tracking purposes on Andes side
     *
     * @param channelID channelID of the client connection
     */
    public void clientConnectionCreated(UUID channelID) {
        InboundAndesChannelEvent channelEvent = new InboundAndesChannelEvent(channelID);
        channelEvent.prepareForChannelOpen();
        inboundEventManager.publishStateEvent(channelEvent);
    }

    /**
     * Close the local subscription with reference to the input subscription event.
     * @param subscriptionEvent disruptor event containing the subscription to close.
     * @throws AndesException
     */
    public void closeLocalSubscription(InboundSubscriptionEvent subscriptionEvent) throws AndesException {
        subscriptionEvent.prepareForCloseSubscription(subscriptionManager);
        inboundEventManager.publishStateEvent(subscriptionEvent);
        try {
            subscriptionEvent.waitForCompletion();
        } catch (SubscriptionAlreadyExistsException e) {
            log.error("Error occurred while closing subscription ", e);
        }
    }

    /**
     * When a local subscription is created notify Andes through this method. This need to be called first to receive
     * any messages from this local subscription
     * @param subscriptionEvent InboundSubscriptionEvent
     * @throws SubscriptionAlreadyExistsException
     */
    public void openLocalSubscription(InboundSubscriptionEvent subscriptionEvent) throws SubscriptionAlreadyExistsException, AndesException {
        subscriptionEvent.prepareForNewSubscription(subscriptionManager);
        inboundEventManager.publishStateEvent(subscriptionEvent);
        subscriptionEvent.waitForCompletion();
    }

    /**
     * Notify client connection is closed from protocol level
     * State related to connection will be updated within Andes
     */
    public void startMessageDelivery() {
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.prepareForStartMessageDelivery(messagingEngine);
        inboundEventManager.publishStateEvent(kernelOpsEvent);
    }

    /**
     * Stop message delivery
     */
    public void stopMessageDelivery() {
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.prepareForStopMessageDelivery(messagingEngine);
        inboundEventManager.publishStateEvent(kernelOpsEvent);
    }

    /**
     * Shut down Andes.
     * NOTE: This is package specific. We don't need access outside from kernel for this task
     */
    public void shutDown() throws AndesException{
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.gracefulShutdown(messagingEngine);
        inboundEventManager.stop();
        kernelOpsEvent.waitForTaskCompletion();
    }

    /**
     * Start message expiration task. This will periodically delete any expired messages
     * NOTE: This is package specific. We don't need access outside from kernel for this task
     */
    void startMessageExpirationWorker() {
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.prepareForStartMessageExpirationWorker(messagingEngine);
        inboundEventManager.publishStateEvent(kernelOpsEvent);
    }

    /**
     * Stop message expiration task
     * NOTE: This is package specific. We don't need outside kernel access for this task
     */
    void stopMessageExpirationWorker() {
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.prepareForStopMessageExpirationWorker(messagingEngine);
        inboundEventManager.publishStateEvent(kernelOpsEvent);
    }

    /**
     * This is the andes-specific purge method and can be called from AMQPBridge,
     * MQTTBridge or UI MBeans (QueueManagementInformationMBean)
     * Remove messages of the queue matching to given destination queue (cassandra / h2 / mysql etc. )
     *
     * @param queueEvent queue event related to purge 
     * @param isTopic weather purging happens for a topic
     * since we cannot guarantee that we caught all messages in delivery threads.)
     * @throws AndesException
     */
    public int purgeQueue(InboundQueueEvent queueEvent, boolean isTopic) throws AndesException {
        queueEvent.purgeQueue(messagingEngine, isTopic);
        inboundEventManager.publishStateEvent(queueEvent);
        try {
            return queueEvent.getPurgedCount(PURGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("Purge event timed out. Purge may have failed or may take longer than " 
                    + PURGE_TIMEOUT_SECONDS + " seconds", e);
        }
        return -1;
    }

    /**
     * Delete messages from store. Optionally move to dead letter channel
     *
     * @param messagesToRemove        List of messages to remove
     * @param moveToDeadLetterChannel if to move to DLC
     * @throws AndesException
     */
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel)
            throws AndesException {
        InboundDeleteMessagesEvent deleteMessagesEvent = new InboundDeleteMessagesEvent(
                messagesToRemove,moveToDeadLetterChannel);
        deleteMessagesEvent.prepareForDelete(messagingEngine);
        inboundEventManager.publishStateEvent(deleteMessagesEvent);
    }

    /**
     * Create queue in Andes kernel
     *
     * @param queueEvent queue event to create
     * @throws AndesException
     */
    public void createQueue(InboundQueueEvent queueEvent) throws AndesException {
        queueEvent.prepareForCreateQueue(contextInformationManager);
        inboundEventManager.publishStateEvent(queueEvent);
    }

    /**
     * Delete the queue from broker. This will purge the queue and
     * delete cluster-wide
     * @param queueEvent  queue event for deleting queue
     * @throws AndesException
     */
    public void deleteQueue(InboundQueueEvent queueEvent) throws AndesException {
        queueEvent.prepareForDeleteQueue(contextInformationManager);
        inboundEventManager.publishStateEvent(queueEvent);
    }

    /**
     * Return the requested chunk of a message's content.
     * @param messageID Unique ID of the Message
     * @param offsetInMessage The offset of the required chunk in the Message content.
     * @return AndesMessagePart
     * @throws AndesException
     */
    public AndesMessagePart getMessageContentChunk(long messageID, int offsetInMessage) throws AndesException {
        return MessagingEngine.getInstance().getMessageContentChunk(messageID, offsetInMessage);
    }

    /**
     * Get a single metadata object
     *
     * @param messageID id of the message
     * @return AndesMessageMetadata
     * @throws AndesException
     */
    public AndesMessageMetadata getMessageMetaData(long messageID) throws AndesException {
        return MessagingEngine.getInstance().getMessageMetaData(messageID);
    }

    /**
     * Message is rejected
     *
     * @param metadata message that is rejected. It must bare id of channel reject came from
     * @throws AndesException
     */
    public void messageRejected(AndesMessageMetadata metadata) throws AndesException {
        MessagingEngine.getInstance().messageRejected(metadata);
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
        MessagingEngine.getInstance().updateMetaDataInformation(currentQueueName, metadataList);
    }

    /**
     * Schedule message for subscription
     *
     * @param messageMetadata message to be scheduled
     * @param subscription    subscription to send
     * @throws AndesException
     */
    public void reQueueMessage(AndesMessageMetadata messageMetadata, LocalSubscription subscription) throws AndesException {
        MessagingEngine.getInstance().reQueueMessage(messageMetadata, subscription);
    }

    /**
     * Move the messages meta data in the given message to the Dead Letter Channel.
     *
     * @param messageId            The message Id to be removed
     * @param destinationQueueName The original destination queue of the message
     * @throws AndesException
     */
    public void moveMessageToDeadLetterChannel(long messageId, String destinationQueueName) throws AndesException {
        MessagingEngine.getInstance().moveMessageToDeadLetterChannel(messageId, destinationQueueName);
    }

    /**
     * Remove in-memory message buffers of the destination matching to given destination in this
     * node. This is called from the HazelcastAgent when it receives a queue purged event.
     *
     * @param destination queue or topic name (subscribed routing key) whose messages should be removed
     * @throws AndesException
     */
    public void clearMessagesFromQueueInMemory(String destination, Long purgedTimestamp) throws AndesException {
        MessagingEngine.getInstance().clearMessagesFromQueueInMemory(destination, purgedTimestamp);
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
        return MessagingEngine.getInstance().getContent(messageId, offsetValue);
    }

    /**
     * Get message count for queue
     *
     * @param queueName name of the queue
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCountOfQueue(String queueName) throws AndesException {
        return MessagingEngine.getInstance().getMessageCountOfQueue(queueName);
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
    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId, long lastMsgID)
            throws AndesException {
        return MessagingEngine.getInstance().getMetaDataList(queueName, firstMsgId, lastMsgID);
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
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId, int count)
            throws AndesException {
        return MessagingEngine.getInstance().getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
    }

    /**
     * Get expired but not yet deleted messages from message store
     * @param limit upper bound for number of messages to be returned
     * @return AndesRemovableMetadata
     * @throws AndesException
     */
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        return MessagingEngine.getInstance().getExpiredMessages(limit);
    }

    /**
     * Generate a new message ID. The return id will be always unique
     * even for different message broker nodes
     *
     * @return id generated
     */
    public long generateNewMessageId() {
        return MessagingEngine.getInstance().generateUniqueId();
    }

    /**
     * Create a new Andes channel for a new local channel.
     *
     * @param listener
     *         Local flow control listener
     * @return AndesChannel
     */
    public AndesChannel createChannel(FlowControlListener listener) {
        return flowControlManager.createChannel(listener);
    }

    /**
     * Remove Andes channel from tracking
     *
     * @param channel
     *         Andes channel
     */
    public void deleteChannel(AndesChannel channel) {
        flowControlManager.deleteChannel(channel);
    }

    /**
     * Create andes binding in Andes kernel
     * @param bindingsEvent InboundBindingEvent binding to be created
     * @throws AndesException
     */
    public void addBinding(InboundBindingEvent bindingsEvent) throws AndesException {
        bindingsEvent.prepareForAddBindingEvent(contextInformationManager);
        inboundEventManager.publishStateEvent(bindingsEvent);
    }

    /**
     * Remove andes binding from andes kernel
     * @param bindingEvent binding to be removed
     * @throws AndesException
     */
    public void removeBinding(InboundBindingEvent bindingEvent) throws AndesException {
        bindingEvent.prepareForRemoveBinding(contextInformationManager);
        inboundEventManager.publishStateEvent(bindingEvent);
    }

    /**
     * Create an exchange in Andes kernel
     *
     * @param exchangeEvent InboundExchangeEvent for AMQP exchange
     * @throws AndesException
     */
    public void createExchange(InboundExchangeEvent exchangeEvent) throws AndesException {
        exchangeEvent.prepareForCreateExchange(contextInformationManager);
        inboundEventManager.publishStateEvent(exchangeEvent);
    }

    /**
     * Delete exchange from andes kernel
     *
     * @param exchangeEvent  exchange to delete
     * @throws AndesException
     */
    public void deleteExchange(InboundExchangeEvent exchangeEvent) throws AndesException{
        exchangeEvent.prepareForDeleteExchange(contextInformationManager);
        inboundEventManager.publishStateEvent(exchangeEvent);
    }

    public boolean checkIfQueueDeletable(InboundQueueEvent queueEvent) throws AndesException {
        queueEvent.prepareForCheckIfQueueDeletable(contextInformationManager);
        inboundEventManager.publishStateEvent(queueEvent);
        
        return queueEvent.IsQueueDeletable();
    }
}

