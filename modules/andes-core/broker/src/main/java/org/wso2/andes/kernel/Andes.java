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
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.List;
import java.util.UUID;

/**
 * API for all the tasks done by Andes.
 */
public class Andes {

    private static Log log = LogFactory.getLog(Andes.class);

    private static Andes instance = new Andes();

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
        flowControlManager = new FlowControlManager();
    }

    /**
     * Initialise is package specific. We don't need outsiders initialising the API
     */
    void initialise(SubscriptionStore subscriptionStore, MessagingEngine messagingEngine) {
        inboundEventManager = InboundEventManagerFactory.createEventManager(subscriptionStore, messagingEngine);
        log.info("Andes API initialised.");
    }

    /**
     * When a message is received from a transport it should be converted to an AndesMessage and handed over to Andes
     * for delivery through this method.
     * @param message AndesMessage
     * @param andesChannel
     */
    public void messageReceived(AndesMessage message, AndesChannel andesChannel) {
        inboundEventManager.messageReceived(message, andesChannel);
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
    public void clientConnectionClosed(long channelID) {
        inboundEventManager.clientConnectionClosed(channelID);
    }

    /**
     * notify client connection is opened. This is for message tracking purposes on Andes side
     *
     * @param channelID channelID of the client connection
     */
    public void clientConnectionCreated(long channelID) {
        inboundEventManager.clientConnectionCreated(channelID);
    }

    public void closeLocalSubscription(LocalSubscription localSubscription) throws AndesException {
        inboundEventManager.closeLocalSubscription(localSubscription);
    }

    /**
     * When a local subscription is created notify Andes through this method. This need to be called first to receive
     * any messages from this local subscription
     * @param localSubscription LocalSubscription
     * @throws AndesException
     */
    public void openLocalSubscription(LocalSubscription localSubscription) throws AndesException {
        inboundEventManager.openLocalSubscription(localSubscription);
    }

    /**
     * Notify client connection is closed from protocol level
     * State related to connection will be updated within Andes
     * @throws Exception
     */
    public void startMessageDelivery() throws Exception {
        inboundEventManager.startMessageDelivery();
    }

    /**
     * Stop message delivery
     */
    public void stopMessageDelivery() {
        inboundEventManager.stopMessageDelivery();
    }

    /**
     * Shut down Andes.
     * NOTE: This is package specific. We don't need access outside from kernel for this task
     */
    void shutDown() {
        inboundEventManager.shutDown();
    }

    /**
     * Start message expiration task. This will periodically delete any expired messages
     * NOTE: This is package specific. We don't need access outside from kernel for this task
     */
    void startMessageExpirationWorker() {
        inboundEventManager.startMessageExpirationWorker();
    }

    /**
     * Stop message expiration task
     * NOTE: This is package specific. We don't need outside kernel access for this task
     */
    void stopMessageExpirationWorker() {
        inboundEventManager.stopMessageExpirationWorker();
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
        return MessagingEngine.getInstance().purgeMessages(destination, ownerName, isTopic);
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
        MessagingEngine.getInstance().deleteMessages(messagesToRemove, moveToDeadLetterChannel);
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
    public void messageRejected(DeliverableAndesMessageMetadata metadata, AndesChannel rejectedChannel) throws AndesException {
        MessagingEngine.getInstance().messageRejected(metadata, rejectedChannel);
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
    public void reQueueMessage(DeliverableAndesMessageMetadata messageMetadata, LocalSubscription subscription) throws AndesException {
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
        return MessagingEngine.getInstance().generateNewMessageId();
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
    public void removeChannel(AndesChannel channel) {
        flowControlManager.removeChannel(channel);
    }
}

