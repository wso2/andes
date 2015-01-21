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

import java.util.List;
import java.util.UUID;

/**
 * Implementations of this interface defines how MB should manage inbound Events
 * Eg: Through disruptor or direct method calls
 */
public interface InboundEventManager {

    /**
     * When a message is received from a transport it is handed over to MessagingEngine through the implementation of
     * inbound event manager. (e.g: through a disruptor ring buffer) Eventually the message will be stored
     * @param message AndesMessage
     * @param andesChannel
     */
    public void messageReceived(AndesMessage message, AndesChannel andesChannel);

    /**
     * Acknowledgement received from clients for sent messages will be handled through this method
     * @param ackData AndesAckData
     * @throws AndesException
     */
    public void ackReceived(AndesAckData ackData) throws AndesException;

    /**
     * Given message is rejected
     *
     * @param metadata message that is rejected. It must bare id of channel reject came from
     * @throws AndesException
     */
    public void messageRejected(AndesMessageMetadata metadata) throws AndesException;

    /**
     * Schedule message for subscription
     *
     * @param messageMetadata message to be scheduled
     * @param subscription    subscription to send
     * @throws AndesException
     */
    public void reQueueMessage(AndesMessageMetadata messageMetadata, LocalSubscription subscription) throws
            AndesException;

    /**
     * Move the messages meta data in the given message to the Dead Letter Channel.
     *
     * @param messageId            The message Id to be removed
     * @param destinationQueueName The original destination queue of the message
     * @throws AndesException
     */
    public void moveMessageToDeadLetterChannel(long messageId, String destinationQueueName) throws AndesException;

    /**
     * Remove in-memory message buffers of the destination matching to given destination in this
     * node. This is called from the HazelcastAgent when it receives a queue purged event.
     *
     * @param storageQueueName queue or topic name (subscribed routing key) which messages should be removed
     * @throws AndesException
     */
    public void clearMessagesFromQueueInMemory(String storageQueueName, Long purgedTimestamp) throws AndesException;

    /**
     * Remove messages of the queue matching to given destination queue (cassandra / h2 / mysql etc. )
     *
     * @param destination queue or topic name (subscribed routing key) whose messages should be removed
     * @param ownerName The user who initiated the purge request
     * @param isTopic weather purging happens for a topic
     * since we cannot guarantee that we caught all messages in delivery threads.)
     * @throws AndesException
     */
    public void purgeQueue(String destination, String ownerName, boolean isTopic) throws AndesException;

    /**
     * Delete messages from store. Optionally move to dead letter channel
     *
     * @param messagesToRemove        List of messages to be removed
     * @param moveToDeadLetterChannel True if messages to be moved to Dead letter channel
     * @throws AndesException
     */
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel)
            throws AndesException;

    /**
     * Update meta data for the given message with given information in the AndesMetaData. Update destination
     * and meta data bytes.
     *
     * @param currentQueueName The queue the Meta Data currently in
     * @param metadataList     The updated meta data list.
     * @throws AndesException
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList)
            throws AndesException;

    /**
     * Set inbound event to start delivery workers for message delivery.
     * @throws Exception
     */
    public void startMessageDelivery() throws Exception;

    /**
     * Set inbound event to stop message delivery
     */
    public void stopMessageDelivery();

    /**
     *  Set inbound event to shut down Andes.
     */
    public void shutDown();

    /**
     * Start message expiration task. This will periodically delete any expired messages
     */
    public void startMessageExpirationWorker();

    /**
     * Stop message expiration task
     */
    public void stopMessageExpirationWorker();

    /**
     * Set inbound event to notify client connection is closed
     * State related to connection will be updated within Andes
     *
     * @param channelID id of the closed connection
     */
    public void clientConnectionClosed(UUID channelID);

    /**
     * Notify client connection is opened. This is for message tracking purposes on Andes side
     * State within Andes will be updated Accordingly.
     *
     * @param channelID channelID of the client connection
     */
    public void clientConnectionCreated(UUID channelID);

    /**
     * Notify Andes to close an existing local subscription.
     * @param localSubscription LocalSubscription
     */
    public void closeLocalSubscription(LocalSubscription localSubscription);

    /**
     * When a local subscription is created notify Andes. This need to be called first to receive
     * any messages from this local subscription.
     * @param localSubscription LocalSubscription
     */
    public void openLocalSubscription(LocalSubscription localSubscription) throws SubscriptionAlreadyExistingException;

}
