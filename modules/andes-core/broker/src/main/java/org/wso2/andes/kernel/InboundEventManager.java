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

import org.wso2.andes.kernel.distruptor.inbound.AndesInboundStateEvent;
import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;

import java.util.List;

/**
 * Implementations of this interface defines how MB should manage inbound Events
 * Eg: Through disruptor or direct method calls
 */
public interface InboundEventManager {

    /**
     * When a message is received from a transport it is handed over to MessagingEngine through the implementation of
     * inbound event manager. (e.g: through a disruptor ring buffer) Eventually the message will be stored
     * @param message AndesMessage
     * @param andesChannel AndesChannel
     * @param pubAckHandler PubAckHandler
     */
    public void messageReceived(AndesMessage message, AndesChannel andesChannel, PubAckHandler pubAckHandler);

    /**
     * Acknowledgement received from clients for sent messages will be handled through this method
     * @param ackData AndesAckData
     * @throws AndesException
     */
    public void ackReceived(AndesAckData ackData) throws AndesException;

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
     * Publish state change event to event Manager
     * @param stateEvent AndesInboundStateEvent
     */
    public void publishStateEvent(AndesInboundStateEvent stateEvent);

    /**
     * Publish an event to update safe zone message ID
     * as per this node (this is used when deleting slots)
     */
    public void updateSlotDeletionSafeZone();

    /**
     * Stop disruptor. This wait until disruptor process pending events in ring buffer.
     */
    public void stop();
}
