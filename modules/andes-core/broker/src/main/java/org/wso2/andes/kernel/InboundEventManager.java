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

    public void messageReceived(AndesMessage message);

    public void ackReceived(AndesAckData ackData) throws AndesException;

    public void messageRejected(AndesMessageMetadata metadata) throws AndesException;

    public void reQueueMessage(AndesMessageMetadata messageMetadata, LocalSubscription subscription) throws
            AndesException;

    public void moveMessageToDeadLetterChannel(long messageId, String destinationQueueName) throws AndesException;

    public void clearMessagesFromQueueInMemory(String storageQueueName, Long purgedTimestamp) throws AndesException;

    public void purgeQueue(String destinationQueue, String ownerName) throws AndesException;

    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel)
            throws AndesException;

    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList)
            throws AndesException;

    public void startMessageDelivery() throws Exception;

    public void stopMessageDelivery();

    public void shutDown();

    public void startMessageExpirationWorker();

    public void stopMessageExpirationWorker();

    public void clientConnectionClosed(UUID channelID);

    public void clientConnectionCreated(UUID channelID);

    public void closeLocalSubscription(LocalSubscription localSubscription);

    public void openLocalSubscription(LocalSubscription localSubscription);

}
