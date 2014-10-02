/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.andes.kernel.storemanager;

import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.MessageStoreManager;
import org.wso2.andes.store.jdbc.JDBCMessageStoreImpl;
import org.wso2.andes.store.jdbc.JDBCConnection;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor;

import java.util.List;

/**
 * This message store manager stores persistent messages through durable asynchronous calls through
 * disruptor and non persistent messages directly in in-memory message store
 */
public class DurableAsyncAndMemStoringManager implements MessageStoreManager {

    /**
     * Disruptor which implements a ring buffer to store messages asynchronously to store
     */
    private DisruptorBasedExecutor disruptorBasedExecutor;

    /**
     * In memory message store (messages are not persisted)
     */
    private MessageStore inMemoryMessageStore;

    /**
     * Initialise disruptor and a in-memory message store. Disruptor is used to store durable
     * messages in durableMessageStore.
     * @param durableMessageStore MessageStore implementation to be used as the durable message
     *                            store.
     * @throws AndesException
     */
    @Override
    public void initialise(MessageStore durableMessageStore) throws AndesException {
        // initialise disruptor with durableMessageStore
        disruptorBasedExecutor = new DisruptorBasedExecutor(this, null);
        // initialise in-memory message store
        inMemoryMessageStore = new JDBCMessageStoreImpl();
        inMemoryMessageStore.initializeMessageStore(JDBCConnection
                                                            .getInMemoryConnectionProperties());
    }

    /**
     * durable messages are stored in durableMessageStore through disruptor. Rest is stored
     * directly in in-memory message store
     * @param metadata AndesMessageMetadata
     * @param channelID channel ID
     * @throws AndesException
     */
    @Override
    public void storeMetadata(AndesMessageMetadata metadata, long channelID) throws AndesException {
        if(metadata.isPersistent()) {
            disruptorBasedExecutor.messageCompleted(metadata, channelID);
        } else {
            inMemoryMessageStore.addMetaData(metadata);
        }
    }

    @Override
    public void storeMessageContent(AndesMessagePart messagePart) throws AndesException {
        // todo is this message part persistent?
    }

    @Override
    public void ackReceived(AndesAckData ackData) throws AndesException {
        // todo where to ack? persistence store?
    }

    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {

    }

    @Override
    public void processAckReceived(List<AndesAckData> ackList) throws AndesException {

    }

    @Override
    public void decrementQueueCount(String queueName) throws AndesException {

    }

    @Override
    public void incrementQueueCount(String queueName) throws AndesException {

    }

    @Override
    public void storeMessagePart(List<AndesMessagePart> messageParts) throws AndesException {

    }

    @Override
    public void storeMetaData(List<AndesMessageMetadata> messageMetadata) throws AndesException {

    }

    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        return null;
    }

    @Override
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove,
                               boolean moveToDeadLetterChannel) throws AndesException {

    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        return null;
    }

    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {
        return null;
    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String queueName,
                                                                       long firstMsgId, int count)
            throws AndesException {
        return null;
    }

    @Override
    public void close() {

    }
}
