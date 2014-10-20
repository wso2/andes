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

package org.wso2.andes.kernel.storemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.util.List;

/**
 * This is the base class for Message Storing Manager. This class implements common methods
 * to all types of storing managers
 */
public abstract class BasicStoringManager implements MessageStoreManager {

    private static Log log = LogFactory.getLog(DirectStoringManager.class);

    private MessageStore messageStore;

    public BasicStoringManager(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getMessagePart(long messageId, int offsetValue) throws AndesException {
        long start = System.currentTimeMillis();
        AndesMessagePart messagePart =  messageStore.getContent(messageId, offsetValue);
        PerformanceCounter.warnIfTookMoreTime("Read Single Message Chunk", start, 30);
        return messagePart;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        long start = System.currentTimeMillis();
        List<AndesRemovableMetadata> expiredMessages =  messageStore.getExpiredMessages(limit);
        PerformanceCounter.warnIfTookMoreTime("Read Expired Messages", start, 100);
        return expiredMessages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {
        long start = System.currentTimeMillis();
        List<AndesMessageMetadata> metadataList =  messageStore.getMetaDataList(queueName, firstMsgId, lastMsgID);
        PerformanceCounter.warnIfTookMoreTime("Read Metadata With Limits", start, 300);
        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String queueName,
                                                                       long firstMsgId, int count)
            throws AndesException {
        long start = System.currentTimeMillis();
        List<AndesMessageMetadata> metadataList = messageStore.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
        PerformanceCounter.warnIfTookMoreTime("Read Metadata ", start, 300);
        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    public void moveMetaDataToQueue(long messageId, String currentQueueName, String targetQueueName) throws
            AndesException {
        long start = System.currentTimeMillis();
        messageStore.moveMetaDataToQueue(messageId, currentQueueName, targetQueueName);
        PerformanceCounter.warnIfTookMoreTime("Move Metadata ", start, 10);
    }


    /**
     * {@inheritDoc}
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException {
        long start = System.currentTimeMillis();
        messageStore.updateMetaDataInformation(currentQueueName, metadataList);
        PerformanceCounter.warnIfTookMoreTime("Update Metadata ", start, 100);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws InterruptedException {
        messageStore.close();
    }
}
