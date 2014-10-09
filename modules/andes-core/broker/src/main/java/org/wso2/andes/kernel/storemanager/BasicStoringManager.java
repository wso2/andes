/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *     WSO2 Inc. licenses this file to you under the Apache License,
 *     Version 2.0 (the "License"); you may not use this file except
 *     in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing,
 *    software distributed under the License is distributed on an
 *    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *    KIND, either express or implied.  See the License for the
 *    specific language governing permissions and limitations
 *    under the License.
 */

package org.wso2.andes.kernel.storemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.MessageStoreManager;
import java.util.List;

/**
 * This is the base class for Message Storing Manager. This class implements common methods
 * to all types of storing managers
 */
public abstract class BasicStoringManager implements MessageStoreManager{

    private static Log log = LogFactory.getLog(DirectStoringManager.class);

    private MessageStore messageStore;

    public BasicStoringManager (MessageStore messageStore){
        this.messageStore = messageStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getMessagePart(long messageId, int offsetValue) throws AndesException {
        return messageStore.getContent(messageId, offsetValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        return messageStore.getExpiredMessages(limit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {
        return messageStore.getMetaDataList(queueName, firstMsgId, lastMsgID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String queueName,
                                                                       long firstMsgId, int count)
            throws AndesException {
        return messageStore.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
    }

    /**
     * {@inheritDoc}
     */
    public void moveMetaDataToQueue(long messageId, String currentQueueName, String targetQueueName) throws
                                                                                                     AndesException {
        messageStore.moveMetaDataToQueue(messageId, currentQueueName, targetQueueName);
    }


    /**
     * {@inheritDoc}
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
                                                                                                            AndesException {
        messageStore.updateMetaDataInformation(currentQueueName, metadataList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        messageStore.close();
    }
}
