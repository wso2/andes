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
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.MessageStoreManager;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor;

/**
 * This message store manager stores messages in durable storage through disruptor (async storing)
 */
public class DurableAsyncStoringManager implements MessageStoreManager {

    /**
     * Disruptor which implements a ring buffer to store messages asynchronously to store
     */
    private DisruptorBasedExecutor disruptorBasedExecutor;

    /**
     * Initialise Disruptor with the durable message store as persistent storage
     * @param durableMessageStore MessageStore implementation to be used as the durable message
     * @throws AndesException
     */
    @Override
    public void initialise(MessageStore durableMessageStore) throws AndesException{
        disruptorBasedExecutor = new DisruptorBasedExecutor(durableMessageStore, null);
    }

    /**
     * store metadata to persistent storage asynchronously through disruptor
     * @param metadata AndesMessageMetadata
     * @param channelID channel ID
     * @throws AndesException
     */
    @Override
    public void storeMetadata(AndesMessageMetadata metadata, long channelID) throws AndesException{
        disruptorBasedExecutor.messageCompleted(metadata, channelID);
    }

    /**
     * store message content to persistent storage asynchronously through disruptor
     * @param messagePart AndesMessagePart
     * @throws AndesException
     */
    @Override
    public void storeMessageContent(AndesMessagePart messagePart) throws AndesException{
        disruptorBasedExecutor.messagePartReceived(messagePart);
    }

    /**
     * Acknowledgement is parsed through to persistent storage through Disruptor
     * @param ackData AndesAckData
     * @throws AndesException
     */
    @Override
    public void ackReceived(AndesAckData ackData) throws AndesException{
        disruptorBasedExecutor.ackReceived(ackData);
    }


}
