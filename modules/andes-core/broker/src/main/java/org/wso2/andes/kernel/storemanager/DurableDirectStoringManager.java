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

import java.util.ArrayList;
import java.util.List;

/**
 * This DurableDirectStoringManager stores directly to the durable message store
 */
public class DurableDirectStoringManager implements MessageStoreManager{

    /**
     * Durable message store which is used to persist messages
     */
    private MessageStore durableMessageStore;

    /**
     * Durable message store is used directly to store.
     * @param durableMessageStore MessageStore implementation to be used as the durable message
     * @throws AndesException
     */
    @Override
    public void initialise(MessageStore durableMessageStore) throws AndesException{
        this.durableMessageStore = durableMessageStore;
    }

    /**
     * Metadata stored in durable message store directly
     * @param metadata AndesMessageMetadata
     * @param channelID channel ID
     * @throws AndesException
     */
    @Override
    public void storeMetadata(AndesMessageMetadata metadata, long channelID) throws AndesException{
        durableMessageStore.addMetaData(metadata);
    }

    /**
     * message content stored in durable message store directly
     * @param messagePart AndesMessagePart
     * @throws AndesException
     */
    @Override
    public void storeMessageContent(AndesMessagePart messagePart) throws AndesException{
        // NOTE: Should there be a method in message store to store a single AndesMessagePart?
        List<AndesMessagePart> partList = new ArrayList<AndesMessagePart>(1); // only 1 entry
        partList.add(messagePart);
        durableMessageStore.storeMessagePart(partList);
    }

    /**
     *
     * @param ackData AndesAckData
     * @throws AndesException
     */
    @Override
    public void ackReceived(AndesAckData ackData) throws AndesException{
        // todo: ack handling will be moved out of message store and this list business will be
        // taken out.
        List<AndesAckData> ackDataList = new ArrayList<AndesAckData>(1); // only 1 entry
        ackDataList.add(ackData);
        durableMessageStore.ackReceived(ackDataList);
    }
}
