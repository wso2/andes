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
package org.wso2.andes.kernel;

/**
 * This interface works as an abstraction for the actual implementation of how a message is
 * persisted to the database.
 * Message may be stored through disruptor or without it. And in some cases messages that need
 * not be persisted will be stored in memory. Implementations of this interface should handle
 * that logic.
 */
public interface MessageStoreManager {

    /**
     * Initialisation of MessageStoreManager
     * @param durableMessageStore MessageStore implementation to be used as the durable message
     *                            store.
     * @throws AndesException
     */
    public void initialise(MessageStore durableMessageStore) throws AndesException;

    /**
     * Store Metadata of the message
     * @param metadata AndesMessageMetadata
     * @param channelID channel ID
     * @throws AndesException
     */
    public void storeMetadata(AndesMessageMetadata metadata, long channelID) throws AndesException;

    /**
     * Store Message Content as parts (chunks)
     * @param messagePart AndesMessagePart
     * @throws AndesException
     */
    public void storeMessageContent(AndesMessagePart messagePart) throws AndesException;

    /**
     * Handle ack received event
     * @param ackData AndesAckData
     * @throws AndesException
     */
    public void ackReceived(AndesAckData ackData) throws AndesException;
}
