/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import java.nio.ByteBuffer;
import java.util.List;

public interface MessageStore {

    public void storeMessagePart(List<AndesMessagePart> part)throws AndesException;

    public void deleteMessageParts(List<Long> messageIdList)throws AndesException;

    public int getContent(String messageId, int offsetValue, ByteBuffer dst);

    public void deleteMessageMetadataFromQueue(QueueAddress queueAddress,List<AndesRemovableMetadata> messagesToRemove) throws AndesException;

    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(QueueAddress queueAddress, long startMsgID, int count)throws AndesException;

    public void ackReceived(List<AndesAckData> ackList)throws AndesException;

    public void addMessageMetaData(QueueAddress queueAddress, List<AndesMessageMetadata> messageList)throws AndesException;

    public void moveMessageMetaData(QueueAddress sourceAddress, QueueAddress targetAddress, List<AndesMessageMetadata> messageList) throws AndesException;

    public long moveAllMessageMetaDataOfQueue(QueueAddress sourceAddress, QueueAddress targetAddress, String destinationQueue) throws AndesException;

    public int countMessagesOfQueue(QueueAddress queueAddress, String destinationQueueNameToMatch) throws AndesException;

    public long getMessageCountForQueue(String destinationQueueName) throws AndesException;

    public AndesMessageMetadata getMetaData(long messageId);

    public void close();

    public void initializeMessageStore (DurableStoreConnection cassandraconnection) throws AndesException;
}
