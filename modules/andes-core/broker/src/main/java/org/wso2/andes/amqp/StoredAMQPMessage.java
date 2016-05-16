/*
 * Copyright (c) 2005-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.amqp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.server.store.StorableMessageMetaData;
import org.wso2.andes.server.store.StoredMessage;
import org.wso2.andes.server.store.TransactionLog;

import java.nio.ByteBuffer;

public class StoredAMQPMessage implements StoredMessage {

    private static Log log = LogFactory.getLog(StoredAMQPMessage.class);

    private final long _messageId;
    private StorableMessageMetaData metaData;
    private String channelID;
    private String exchange;
    private Slot slot;


    /**
     * Create a stored message combining metadata and message ID
     * @param messageId message ID
     * @param metaData  metadata of message
     */
    //todo; we can generalize this as StoredMessage (based on metada info it will refer relevant store)
    public StoredAMQPMessage(long messageId, StorableMessageMetaData metaData) {

        this._messageId = messageId;
        this.metaData = metaData;
    }

    @Override
    public StorableMessageMetaData getMetaData() {
        if (metaData == null) {
            try {
                QpidAndesBridge.getMessageMetaData(_messageId);
            } catch (AMQException e) {
                log.error("Error while getting message metaData for message ID " + _messageId);
            }
        }
        return metaData;
    }

    @Override
    public long getMessageNumber() {
        return _messageId;
    }

    @Override
    /**
     * write content to the message store
     */
    public void addContent(int offsetInMessage, ByteBuffer src) {
        // NOTE: We do nothing in this. Messages are stored when all message metadata and content received
    }

    @Override
    public void duplicateMessageContent(long messageId, long messageIdOfClone) throws AndesException {

    }

    @Override
    /**
     * get content for offset in a message
     */
    public int getContent(int offsetInMessage, ByteBuffer dst) {
        int c = 0;
        try {
            c = QpidAndesBridge.getMessageContentChunk(_messageId, offsetInMessage, dst);
        } catch (AMQException e) {
           log.error("Error while getting message content chunk messageID=" + _messageId + " offset=" + offsetInMessage,e);
        }
        return c;
    }

    @Override
    public TransactionLog.StoreFuture flushToStore() {

        throw new UnsupportedOperationException();
    }

    public String getChannelID() {
        return channelID;
    }

    public void setChannelID(String channelID) {
        this.channelID = channelID;
    }

    @Override
    public void remove() {
        //Todo:when this is called we have to remove content from the storage?? we have to do buffering here. but both queue and topic deletions come here
        //Todo: or is it a remove metadata from this object, no need to keep in-memory?
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getExchange() {
        return exchange;
    }

    public Slot getSlot() {
        return slot;
    }

    public void setSlot(Slot slot){
        this.slot = slot;
    }

}
