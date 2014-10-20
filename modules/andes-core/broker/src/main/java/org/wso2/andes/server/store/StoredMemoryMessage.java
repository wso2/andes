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

package org.wso2.andes.server.store;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.slot.Slot;

import java.nio.ByteBuffer;

public class StoredMemoryMessage implements StoredMessage
{
    private final long _messageNumber;
    private final ByteBuffer _content;
    private final StorableMessageMetaData _metaData;

    public StoredMemoryMessage(long messageNumber, StorableMessageMetaData metaData)
    {
        _messageNumber = messageNumber;
        _metaData = metaData;
        _content = ByteBuffer.allocate(metaData.getContentSize());

    }

    public long getMessageNumber()
    {
        return _messageNumber;
    }

    public void addContent(int offsetInMessage, ByteBuffer src)
    {
        src = src.duplicate();
        ByteBuffer dst = _content.duplicate();
        dst.position(offsetInMessage);
        dst.put(src);
    }

    @Override
    public void duplicateMessageContent(long messageId, long messageIdOfClone) throws AndesException {
        //todo: implement this
        throw new UnsupportedOperationException();
    }

    public int getContent(int offset, ByteBuffer dst)
    {
        ByteBuffer src = _content.duplicate();
        src.position(offset);
        src = src.slice();
        if(dst.remaining() < src.limit())
        {
            src.limit(dst.remaining());
        }
        dst.put(src);
        return src.limit();
    }

    public TransactionLog.StoreFuture flushToStore()
    {
        return MessageStore.IMMEDIATE_FUTURE;
    }


    public StorableMessageMetaData getMetaData()
    {
        return _metaData;
    }

    public void remove()
    {
    }

    public void setExchange(String exchange) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Slot getSlot() {
        return null;
    }

    @Override
    public void setSlot(Slot slot) {
        //this method will not be used by instances of this class. This is only to set slot
        // in  AMQP messages
    }
}
