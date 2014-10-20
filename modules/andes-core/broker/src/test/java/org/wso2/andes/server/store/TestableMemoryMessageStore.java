/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.server.store;

import org.wso2.andes.AMQStoreException;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.slot.Slot;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Adds some extra methods to the memory message store for testing purposes.
 */
public class TestableMemoryMessageStore extends MemoryMessageStore
{

    MemoryMessageStore _mms = null;
    private HashMap<Long, AMQQueue> _messages = new HashMap<Long, AMQQueue>();
    private AtomicInteger _messageCount = new AtomicInteger(0);

    public TestableMemoryMessageStore(MemoryMessageStore mms)
    {
        _mms = mms;
    }

    public TestableMemoryMessageStore()
    {

    }

    @Override
    public void close() throws Exception
    {
        // Not required to do anything
    }

    @Override
    public StoredMessage addMessage(StorableMessageMetaData metaData)
    {
        return new TestableStoredMessage(super.addMessage(metaData));
    }

    public int getMessageCount()
    {
        return _messageCount.get();
    }

    private class TestableTransaction implements Transaction
    {
        public void enqueueMessage(TransactionLogResource queue, Long messageId) throws AMQStoreException
        {
            getMessages().put(messageId, (AMQQueue)queue);
        }

        public void dequeueMessage(TransactionLogResource queue, Long messageId) throws AMQStoreException
        {
            getMessages().remove(messageId);
        }

        public void commitTran() throws AMQStoreException
        {
        }

        public StoreFuture commitTranAsync() throws AMQStoreException
        {
            return new StoreFuture()
                    {
                        public boolean isComplete()
                        {
                            return true;
                        }

                        public void waitForCompletion()
                        {

                        }
                    };
        }

        public void abortTran() throws AMQStoreException
        {
        }
    }


    @Override
    public Transaction newTransaction()
    {
        return new TestableTransaction();
    }

    public HashMap<Long, AMQQueue> getMessages()
    {
        return _messages;
    }

    private class TestableStoredMessage implements StoredMessage
    {
        private final StoredMessage _storedMessage;

        public TestableStoredMessage(StoredMessage storedMessage)
        {
            _messageCount.incrementAndGet();
            _storedMessage = storedMessage;
        }

        public StorableMessageMetaData getMetaData()
        {
            return _storedMessage.getMetaData();
        }

        public long getMessageNumber()
        {
            return _storedMessage.getMessageNumber();
        }

        public void addContent(int offsetInMessage, ByteBuffer src)
        {
            _storedMessage.addContent(offsetInMessage, src);
        }

        @Override
        public void duplicateMessageContent(long messageId, long messageIdOfClone) throws AndesException {
            _storedMessage.duplicateMessageContent(messageId,messageIdOfClone);
        }

        public int getContent(int offsetInMessage, ByteBuffer dst)
        {
            return _storedMessage.getContent(offsetInMessage, dst);
        }

        public StoreFuture flushToStore()
        {
            return _storedMessage.flushToStore();
        }

        public void remove()
        {
            _storedMessage.remove();
            _messageCount.decrementAndGet();
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
}
