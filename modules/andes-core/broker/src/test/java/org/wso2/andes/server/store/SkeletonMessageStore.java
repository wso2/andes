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

import org.apache.commons.configuration.Configuration;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQStoreException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.framing.abstraction.ContentChunk;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.message.ServerMessage;
import org.wso2.andes.server.logging.LogSubject;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.ByteBuffer;

/**
 * A message store that does nothing. Designed to be used in tests that do not want to use any message store
 * functionality.
 */
public class SkeletonMessageStore implements MessageStore
{
    private final AtomicLong _messageId = new AtomicLong(1);

    public void configure(String base, Configuration config) throws Exception
    {
    }

    public void configureConfigStore(String name,
                          ConfigurationRecoveryHandler recoveryHandler,
                          Configuration config,
                          LogSubject logSubject) throws Exception
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void configureMessageStore(String name,
                                      MessageStoreRecoveryHandler recoveryHandler,
                                      Configuration config,
                                      LogSubject logSubject) throws Exception
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void close() throws Exception
    {
    }

    public <M extends StorableMessageMetaData> StoredMessage<M> addMessage(M metaData)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeMessage(Long messageId)
    {
    }

    public void createExchange(Exchange exchange) throws AMQStoreException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeExchange(Exchange exchange) throws AMQStoreException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void bindQueue(Exchange exchange, AMQShortString routingKey, AMQQueue queue, FieldTable args) throws AMQStoreException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void unbindQueue(Exchange exchange, AMQShortString routingKey, AMQQueue queue, FieldTable args) throws AMQStoreException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void createQueue(AMQQueue queue) throws AMQStoreException
    {
    }

    public void createQueue(AMQQueue queue, FieldTable arguments) throws AMQStoreException
    {
    }




    public List<AMQQueue> createQueues() throws AMQException
    {
        return null;
    }

    public Long getNewMessageId()
    {
        return _messageId.getAndIncrement();
    }

    public void storeContentBodyChunk(
            Long messageId,
            int index,
            ContentChunk contentBody,
            boolean lastContentBody) throws AMQException
    {

    }

    public void storeMessageMetaData(Long messageId, MessageMetaData messageMetaData) throws AMQException
    {

    }

    public MessageMetaData getMessageMetaData(Long messageId) throws AMQException
    {
        return null;
    }

    public ContentChunk getContentBodyChunk(Long messageId, int index) throws AMQException
    {
        return null;
    }

    public boolean isPersistent()
    {
        return false;
    }

    public void storeMessageHeader(Long messageNumber, ServerMessage message)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void storeContent(Long messageNumber, long offset, ByteBuffer body)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public ServerMessage getMessage(Long messageNumber)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeQueue(final AMQQueue queue) throws AMQStoreException
    {

    }

    public void configureTransactionLog(String name,
                                        TransactionLogRecoveryHandler recoveryHandler,
                                        Configuration storeConfiguration,
                                        LogSubject logSubject) throws Exception
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public Transaction newTransaction()
    {
        return new Transaction()
        {

            public void enqueueMessage(TransactionLogResource  queue, Long messageId) throws AMQStoreException
            {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void dequeueMessage(TransactionLogResource  queue, Long messageId) throws AMQStoreException
            {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void commitTran() throws AMQStoreException
            {
                //To change body of implemented methods use File | Settings | File Templates.
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
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
    }

    public void updateQueue(AMQQueue queue) throws AMQStoreException
    {

    }

}
