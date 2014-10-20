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
package org.wso2.andes.server.virtualhost;

import org.wso2.andes.server.store.ConfigurationRecoveryHandler;
import org.wso2.andes.server.store.MessageStore;
import org.wso2.andes.server.store.MessageStoreRecoveryHandler;
import org.wso2.andes.server.store.StoredMessage;
import org.wso2.andes.server.store.TransactionLogRecoveryHandler;
import org.wso2.andes.server.store.TransactionLog;
import org.wso2.andes.server.store.TransactionLogResource;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.AMQQueueFactory;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.logging.subjects.MessageStoreLogSubject;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.messages.TransactionLogMessages;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.message.ServerMessage;
import org.wso2.andes.server.message.MessageTransferMessage;
import org.wso2.andes.server.binding.BindingFactory;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.AMQException;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

public class VirtualHostConfigRecoveryHandler implements ConfigurationRecoveryHandler,
                                                        ConfigurationRecoveryHandler.QueueRecoveryHandler,
                                                        ConfigurationRecoveryHandler.ExchangeRecoveryHandler,
                                                        ConfigurationRecoveryHandler.BindingRecoveryHandler,
                                                        MessageStoreRecoveryHandler,
                                                        MessageStoreRecoveryHandler.StoredMessageRecoveryHandler,
                                                        TransactionLogRecoveryHandler,
                                                        TransactionLogRecoveryHandler.QueueEntryRecoveryHandler
{
    private static final Logger _logger = Logger.getLogger(VirtualHostConfigRecoveryHandler.class);


    private final VirtualHost _virtualHost;

    private MessageStoreLogSubject _logSubject;
    private List<ProcessAction> _actions;

    private MessageStore _store;
    private TransactionLog _transactionLog;

    private final Map<String, Integer> _queueRecoveries = new TreeMap<String, Integer>();
    private Map<Long, ServerMessage> _recoveredMessages = new HashMap<Long, ServerMessage>();
    private Map<Long, StoredMessage> _unusedMessages = new HashMap<Long, StoredMessage>();



    public VirtualHostConfigRecoveryHandler(VirtualHost virtualHost)
    {
        _virtualHost = virtualHost;
    }

    public QueueRecoveryHandler begin(MessageStore store)
    {
        _logSubject = new MessageStoreLogSubject(_virtualHost,store);
        _store = store;
        CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERY_START(null, false));

        return this;
    }

    public void queue(String queueName, String owner, boolean exclusive, FieldTable arguments)
    {
        try
        {
            AMQShortString queueNameShortString = new AMQShortString(queueName);
    
            AMQQueue q = _virtualHost.getQueueRegistry().getQueue(queueNameShortString);
    
            if (q == null)
            {
                q = AMQQueueFactory.createAMQQueueImpl(queueNameShortString, true, owner == null ? null : new AMQShortString(owner), false, exclusive, _virtualHost,
                                                       arguments);
                _virtualHost.getQueueRegistry().registerQueue(q);
            }
    
            CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERY_START(queueName, true));
    
            //Record that we have a queue for recovery
            _queueRecoveries.put(queueName, 0);
        }
        catch (AMQException e)
        {
            // TODO
            throw new RuntimeException(e);
        }
    }

    public ExchangeRecoveryHandler completeQueueRecovery()
    {
        return this;
    }

    public void exchange(String exchangeName, String type, boolean autoDelete)
    {
        try
        {
            Exchange exchange;
            AMQShortString exchangeNameSS = new AMQShortString(exchangeName);
            exchange = _virtualHost.getExchangeRegistry().getExchange(exchangeNameSS);
            if (exchange == null)
            {
                exchange = _virtualHost.getExchangeFactory().createExchange(exchangeNameSS, new AMQShortString(type), true, autoDelete, 0);
                _virtualHost.getExchangeRegistry().registerExchange(exchange);
            }
        }
        catch (AMQException e)
        {
            // TODO
            throw new RuntimeException(e);
        }
    }

    public BindingRecoveryHandler completeExchangeRecovery()
    {
        return this;
    }

    public StoredMessageRecoveryHandler begin()
    {
        // TODO - log begin
        return this;
    }

    public void message(StoredMessage message)
    {
        ServerMessage serverMessage;
        switch(message.getMetaData().getType())
        {
            case META_DATA_0_8:
                serverMessage = new AMQMessage(message);
                break;
            case META_DATA_0_10:
                serverMessage = new MessageTransferMessage(message, null);
                break;
            default:
                throw new RuntimeException("Unknown message type retrieved from store " + message.getMetaData().getClass());
        }

        //_logger.debug("Recovered message with id " + serverMessage);
        

        _recoveredMessages.put(message.getMessageNumber(), serverMessage);
        _unusedMessages.put(message.getMessageNumber(), message);
    }

    public void completeMessageRecovery()
    {
        //TODO - log end
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public TransactionLogRecoveryHandler.QueueEntryRecoveryHandler begin(TransactionLog log)
    {
        _transactionLog = log;
        return this;
    }

    private static final class ProcessAction
    {
        private final AMQQueue _queue;
        private final AMQMessage _message;

        public ProcessAction(AMQQueue queue, AMQMessage message)
        {
            _queue = queue;
            _message = message;
        }

        public void process()
        {
            try
            {
                _queue.enqueue(_message);
            }
            catch(AMQException e)
            {
                throw new RuntimeException(e);
            }
        }

    }

    public void binding(String exchangeName, String queueName, String bindingKey, ByteBuffer buf)
    {
        _actions = new ArrayList<ProcessAction>();
        try
        {
            Exchange exchange = _virtualHost.getExchangeRegistry().getExchange(exchangeName);
            if (exchange == null)
            {
                _logger.error("Unknown exchange: " + exchangeName + ", cannot bind queue : " + queueName);
                return;
            }
            
            AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(new AMQShortString(queueName));
            if (queue == null)
            {
                _logger.error("Unknown queue: " + queueName + ", cannot be bound to exchange: " + exchangeName);
            }
            else
            {
                FieldTable argumentsFT = null;
                if(buf != null)
                {
                    argumentsFT = new FieldTable(org.apache.mina.common.ByteBuffer.wrap(buf),buf.limit());
                }

                BindingFactory bf = _virtualHost.getBindingFactory();

                Map<String, Object> argumentMap = FieldTable.convertToMap(argumentsFT);

                if(bf.getBinding(bindingKey, queue, exchange, argumentMap) == null)
                {

                    _logger.info("Restoring binding: (Exchange: " + exchange.getNameShortString() + ", Queue: " + queueName
                        + ", Routing Key: " + bindingKey + ", Arguments: " + argumentsFT + ")");

                    bf.restoreBinding(bindingKey, queue, exchange, argumentMap);
                }
            }
        }
        catch (AMQException e)
        {
             throw new RuntimeException(e);
        }

    }

    public void completeBindingRecovery()
    {
        //return this;
    }

    public void complete()
    {


    }

    public void queueEntry(final String queueName, long messageId)
    {
        AMQShortString queueNameShortString = new AMQShortString(queueName);

        AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(queueNameShortString);

        try
        {
            if(queue != null)
            {
                ServerMessage message = _recoveredMessages.get(messageId);
                _unusedMessages.remove(messageId);

                if(message != null)
                {


                    if (_logger.isDebugEnabled())
                    {
                        _logger.debug("On recovery, delivering " + message.getMessageNumber() + " to " + queue.getNameShortString());
                    }

                    Integer count = _queueRecoveries.get(queueName);
                    if (count == null)
                    {
                        count = 0;
                    }

                    queue.enqueue(message);

                    _queueRecoveries.put(queueName, ++count);
                }
                else
                {
                    _logger.warn("Message id " + messageId + " referenced in log as enqueued in queue " + queue.getNameShortString() + " is unknown, entry will be discarded");
                    TransactionLog.Transaction txn = _transactionLog.newTransaction();
                    txn.dequeueMessage(queue, messageId);
                    txn.commitTranAsync();
                }
            }
            else
            {
                _logger.warn("Message id " + messageId + " in log references queue " + queueName + " which is not in the configuration, entry will be discarded");
                TransactionLog.Transaction txn = _transactionLog.newTransaction();
                TransactionLogResource mockQueue =
                        new TransactionLogResource()
                        {

                            public String getResourceName()
                            {
                                return queueName;
                            }
                        };
                txn.dequeueMessage(mockQueue, messageId);
                txn.commitTranAsync();
            }

        }
        catch(AMQException e)
        {
            throw new RuntimeException(e);
        }



    }

    public void completeQueueEntryRecovery()
    {

        for(StoredMessage m : _unusedMessages.values())
        {
            _logger.warn("Message id " + m.getMessageNumber() + " in store, but not in any queue - removing....");
            m.remove();
        }

        for(Map.Entry<String,Integer> entry : _queueRecoveries.entrySet())
        {
            CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERED(entry.getValue(), entry.getKey()));

            CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERY_COMPLETE(entry.getKey(), true));
        }

        CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERY_COMPLETE(null, false));
    }

}
