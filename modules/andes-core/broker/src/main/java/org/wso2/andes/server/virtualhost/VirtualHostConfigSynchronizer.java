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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQInternalException;
import org.wso2.andes.AMQSecurityException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.binding.BindingFactory;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.AMQQueueFactory;
import org.wso2.andes.server.store.ConfigurationRecoveryHandler;

import java.nio.ByteBuffer;
import java.util.Map;

public class VirtualHostConfigSynchronizer implements
        ConfigurationRecoveryHandler.QueueRecoveryHandler,
        ConfigurationRecoveryHandler.ExchangeRecoveryHandler,
        ConfigurationRecoveryHandler.BindingRecoveryHandler {

    private final VirtualHost _virtualHost;
    private static final Logger _logger = Logger.getLogger(VirtualHostConfigSynchronizer.class);
    private static Log log = LogFactory.getLog(VirtualHostConfigSynchronizer.class);


    public VirtualHostConfigSynchronizer(VirtualHost _virtualHost) {
        this._virtualHost = _virtualHost;
    }


    @Override
    public ConfigurationRecoveryHandler.BindingRecoveryHandler completeExchangeRecovery() {
        return null;
    }

    @Override
    public ConfigurationRecoveryHandler.ExchangeRecoveryHandler completeQueueRecovery() {
        return null;
    }

    @Override
    public void completeBindingRecovery() {

    }

    /**
     * add an exchange to the local node
     *
     * @param exchange exchange to be created
     */
    public void clusterExchangeAdded(AndesExchange exchange) throws AndesException {
        try {
            exchange(exchange.exchangeName, exchange.type, exchange.autoDelete);
            AndesContext.getInstance().getAMQPConstructStore().addExchange(exchange, false);
        } catch (Exception e) {
            log.error("could not add cluster exchange", e);
            throw new AndesException("could not add cluster exchange", e);
        }
    }

    /**
     * remove exchange from local node
     *
     * @param exchange exchange to be removed
     */
    public void clusterExchangeRemoved(AndesExchange exchange) throws AndesException {
        try {
            removeExchange(exchange.exchangeName);
        } catch (Exception e) {
            log.error("could not remove cluster exchange", e);
            throw new AndesException("could not remove cluster exchange", e);
        }
    }

    /**
     * create a queue in local node
     *
     * @param queue queue to be created
     */
    public void clusterQueueAdded(AndesQueue queue) throws AndesException {
        try {
            queue(queue.queueName, queue.queueOwner, queue.isExclusive, null);
            AndesContext.getInstance().getAMQPConstructStore().addQueue(queue, false);
        } catch (Exception e) {
            log.error("could not add cluster queue", e);
            throw new AndesException("could not add cluster queue : " + queue.toString(), e);
        }

    }

    /**
     * remove queue from local node
     *
     * @param queue queue to be removed
     */
    public void clusterQueueRemoved(AndesQueue queue) throws AndesException {
        try {
            log.info("Queue removal request received queue= " + queue.queueName);
            removeQueue(queue.queueName);
            AndesContext.getInstance().getAMQPConstructStore().removeQueue(queue.queueName, false);
        } catch (Exception e) {
            log.error("could not remove cluster queue", e);
            throw new AndesException("could not remove cluster queue : " + queue.toString(), e);
        }
    }

    /**
     * Check if queue is deletable
     * @param queue AMQQueue
     * @return possibility of deleting queue
     * @throws AMQException
     */
    public boolean checkIfQueueDeletable(AMQQueue queue) throws AMQException{
        try {
            return Andes.getInstance().checkIfQueueDeletable(AMQPUtils.createAndesQueue(queue));
        } catch (AndesException e) {
            log.error("error while checking if queue is deletable", e);
            throw new AMQException("error while checking if queue is deletable : " + queue, e);
        }
    }

    /**
     * purge queue from local node - clear all in memory message buffers for the queue in this node.
     *
     * @param queue
     * @throws AndesException
     */
    public void clusterQueuePurged(AndesQueue queue) throws AndesException {
        try {
            log.info("Queue purge request received queue= " + queue.queueName);
            MessagingEngine.getInstance().clearMessagesFromQueueInMemory(queue.queueName,
                    queue.getLastPurgedTimestamp());

        } catch (AndesException e) {
            throw new AndesException("Could not purge cluster queue : " + queue.toString(), e);
        }
    }

    /**
     * add binding to the local node
     *
     * @param binding binding to be added
     */
    public void clusterBindingAdded(AndesBinding binding) throws AndesException {
        try {
            binding(binding.boundExchangeName, binding.boundQueue.queueName, binding.routingKey, null);
            AndesContext.getInstance().getAMQPConstructStore().addBinding(binding, false);
        } catch (Exception e) {
            log.error("could not add cluster binding + " + binding.toString(), e);
            throw new AndesException("could not add cluster binding : " + binding.toString(), e);
        }
    }

    /**
     * remove binding rom local node
     *
     * @param binding binding to be removed
     */
    public void clusterBindingRemoved(AndesBinding binding) throws AndesException {
        try {
            removeBinding(binding.boundExchangeName, binding.boundQueue.queueName, binding.routingKey, null);
        } catch (Exception e) {
            log.error("could not remove cluster binding + " + binding.toString(), e);
            throw new AndesException("could not remove cluster binding : " + binding.toString(), e);
        }
    }

    //add the exchange into internal qpid if NOT present
    @Override
    public void exchange(String exchangeName, String type, boolean autoDelete) {
        synchronized (this) {
            try {
                Exchange exchange;
                AMQShortString exchangeNameSS = new AMQShortString(exchangeName);
                exchange = _virtualHost.getExchangeRegistry().getExchange(exchangeNameSS);
                if (exchange == null) {
                    exchange = _virtualHost.getExchangeFactory().createExchange(exchangeNameSS, new AMQShortString(type), true, autoDelete, 0);
                    _virtualHost.getExchangeRegistry().registerExchange(exchange);
                    _logger.info("Added Exchange: " + exchangeName
                            + ", Type: " + type + ", autoDelete: " + autoDelete);
                }
            } catch (AMQException e) {
                log.error("Error while creating Exchanges", e);
                throw new RuntimeException(e);
            }
        }
    }

    //add the queue into internal qpid if NOT present
    @Override
    public void queue(String queueName, String owner, boolean exclusive, FieldTable arguments) {

        synchronized (this) {
            try {
                AMQShortString queueNameShortString = new AMQShortString(queueName);

                AMQQueue q = _virtualHost.getQueueRegistry().getQueue(queueNameShortString);

                if (q == null) {
                    //if a new durable queue is added we can know it here
                    q = AMQQueueFactory.createAMQQueueImpl(queueNameShortString, true, owner == null ? null : new AMQShortString(owner), false, exclusive, _virtualHost,
                            arguments);
                    _virtualHost.getQueueRegistry().registerQueue(q);
                    _logger.info("Queue sync - Added Queue: " + queueName
                            + ", Owner: " + owner + ", IsExclusive: " + exclusive + ", Arguments: " + arguments);
                }
            } catch (AMQException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //add the binding into internal qpid if NOT present
    @Override
    public void binding(String exchangeName, String queueName, String bindingKey, ByteBuffer buf) {
        synchronized (this) {
            try {
                Exchange exchange = _virtualHost.getExchangeRegistry().getExchange(exchangeName);
                if (exchange == null) {
                    _logger.error("Unknown exchange: " + exchangeName + ", cannot bind queue : " + queueName);
                    return;
                }

                //we do not sync durable topic bindings
    /*            if (exchange.getName().equals("amq.topic")) {
                    if (log.isDebugEnabled()) {
                        log.debug("syncing binding excluding durable topic bindings");
                    }
                    return;
                }*/

                AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(new AMQShortString(queueName));
                if (queue == null) {
                    _logger.error("Unknown queue: " + queueName + ", cannot be bound to exchange: " + exchangeName);
                } else {
                    FieldTable argumentsFT = null;
                    if (buf != null) {
                        argumentsFT = new FieldTable(org.apache.mina.common.ByteBuffer.wrap(buf), buf.limit());
                    }

                    BindingFactory bf = _virtualHost.getBindingFactory();

                    Map<String, Object> argumentMap = FieldTable.convertToMap(argumentsFT);

                    boolean isBindingAlreadyPresent = true;
                    if (bf.getBinding(bindingKey, queue, exchange, argumentMap) == null) {
                        //for direct exchange do an additional check to see if a binding is
                        //already added to default exchange. We do not need duplicates as default
                        //exchange is an direct exchange
                        if (exchange.getName().equals(AMQPUtils.DIRECT_EXCHANGE_NAME)) {
                            Exchange testExchange = _virtualHost.getExchangeRegistry().getExchange(
                                    AMQPUtils.DEFAULT_EXCHANGE_NAME);
                            if (bf.getBinding(bindingKey, queue, testExchange,
                                              argumentMap) == null) {
                                isBindingAlreadyPresent = false;
                            }
                        } else {
                            isBindingAlreadyPresent = false;
                        }
                    }
                    if(!isBindingAlreadyPresent) {
                        _logger.info("Binding Sync - Added  Binding: (Exchange: " + exchange.getNameShortString() + ", Queue: " + queueName
                                     + ", Routing Key: " + bindingKey + ", Arguments: " + argumentsFT + ")");

                        bf.restoreBinding(bindingKey, queue, exchange, argumentMap);
                    }
                }
            } catch (AMQException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Remove binding from internal qpid node
     *
     * @param exchangeName exchange of the binding
     * @param queueName    name of the queue of binding
     * @param bindingKey   routing key
     * @param buf          arguments
     * @throws AMQSecurityException
     * @throws AMQInternalException
     */
    private void removeBinding(String exchangeName, String queueName, String bindingKey, ByteBuffer buf) throws AMQSecurityException, AMQInternalException {

        synchronized (this) {
            Exchange exchange = _virtualHost.getExchangeRegistry().getExchange(exchangeName);
            if (exchange == null) {
                return;
            }

    /*        //we do not sync durable topic bindings
            if (exchange.getName().equals("amq.topic")) {
                if (log.isDebugEnabled()) {
                    log.debug("syncing binding excluding durable topic bindings");
                }
                return;
            }*/

            AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(new AMQShortString(queueName));
            if (queue != null) {
                FieldTable argumentsFT = null;
                if (buf != null) {
                    argumentsFT = new FieldTable(org.apache.mina.common.ByteBuffer.wrap(buf), buf.limit());
                }

                BindingFactory bf = _virtualHost.getBindingFactory();

                Map<String, Object> argumentMap = FieldTable.convertToMap(argumentsFT);

                if (bf.getBinding(bindingKey, queue, exchange, argumentMap) != null) {

                    _logger.info("Binding Sync - Removed binding: (Exchange: " + exchange.getNameShortString() + ", Queue: " + queueName
                            + ", Routing Key: " + bindingKey + ", Arguments: " + argumentsFT + ")");

                    bf.removeBinding(bindingKey, queue, exchange, argumentMap, false);
                }
            }
        }
    }

    /**
     * remove queue from internal qpid node
     *
     * @param queueName name of the queue to be removed
     * @throws AndesException
     */
    private void removeQueue(String queueName) throws AndesException {

        synchronized (this) {
            AMQShortString queueNameShortString = new AMQShortString(queueName);
            AMQQueue q = _virtualHost.getQueueRegistry().getQueue(queueNameShortString);
            if (q != null) {
                /**
                 * 1. remove all the bindings
                 * 2. unregister queue
                 */
                try {
                    q.delete();
                } catch (AMQException e) {
                    _logger.error("Error while removing the queue " + queueName);
                    throw new AndesException(e);
                }
                _logger.info("Queue sync - Removed Queue: " + queueName);
            }
        }
    }

    /**
     * remove exchange from internal qpid node
     *
     * @param exchangeName name of exchange to be removed
     * @throws AMQException
     */
    private void removeExchange(String exchangeName) throws AMQException {

        synchronized (this) {
            _virtualHost.getExchangeRegistry().unregisterExchange(exchangeName, true);
            _logger.info("Exchange sync - Removed Exchange: " + exchangeName);
        }
    }
}
