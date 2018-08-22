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
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.binding.BindingFactory;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.AMQQueueFactory;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.store.ConfigurationRecoveryHandler;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * VirtualHostConfigSynchronizer is for syncing exchanges,queues,bindings
 * into QPID based AMQP transport
 */
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
     * Add an messageRouter to AMQP model of the node
     *
     * @param messageRouter messageRouter to be created
     */
    public void clusterExchangeAdded(AndesMessageRouter messageRouter) throws AndesException {
        try {
            if(!(MQTTUtils.MQTT_EXCHANGE_NAME.equals(messageRouter.getName())
                    ||AMQPUtils.DLC_EXCHANGE_NAME.equals(messageRouter.getName()))) {
                exchange(messageRouter.getName(), messageRouter.getType(), messageRouter.isAutoDelete());
            }
        } catch (Exception e) {
            log.error("could not add cluster messageRouter", e);
            throw new AndesException("could not add cluster messageRouter", e);
        }
    }

    /**
     * Remove message router from AMQP model of the node
     *
     * @param exchangeName name of exchange to be removed
     */
    public void clusterExchangeRemoved(String exchangeName) throws AndesException {
        try {
            if(!(MQTTUtils.MQTT_EXCHANGE_NAME.equals(exchangeName)
                    ||AMQPUtils.DLC_EXCHANGE_NAME.equals(exchangeName))) {
                removeExchange(exchangeName);
            }
        } catch (Exception e) {
            throw new AndesException("could not remove cluster exchange from Internal Qpid registry", e);
        }
    }

    /**
     * Create a queue in AMQP model of the node
     *
     * @param queue queue to be created
     */
    public void clusterQueueAdded(StorageQueue queue) throws AndesException {
        try {
            //It is not possible to check if queue is bound to
            // MQTT Exchange as binding sync is done later. Thus checking the name
            if(!queue.getName().contains(AndesUtils.MQTT_TOPIC_STORAGE_QUEUE_PREFIX)) {
                queue(queue.getName(), queue.getQueueOwner(), queue.isExclusive(), null);
            }
        } catch (Exception e) {
            log.error("could not add cluster queue", e);
            throw new AndesException("could not add cluster queue : " + queue.toString(), e);
        }

    }

    /**
     * Remove queue from AMQP model of the node
     *
     * @param queue queue to be removed
     */
    public void clusterQueueRemoved(StorageQueue queue) throws AndesException {
        try {
            log.info("Queue removal request received queue= " + queue.getName());
            if(!queue.getName().contains(AndesUtils.MQTT_TOPIC_STORAGE_QUEUE_PREFIX)) {
                removeQueue(queue.getName());
            }
        } catch (Exception e) {
            log.error("could not remove cluster queue", e);
            throw new AndesException("could not remove cluster queue : " + queue.toString(), e);
        }
    }

    /**
     * Add binding to AMQP model of the node
     *
     * @param binding binding to be added
     */
    public void clusterBindingAdded(AndesBinding binding) throws AndesException {
        try {

            String boundQueueName = binding.getBoundQueue().getName();
            if(!(boundQueueName.contains(AndesUtils.MQTT_TOPIC_STORAGE_QUEUE_PREFIX)
                    || DLCQueueUtils.isDeadLetterQueue(boundQueueName))) {
                binding(binding.getMessageRouterName(),
                        binding.getBoundQueue().getName(),
                        binding.getBindingKey(),
                        null);
            }
        } catch (Exception e) {
            log.error("could not add cluster binding + " + binding.toString(), e);
            throw new AndesException("could not add cluster binding : " + binding.toString(), e);
        }
    }

    /**
     * Remove binding from AMQP model of the node
     *
     * @param binding binding to be removed
     */
    public void clusterBindingRemoved(AndesBinding binding) throws AndesException {
        try {
            String boundQueueName = binding.getBoundQueue().getName();
            if(!(boundQueueName.contains(AndesUtils.MQTT_TOPIC_STORAGE_QUEUE_PREFIX)
                    || DLCQueueUtils.isDeadLetterQueue(boundQueueName))) {
                removeBinding(binding.getMessageRouterName(),
                        binding.getBoundQueue().getName(),
                        binding.getBindingKey(),
                        null);
            }

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
                    exchange = _virtualHost.getExchangeFactory().createExchange(exchangeNameSS,
                            new AMQShortString(type), true, autoDelete, 0);
                    _virtualHost.getExchangeRegistry().registerExchange(exchange);
                    if(_logger.isDebugEnabled()) {
                        _logger.debug("Added Exchange: " + exchangeName
                                + ", Type: " + type + ", autoDelete: " + autoDelete);
                    }
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
                    q = AMQQueueFactory.createAMQQueueImpl(queueNameShortString,
                            true,
                            owner == null ? null : new AMQShortString(owner),
                            false, exclusive,
                            _virtualHost, arguments);

                    _virtualHost.getQueueRegistry().registerQueue(q);

                    if(_logger.isDebugEnabled()) {
                        _logger.debug("Queue sync - Added Queue: " + queueName
                                + ", Owner: " + owner + ", IsExclusive: " + exclusive + ", Arguments: " + arguments);
                    }
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

                AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(new AMQShortString(queueName));
                if (queue == null) {
                    _logger.error("Unknown queue: " + queueName + ", cannot be bound to exchange: " + exchangeName);
                } else {
                    FieldTable argumentsFT = null;
                    if (buf != null) {
                        argumentsFT = new FieldTable(org.wso2.org.apache.mina.common.ByteBuffer.wrap(buf), buf.limit());
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
                        if(_logger.isDebugEnabled()) {
                            _logger.debug("Binding Sync - Added  Binding: (Exchange: "
                                    + exchange.getNameShortString() + ", Queue: " + queueName
                                    + ", Routing Key: " + bindingKey + ", Arguments: " + argumentsFT + ")");
                        }
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
    private void removeBinding(String exchangeName, String queueName, String bindingKey, ByteBuffer buf)
            throws AMQSecurityException, AMQInternalException {

        synchronized (this) {
            Exchange exchange = _virtualHost.getExchangeRegistry().getExchange(exchangeName);
            if (exchange == null) {
                return;
            }

            AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(new AMQShortString(queueName));
            if (queue != null) {
                FieldTable argumentsFT = null;
                if (buf != null) {
                    argumentsFT = new FieldTable(org.wso2.org.apache.mina.common.ByteBuffer.wrap(buf), buf.limit());
                }

                BindingFactory bf = _virtualHost.getBindingFactory();

                Map<String, Object> argumentMap = FieldTable.convertToMap(argumentsFT);

                if (bf.getBinding(bindingKey, queue, exchange, argumentMap) != null) {

                    if(_logger.isDebugEnabled()) {
                        _logger.debug("Binding Sync - Removed binding: (Exchange: " + exchange.getNameShortString()
                                + ", Queue: " + queueName
                                + ", Routing Key: " + bindingKey + ", Arguments: " + argumentsFT + ")");
                    }

                    bf.removeBinding(bindingKey, queue, exchange, argumentMap, false);
                }
            }
        }
    }

    /**
     * Remove queue from internal qpid node
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
                    q.remoteDelete();
                } catch (AMQException e) {
                    _logger.error("Error while removing the queue " + queueName);
                    throw new AndesException(e);
                }
                if(_logger.isDebugEnabled()) {
                    _logger.debug("Queue sync - Removed Queue: " + queueName);
                }
            }
        }
    }

    /**
     * Remove exchange from internal qpid node
     *
     * @param exchangeName name of exchange to be removed
     * @throws AMQException
     */
    private void removeExchange(String exchangeName) throws AMQException {

        synchronized (this) {
            _virtualHost.getExchangeRegistry().unregisterExchange(exchangeName, true);
            if(_logger.isDebugEnabled()) {
                _logger.debug("Exchange sync - Removed Exchange: " + exchangeName);
            }
        }
    }
}
