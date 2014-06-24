package org.wso2.andes.server.virtualhost;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQInternalException;
import org.wso2.andes.AMQSecurityException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.binding.BindingFactory;
import org.wso2.andes.server.cluster.coordination.SubscriptionListener;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.AMQQueueFactory;
import org.wso2.andes.server.store.ConfigurationRecoveryHandler;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VirtualHostConfigSynchronizer implements
        ConfigurationRecoveryHandler.QueueRecoveryHandler,
        ConfigurationRecoveryHandler.ExchangeRecoveryHandler,
        ConfigurationRecoveryHandler.BindingRecoveryHandler, SubscriptionListener {

    private final VirtualHost _virtualHost;
    private static final Logger _logger = Logger.getLogger(VirtualHostConfigSynchronizer.class);
    private int _syncInterval;
    private boolean running = false;
    private SubscriptionStore subscriptionStore;

    private static Log log = LogFactory.getLog(VirtualHostConfigSynchronizer.class);


    public VirtualHostConfigSynchronizer(VirtualHost _virtualHost, int synchInterval) {
        this._virtualHost = _virtualHost;
        this._syncInterval = synchInterval;
        this.subscriptionStore = MessagingEngine.getInstance().getSubscriptionStore();
    }

    @Override
    public void binding(String exchangeName, String queueName, String bindingKey, ByteBuffer buf) {
        synchronized( this ) {
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

                    if (bf.getBinding(bindingKey, queue, exchange, argumentMap) == null) {

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

    @Override
    public void completeBindingRecovery() {

    }

    @Override
    public void exchange(String exchangeName, String type, boolean autoDelete) {

        synchronized( this ) {
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

    @Override
    public ConfigurationRecoveryHandler.BindingRecoveryHandler completeExchangeRecovery() {
        return null;
    }

    @Override
    public void queue(String queueName, String owner, boolean exclusive, FieldTable arguments) {

        synchronized( this ) {
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

    @Override
    public ConfigurationRecoveryHandler.ExchangeRecoveryHandler completeQueueRecovery() {
        return null;
    }

    public void start() {
        if (!running && MessagingEngine.getInstance().getSubscriptionStore() != null) {
            running = true;
            Thread t = new Thread(new VirtualHostConfigSynchronizingTask(this));
            t.setName(this.getClass().getSimpleName());
            t.start();
        }
    }

    public void stop() {
        running = false;
    }

    @Override
    public void subscriptionsChanged() {
        log.info("Handling Cluster Gossip: Synchronizing Exchanges, Queues and Bindings...");
        syncExchangesQueuesAndBindings();
    }


    /**
     * reload subscriptions
     * sync exchanges
     * sync queues
     * sync bindings
     */
    public void syncExchangesQueuesAndBindings() {
        if (subscriptionStore != null) {
            //record older queues and bindings
            List<AndesQueue> olderQueues = subscriptionStore.getDurableQueues();
            Map<String, AndesQueue> olderQueueMap = new HashMap<String, AndesQueue>();
            for (AndesQueue q : olderQueues) {
                olderQueueMap.put(q.queueName, q);
            }
            List<AndesBinding> olderBindings = subscriptionStore.getDurableBindings();
            Map<String, AndesBinding> olderBindingMap = new HashMap<String, AndesBinding>();
            for (AndesBinding b : olderBindings) {
                olderBindingMap.put(b.boundExchangeName.concat(b.routingKey).concat(b.boundQueue.queueName), b);
            }
            //update cluster subscriptions
            subscriptionStore.reloadSubscriptionsFromStorage();
            syncExchanges(this);
            syncQueues(this, olderQueueMap);
            syncBindngs(this, olderBindingMap);
        }
    }

    private class VirtualHostConfigSynchronizingTask implements Runnable {
        private VirtualHostConfigSynchronizer syc;

        public VirtualHostConfigSynchronizingTask(VirtualHostConfigSynchronizer synchronizer) {
            this.syc = synchronizer;
        }


        @Override
        public void run() {
            while (running) {
                try {
                    syncExchangesQueuesAndBindings();

                } catch (Exception e) {
                    log.error("Error while syncing Virtual host details ", e);
                }
                try {
                    Thread.sleep(_syncInterval * 1000);
                } catch (InterruptedException e) {
                    // ignore
                }

            }
        }
    }

    private void syncQueues(VirtualHostConfigSynchronizer vhcs, Map<String, AndesQueue> olderQueueMap) {
        List<AndesQueue> queues = subscriptionStore.getDurableQueues();
        for (AndesQueue queue : queues) {
            vhcs.queue(queue.queueName, queue.queueOwner, queue.isExclusive, null);
            olderQueueMap.remove(queue.queueName);
        }
        //remove queues
        for (AndesQueue q : olderQueueMap.values()) {
            removeQueue(q.queueName);
        }
    }

    private void syncBindngs(VirtualHostConfigSynchronizer vhcs, Map<String, AndesBinding> olderBindingMap) {
        try {
            List<AndesBinding> bindings = subscriptionStore.getDurableBindings();
            for (AndesBinding binding : bindings) {
                vhcs.binding(binding.boundExchangeName, binding.boundQueue.queueName, binding.routingKey, null);
                olderBindingMap.remove(binding.boundExchangeName.concat(binding.routingKey).concat(binding.boundQueue.queueName));
            }
            //remove bindings
            for (AndesBinding binding : olderBindingMap.values()) {
                removeBinding(binding.boundExchangeName, binding.boundQueue.queueName, binding.routingKey, null);
            }
        } catch (Exception e) {
            log.error("Error while syncing bindings ", e);
            throw new RuntimeException(e);
        }
    }

    private void syncExchanges(VirtualHostConfigSynchronizer vhcs) {
        try {
            List<AndesExchange> exchanges = subscriptionStore.getExchanges();
            for (AndesExchange exchange : exchanges) {
                vhcs.exchange(exchange.exchangeName, exchange.type, exchange.autoDelete != 0);
            }
        } catch (AndesException e) {
            log.error("Error while syncing exchanges");
        }

    }

    private void removeBinding(String exchangeName, String queueName, String bindingKey, ByteBuffer buf) throws AMQSecurityException, AMQInternalException {

        synchronized( this ) {
            Exchange exchange = _virtualHost.getExchangeRegistry().getExchange(exchangeName);
            if (exchange == null) {
                //_logger.error("Unknown exchange: " + exchangeName + ", cannot remove binding : " + queueName);
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
            if (queue == null) {
                return;
                //_logger.error("Unknown queue: " + queueName + ", cannot be unbind from exchange: " + exchangeName);
            } else {
                FieldTable argumentsFT = null;
                if (buf != null) {
                    argumentsFT = new FieldTable(org.apache.mina.common.ByteBuffer.wrap(buf), buf.limit());
                }

                BindingFactory bf = _virtualHost.getBindingFactory();

                Map<String, Object> argumentMap = FieldTable.convertToMap(argumentsFT);

                if (bf.getBinding(bindingKey, queue, exchange, argumentMap) != null) {

                    _logger.info("Binding Sync - Removed binding: (Exchange: " + exchange.getNameShortString() + ", Queue: " + queueName
                            + ", Routing Key: " + bindingKey + ", Arguments: " + argumentsFT + ")");

                    bf.removeBinding(bindingKey, queue, exchange, argumentMap);
                }
            }
        }
    }

    private void removeQueue(String queueName) {

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
                    throw new RuntimeException(e);
                }
                _logger.info("Queue sync - Removed Queue: " + queueName);
            }
        }
    }
}
