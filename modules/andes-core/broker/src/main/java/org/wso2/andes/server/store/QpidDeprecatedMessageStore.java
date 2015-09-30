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

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQStoreException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.kernel.*;
import org.wso2.andes.store.StoredAMQPMessage;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.subscription.SubscriptionStore;

/**
 * Implementations of {#{@link org.wso2.andes.kernel.MessageStore} replaces the functionality provided by this class
 */
@Deprecated
public class QpidDeprecatedMessageStore implements MessageStore {

    private boolean configured = false;
    private static Log log =
            LogFactory.getLog(QpidDeprecatedMessageStore.class);
    private SubscriptionStore subscriptionStore;

    /**
     * Set CassandraMessageStore at ClusterResourceHolder
     */
    public QpidDeprecatedMessageStore() {
        ClusterResourceHolder.getInstance().setQpidDeprecatedMessageStore(this);
    }

    @Override
    public void configureConfigStore(String name, ConfigurationRecoveryHandler recoveryHandler,

                                     Configuration config, LogSubject logSubject) throws Exception {
        if (!configured) {
            performCommonConfiguration(config);
            recover(recoveryHandler);
        }

    }

    @Override
    /**
     * Initialise the message store
     */
    public void configureMessageStore(String name, MessageStoreRecoveryHandler recoveryHandler,
                                      Configuration config, LogSubject logSubject) throws Exception {
        if (!configured) {
            performCommonConfiguration(config);
        }
        //todo recoverMessages(): hasitha : is there anything to recover? All messages are durable
    }

    /**
     * Perform configurations using the configurations at cluster
     *
     * @param configuration configuration object
     * @throws Exception
     */
    private void performCommonConfiguration(Configuration configuration) throws Exception {
        subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        configured = true;
    }

    /**
     * recover bindings
     *
     * @param recoveryHandler recovery handler
     * @throws AMQException
     */
    public void recover(ConfigurationRecoveryHandler recoveryHandler) throws AMQException {


/*        boolean readyOrTimeOut = false;
        boolean error = false;

        int initTimeOut = 10;
        int count = 0;
        int maxTries = 10;

        while (!readyOrTimeOut) {
            try {
                ConfigurationRecoveryHandler.QueueRecoveryHandler qrh = recoveryHandler.begin(this);
                loadQueues(qrh);

                ConfigurationRecoveryHandler.ExchangeRecoveryHandler erh = qrh.completeQueueRecovery();
                List<String> exchanges = loadExchanges(erh);
                ConfigurationRecoveryHandler.BindingRecoveryHandler brh = erh.completeExchangeRecovery();
                recoverBindings(brh, exchanges);
                brh.completeBindingRecovery();
            } catch (Exception e) {
                error = true;
                log.error("Error recovering persistent state: " + e.getMessage(), e);
            } finally {
                if (!error) {
                    readyOrTimeOut = true;
                    continue;
                } else {
                    long waitTime = initTimeOut * 1000 * (long) Math.pow(2, count);
                    log.warn("Waiting for Cluster data to be synced Please ,start the other nodes soon, wait time: "
                            + waitTime + "ms");
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException e) {

                    }
                    if (count > maxTries) {
                        readyOrTimeOut = true;
                        throw new AMQStoreException("Max Backoff attempts expired for data recovery");
                    }
                    count++;
                }
            }

        }*/


    }

    /**
     * at recovery load queues which were there when shutting down
     *
     * @param qrh Queue Recovery Handler
     * @throws Exception
     */
    public void loadQueues(ConfigurationRecoveryHandler.QueueRecoveryHandler qrh) throws Exception {
/*        try {
            if(subscriptionStore == null) {
                subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
            }
            List<AndesQueue> queues = subscriptionStore.getDurableQueues();
            for(AndesQueue queue : queues) {
                qrh.queue(queue.queueName, queue.queueOwner, queue.isExclusive, null);
            }

        } catch (Exception e) {
            throw new AMQStoreException("Error in loading queues", e);
        }*/


    }

    public void recoverBindings(ConfigurationRecoveryHandler.BindingRecoveryHandler brh,
                                List<String> exchanges) throws Exception {
/*        List<AndesBinding> bindings = subscriptionStore.getDurableBindings();
        for(AndesBinding b : bindings) {
            brh.binding(b.boundExchangeName, b.boundQueue.queueName, b.routingKey, null);
        }*/

    }

    @Override
    /**
     * close and stop tasks running under cassandra message store
     */
    public void close() throws Exception {

    }

    @Override
    public <T extends StorableMessageMetaData> StoredMessage<T> addMessage(T metaData) {
        try {
            long mid = MessagingEngine.getInstance().generateUniqueId();
            if (log.isDebugEnabled()) {
                log.debug("MessageID generated:" + mid);
            }
            return new StoredAMQPMessage(mid, metaData);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    /**
     * Create a new exchange adding it to the store
     */
    public void createExchange(Exchange exchange) throws AMQStoreException {

        //we do nothing here now
    }

/*    */

    /**
     * Load exchanges at a recovery from the permanent cassandra storage
     *
     * @return list of exchanges
     * @throws Exception
     *//*
    public List<String> loadExchanges(ConfigurationRecoveryHandler.ExchangeRecoveryHandler erh)
            throws Exception {
        List<String> exchangeNames = new ArrayList<String>();
        List<AndesExchange> exchanges = subscriptionStore.getExchanges();
        for(AndesExchange exchange : exchanges) {
            exchangeNames.add(exchange.exchangeName);
            erh.exchange(exchange.exchangeName,exchange.type,exchange.autoDelete);
        }

        return exchangeNames;
    }*/
    @Override
    public void removeExchange(Exchange exchange) throws AMQStoreException {
/*        try {
            subscriptionStore.deleteExchange(new AndesExchange(exchange.getName(), exchange.getTypeShortString().asString(),
                    exchange.isAutoDelete() ? (short) 1 : (short) 0));
        } catch (AndesException e) {
            throw new AMQStoreException("Error while removing exchage", e);
        }*/
    }

    @Override
    /**
     * bind a queue to an exchange in durable subscriptions
     */
    public void bindQueue(Exchange exchange, AMQShortString routingKey,
                          AMQQueue queue, FieldTable args) throws AMQStoreException {

        //addBinding(exchange, queue, routingKey.asString());
        // we do nothing here now


    }

    @Override
    public void unbindQueue(Exchange exchange, AMQShortString routingKey, AMQQueue queue, FieldTable args) throws AMQStoreException {

        //removeBinding(exchange, queue, routingKey.asString());
        //we do nothing here now

    }

    @Override
    public void createQueue(AMQQueue queue, FieldTable arguments) throws AMQStoreException {
        //createQueue(queue);
        //we do nothing here now
    }

    public void createQueue(AMQQueue queue) {
        //we do nothing here now
    }

    @Override
    /**
     * Remove destination queue detail from cassandra
     */
    public void removeQueue(AMQQueue queue) throws AMQStoreException {
/*        try {
        subscriptionStore.removeQueue(queue.getName(), queue.isExclusive());
        } catch (AndesException e) {
            throw new AMQStoreException("Error while deleting queue : " + queue, e);
        }*/

    }

    @Override
    /**
     * Update queue detail in Cassandra. This is only for durable queues
     */
    public void updateQueue(AMQQueue queue) throws AMQStoreException {

        //we do nothing here now
    }

    @Override
    public void configureTransactionLog(String name, TransactionLogRecoveryHandler recoveryHandler,
                                        Configuration storeConfiguration, LogSubject logSubject) throws Exception {
    }

    @Override
    public Transaction newTransaction() {
        return new AndesTransaction();
    }

    public boolean isConfigured() {
        return configured;
    }

    //inner class handling Cassandra Transactions
    private class AndesTransaction implements Transaction {

        public void enqueueMessage(final TransactionLogResource queue, final Long messageId)
                throws AMQStoreException {

        }

        /**
         * dequeue message from queue entries for transactions
         *
         * @param queue     The queue to place the message on.
         * @param messageId The message to dequeue.
         * @throws AMQStoreException
         */
        public void dequeueMessage(final TransactionLogResource queue, Long messageId) throws AMQStoreException {

        }

        public void commitTran() throws AMQStoreException {

        }

        public StoreFuture commitTranAsync() throws AMQStoreException {
            return new StoreFuture() {
                public boolean isComplete() {
                    return true;
                }

                public void waitForCompletion() {

                }
            };
        }

        public void abortTran() throws AMQStoreException {

        }
    }
}

