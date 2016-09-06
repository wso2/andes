/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.disruptor.inbound.InboundBindingEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundBindingSyncEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundExchangeEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundExchangeSyncEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundQueueEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundQueueSyncEvent;
import org.wso2.andes.kernel.disruptor.inbound.QueueInfo;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.router.MessageRouterFactory;
import org.wso2.andes.kernel.router.QueueMessageRouter;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationAgent;
import org.wso2.andes.server.cluster.coordination.CoordinationComponentFactory;

import java.util.List;

/**
 * This class is for managing control information of
 * Andes. (eg: exchanges/queues/bindings)
 */
public class AndesContextInformationManager {

    /**
     * The logger used for logging information, warnings, errors and etc.
     */
    private static final Log log = LogFactory.getLog(AndesContextInformationManager.class);

    /**
     * Reference to AndesContextStore to manage exchanges/bindings and queues in persistence storage 
     */
    private AndesContextStore contextStore;

    /**
     * Factory for creating AndesMessageRouters
     */
    private MessageRouterFactory messageRouterFactory;

    /**
     * Reference to message store to be used from message count related functionality 
     */
    private MessageStore messageStore;

    /**
     * To manage exchanges bindings and queues
     */
    private AMQPConstructStore amqpConstructStore;

    /**
     * Manages all operations related to subscription changes such as addition, disconnection and deletion
     */
    AndesSubscriptionManager subscriptionManager;

    /**
     * Notification agent used to send notifications to other nodes in cluster on changes
     */
    private ClusterNotificationAgent clusterNotificationAgent;

    /**
     * Initializes the andes context information manager
     *
     * @param constructStore store managing AMQP related artifacts
     * @param subscriptionManager manager for all subscriptions
     * @param contextStore store persisting message routers, queues, subscribers and AMQP specific artifacts
     * @param messageStore store persisting messages
     */
    public AndesContextInformationManager(AMQPConstructStore constructStore,
                                          AndesSubscriptionManager subscriptionManager,
                                          AndesContextStore contextStore,
                                          MessageStore messageStore) throws AndesException {

        this.subscriptionManager = subscriptionManager;
        this.messageRouterFactory = new MessageRouterFactory();
        this.messageStore = messageStore;
        this.contextStore = contextStore;
        this.amqpConstructStore = constructStore;
        CoordinationComponentFactory coordinationComponentFactory = new CoordinationComponentFactory();
        this.clusterNotificationAgent = coordinationComponentFactory.createClusterNotificationAgent();
    }

    /**
     * Create an exchange in andes kernel and notify all listeners
     *
     * @param exchangeEvent local exchangeEvent
     * @throws org.wso2.andes.kernel.AndesException
     */
    public void createExchange(InboundExchangeEvent exchangeEvent) throws AndesException {

        AndesMessageRouter messageRouter = messageRouterFactory.
                createMessageRouter(exchangeEvent.getMessageRouterName(), exchangeEvent.getType(),
                        exchangeEvent.isAutoDelete());

        AndesContext.getInstance().getMessageRouterRegistry().
                registerMessageRouter(messageRouter.getName(), messageRouter);

        contextStore.storeExchangeInformation(messageRouter.getName(), messageRouter.encodeAsString());

        clusterNotificationAgent.notifyMessageRouterChange(messageRouter, ClusterNotificationListener
                .MessageRouterChange.Added);
    }

    /**
     * Sync exchange creation. This will create an exchange inside Andes
     *
     * @param exchangeSyncEvent event information
     * @throws AndesException
     */
    public void syncExchangeCreate(InboundExchangeSyncEvent exchangeSyncEvent) throws AndesException {

        //create a message router and register
        AndesMessageRouter messageRouter = messageRouterFactory.
                createMessageRouter(exchangeSyncEvent.getEncodedExchangeInfo());
        AndesContext.getInstance().getMessageRouterRegistry().
                registerMessageRouter(messageRouter.getName(), messageRouter);

        ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer().
                clusterExchangeAdded(messageRouter);
        log.info("Message Router Sync [create]: " + messageRouter.getName());
    }

    /**
     * Delete exchange from andes kernel and notify the listeners
     *
     * @param messageRouterEvent messageRouterEvent event information
     * @throws org.wso2.andes.kernel.AndesException
     */
    public void deleteExchange(InboundExchangeEvent messageRouterEvent) throws AndesException {
        AndesMessageRouter removedRouter = AndesContext.getInstance().getMessageRouterRegistry().
                removeMessageRouter(messageRouterEvent.getMessageRouterName());
        contextStore.deleteExchangeInformation(messageRouterEvent.getMessageRouterName());
        clusterNotificationAgent.notifyMessageRouterChange(removedRouter,
                ClusterNotificationListener.MessageRouterChange.Deleted);
    }

    /**
     * Sync exchange deletion. This will remove the exchange
     * and all related information from memory
     *
     * @param exchangeSyncEvent incoming sync event
     * @throws AndesException
     */
    public void syncExchangeDelete(InboundExchangeSyncEvent exchangeSyncEvent) throws AndesException {

        AndesMessageRouter mockMessageRouter = new QueueMessageRouter(exchangeSyncEvent.getEncodedExchangeInfo());
        String messageRouterName = mockMessageRouter.getName();

        AndesMessageRouter removedRouter = AndesContext.getInstance().getMessageRouterRegistry().
                removeMessageRouter(messageRouterName);

        //remove exchange inside Qpid
        ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer().
                clusterExchangeRemoved(messageRouterName);

        log.info("Message Router Sync [delete]: " + removedRouter.getName());
    }

    /**
     * Purge storage queue. This will remove all persisted messages of the queue
     * along with memory buffered messages.
     *
     * @param queuePurgeEvent Inbound event representing queue change
     * @return number of messsages removed from persistent store
     * @throws AndesException
     */
    public int handleQueuePurge(InboundQueueEvent queuePurgeEvent) throws AndesException {

        StorageQueue queue = queuePurgeEvent.toStorageQueue();
        //get the queue from queue registry
        StorageQueue registeredQueue = AndesContext.getInstance().
                getStorageQueueRegistry().getStorageQueue(queue.getName());
        int numOfMessagesPurged = registeredQueue.purgeMessages();
        //notify other nodes
        clusterNotificationAgent.notifyQueueChange(queue, ClusterNotificationListener.QueueChange.Purged);
        return numOfMessagesPurged;
    }

    /**
     * Handle notification of a queue purge from remote node. This will remove any message
     * buffered from that queue in current node.
     *
     * @param queuePurgeNotification Inbound event representing queue change notification
     * @throws AndesException
     */
    public void handleQueuePurgeNotification(InboundQueueSyncEvent queuePurgeNotification)
            throws AndesException{

        // Clear in memory messages of self (node)
        StorageQueue queueWithEvent = queuePurgeNotification.toStorageQueue();
        StorageQueue registeredQueue = AndesContext.getInstance().getStorageQueueRegistry().getStorageQueue
                (queueWithEvent.getName());
        registeredQueue.clearMessagesReadToBufferForDelivery();

        log.info("Queue Sync [purge]: " + registeredQueue.getName());
    }

    /**
     * Create a persistent queue in andes kernel
     *
     * @param queueCreateEvent Inbound event representing queue create
     * @throws AndesException
     */
    public void createQueue(InboundQueueEvent queueCreateEvent) throws AndesException {
        StorageQueue queueEvent = queueCreateEvent.toStorageQueue();

        StorageQueue queue = AndesContext.getInstance().
                getStorageQueueRegistry().registerStorageQueue(queueEvent.getName(),
                queueEvent.isDurable(), queueEvent.isShared(), queueEvent.getQueueOwner(),
                queueEvent.isExclusive());

        contextStore.storeQueueInformation(queueEvent.getName(), queueEvent.encodeAsString());
        //create a space to keep message counter on this queue
        messageStore.addQueue(queue.getName());
        clusterNotificationAgent.notifyQueueChange(queueEvent, ClusterNotificationListener.QueueChange.Added);

        log.info("Queue Created: " + queue.getName());
    }

    /**
     * Handle notification of a queue creation of a remote node.
     *
     * @param queueCreateEvent Inbound event representing queue change notification
     * @throws AndesException
     */
    public void syncQueueCreate(InboundQueueSyncEvent queueCreateEvent) throws AndesException {
        StorageQueue queueEvent = queueCreateEvent.toStorageQueue();

        StorageQueue storageQueueToAdd = AndesContext.getInstance().
                getStorageQueueRegistry().registerStorageQueue(queueEvent.getName(),
                queueEvent.isDurable(), queueEvent.isShared(), queueEvent.getQueueOwner(),
                queueEvent.isExclusive());

        //add queue inside Qpid
        ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer().
                clusterQueueAdded(storageQueueToAdd);

        log.info("Queue Sync [create]: " + storageQueueToAdd.getName());
    }

    /**
     * Check if queue is deletable. If the queue has any subscriber cluster-wide
     * it is not deletable
     *
     * @param queueName name of the queue
     * @return true if possible to delete queue
     * @throws AndesException
     */
    public boolean checkIfQueueDeletable(String queueName) throws AndesException {
        boolean queueDeletable = false;

        Iterable<org.wso2.andes.kernel.subscription.AndesSubscription> activeSubscriptions
                = subscriptionManager.getAllSubscriptionsByQueue(queueName);

        if (!activeSubscriptions.iterator().hasNext()) {
            queueDeletable = true;
        }
        return queueDeletable;
    }

    /**
     * Delete the queue from broker. This will remove all bounded subscriptions, notify and
     * remove bindings before removing the queue.
     *
     * @param queueDeleteEvent name of the storage queue to delete
     * @throws AndesException issue on deleting queue
     */
    public void deleteQueue(InboundQueueEvent queueDeleteEvent) throws AndesException {

        StorageQueue queueWithEvent = queueDeleteEvent.toStorageQueue();
        String storageQueueName = queueWithEvent.getName();

        //remove all subscriptions to the queue
        subscriptionManager.closeAllSubscriptionsBoundToQueue(storageQueueName);

        //remove all bindings from memory if not removed
        List<AndesBinding> removedBindings = amqpConstructStore.removeAllBindingsForQueue(storageQueueName);
        for (AndesBinding removedBinding : removedBindings) {
            clusterNotificationAgent.notifyBindingsChange(removedBinding,
                    ClusterNotificationListener.BindingChange.Deleted);
        }

        //purge the queue cluster-wide. Other nodes will only delete messages buffered to memory on those nodes
        handleQueuePurge(queueDeleteEvent);

        // Remove queue information from database
        contextStore.deleteQueueInformation(storageQueueName);
        messageStore.removeQueue(storageQueueName);

        // Remove queue mapping from cache after removing it from DB
        messageStore.removeLocalQueueData(storageQueueName);

        //identify storage queue, unbind it from router and delete from queue registry
        StorageQueue storageQueue = AndesContext.getInstance().
                getStorageQueueRegistry().removeStorageQueue(storageQueueName);

        //Notify cluster to delete queue
        clusterNotificationAgent.notifyQueueChange(storageQueue, ClusterNotificationListener.QueueChange.Deleted);

        log.info("Queue Deleted: " + storageQueueName);
    }

    /**
     * Handle queue delete notification of a remote node.
     *
     * @param queueDeleteSyncEvent Inbound event representing a queue deletion
     * @throws AndesException
     */
    public void syncQueueDelete(InboundQueueSyncEvent queueDeleteSyncEvent) throws AndesException {
        StorageQueue queueWithEvent = queueDeleteSyncEvent.toStorageQueue();
        String storageQueueName = queueWithEvent.getName();

        //remove all subscriptions to the queue
        subscriptionManager.closeAllSubscriptionsBoundToQueue(storageQueueName);

        //clear in-memory messages buffered for queue
        handleQueuePurgeNotification(queueDeleteSyncEvent);

        //remove all bindings from memory if not removed
        amqpConstructStore.removeAllBindingsForQueue(storageQueueName);

        //remove queue mapping
        messageStore.removeLocalQueueData(storageQueueName);

        //identify storage queue and delete from queue registry
        StorageQueue queueToDelete = AndesContext.getInstance().
                getStorageQueueRegistry().removeStorageQueue(storageQueueName);
        //remove queue inside Qpid
        ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer().clusterQueueRemoved(queueToDelete);

        log.info("Queue Sync [delete]: " + queueToDelete.toString());
    }

    /**
     * Create andes binding in Andes kernel. At this step we create the storage queue
     * if not already created. Reason is, at AMQP queue creation there is no information
     * on bindings. Thus we cannot generate storage queue name. Here there is binding information needed.
     *
     * @param bindingEvent binding to be created
     * @throws AndesException
     */
    public void createBinding(InboundBindingEvent bindingEvent) throws AndesException {

        //check queue already exists. If not create
        QueueInfo queueInfo = bindingEvent.getBoundedQueue();
        StorageQueue queue = AndesContext.getInstance().
                getStorageQueueRegistry().getStorageQueue(queueInfo.getQueueName());
        if(null == queue) {
            InboundQueueEvent queueCreateEvent = new InboundQueueEvent(queueInfo.getQueueName(),
                    queueInfo.isDurable(), queueInfo.isShared(), queueInfo.getQueueOwner(),
                    queueInfo.isExclusive());

            queueCreateEvent.prepareForCreateQueue(this);
            createQueue(queueCreateEvent);

            queue = AndesContext.getInstance().
                    getStorageQueueRegistry().getStorageQueue(queueInfo.getQueueName());
        }

        //bind queue to messageRouter
        String bindingKey = bindingEvent.getBindingKey();
        String messageRouterName= bindingEvent.getBoundMessageRouterName();

        AndesMessageRouter messageRouter = AndesContext.getInstance().
                getMessageRouterRegistry().getMessageRouter(messageRouterName);

        queue.bindQueueToMessageRouter(bindingKey,messageRouter);

        AndesBinding binding = new AndesBinding(bindingEvent.getBoundMessageRouterName(),
                queue, bindingEvent.getBindingKey());

        amqpConstructStore.addBinding(binding, true);
        clusterNotificationAgent.notifyBindingsChange(binding, ClusterNotificationListener.BindingChange.Added);
    }

    /**
     * Hanlde binding creation notification from a remote node
     *
     * @param bindingSyncEvent Inbound event representing a binding create
     * @throws AndesException
     */
    public void syncCreateBinding(InboundBindingSyncEvent bindingSyncEvent) throws AndesException {

        AndesBinding binding = new AndesBinding(bindingSyncEvent.getEncodedBindingInfo());

        //bind queue to messageRouter
        StorageQueue queueToBind = binding.getBoundQueue();
        String messageRouterToBind = binding.getMessageRouterName();
        AndesMessageRouter messageRouter = AndesContext.getInstance().
                getMessageRouterRegistry().getMessageRouter(messageRouterToBind);
        queueToBind.bindQueueToMessageRouter(binding.getBindingKey(),messageRouter);
        messageRouter.addMapping(binding.getBindingKey(), queueToBind);

        amqpConstructStore.addBinding(binding, false);
        //add binding inside qpid
        ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer().clusterBindingAdded(binding);

        log.info("Binding Sync [create]: " + binding.toString());
    }

    /**
     * Remove andes binding from andes kernel
     *
     * @param removeBindingEvent binding remove request event
     * @throws AndesException
     */
    public void removeBinding(InboundBindingEvent removeBindingEvent) throws AndesException {
        String messageRouterName = removeBindingEvent.getBoundMessageRouterName();
        String boundQueueName = removeBindingEvent.getBoundedQueue().getQueueName();

        StorageQueue queue = AndesContext.getInstance().
                getStorageQueueRegistry().getStorageQueue(boundQueueName);

        AndesMessageRouter messageRouter = AndesContext.getInstance().
                getMessageRouterRegistry().getMessageRouter(messageRouterName);

        /*
         * queue can be null if it is already removed. For non-durable topic we keep
         * a single queue for all messages but Qpid issues binding remove calls for
         * every internal queues it create for each subscriber
         */
        if((null != queue) && (null != messageRouter)) {
            messageRouter.removeMapping(removeBindingEvent.getBindingKey(), queue);

            AndesBinding removedBinding = amqpConstructStore.removeBinding(messageRouterName, boundQueueName, true);

            clusterNotificationAgent.notifyBindingsChange(removedBinding,
                    ClusterNotificationListener.BindingChange.Deleted);

            //if a non durable queue on binding removal delete the queue if there are no more
            // subscribers. Delete call for non durable queues is prevented at Qpid-Andes bridge.
            if(!queue.isDurable() && queue.getBoundedSubscriptions().isEmpty()) {
                InboundQueueEvent queueDeleteEvent = new InboundQueueEvent(queue.getName(),
                        queue.isDurable(), queue.isShared(), queue.getQueueOwner(), queue.isExclusive());
                deleteQueue(queueDeleteEvent);
            }
        }
    }

    /**
     * Handle binding removal notification from a remote node
     *
     * @param bindingSyncEvent Inbound event representing binding removal
     * @throws AndesException
     */
    public void syncRemoveBinding(InboundBindingSyncEvent bindingSyncEvent) throws AndesException {

        AndesBinding binding = new AndesBinding(bindingSyncEvent.getEncodedBindingInfo());

        //find and remove binding
        AndesBinding removedBinding = amqpConstructStore.removeBinding(binding.getMessageRouterName(), binding
                .getBoundQueue().getName(), true);

        //unbind queue from messageRouter
        StorageQueue boundQueue = removedBinding.getBoundQueue();

        boundQueue.unbindQueueFromMessageRouter();

        //remove binding inside Qpid
        ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer().clusterBindingRemoved(removedBinding);

        log.info("Binding Sync [delete]: " + binding.toString());
    }

}
