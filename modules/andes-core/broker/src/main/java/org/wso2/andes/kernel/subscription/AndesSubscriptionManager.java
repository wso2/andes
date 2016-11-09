/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel.subscription;

import com.googlecode.cqengine.query.Query;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ClusterNotificationListener;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.SubscriptionListener;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionSyncEvent;
import org.wso2.andes.kernel.registry.StorageQueueRegistry;
import org.wso2.andes.kernel.registry.SubscriptionRegistry;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationAgent;
import org.wso2.andes.server.cluster.coordination.CoordinationComponentFactory;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionListener;
import org.wso2.andes.store.AndesStoreUnavailableException;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;
import org.wso2.carbon.metrics.manager.Gauge;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.googlecode.cqengine.query.QueryFactory.and;
import static com.googlecode.cqengine.query.QueryFactory.contains;
import static com.googlecode.cqengine.query.QueryFactory.equal;

/**
 * Managers subscription add/remove and subscription query tasks inside Andes kernel
 */
public class AndesSubscriptionManager implements NetworkPartitionListener, StoreHealthListener {

    private static Log log = LogFactory.getLog(AndesSubscriptionManager.class);

    /**
     * Select all nodes regardless of the filtering parameters.
     */
    private static final String SELECT_ALL_NODES = "All";

    /**
     * Factory for creating subscriptions.
     */
    private AndesSubscriptionFactory subscriptionFactory;

    /**
     * Broker wide registry for storing subscriptions
     */
    private SubscriptionRegistry subscriptionRegistry;

    /**
     * Broker wide registry for storing queues
     */
    private StorageQueueRegistry storageQueueRegistry;

    /**
     * ID of the local node
     */
    private String localNodeId;

    /**
     * True when the minimum node count is not fulfilled, False otherwise
     */
    private volatile boolean isNetworkPartitioned;

    /**
     * Listeners who are interested in local subscription changes
     */
    private List<SubscriptionListener> subscriptionListeners = new ArrayList<>();

    /**
     * Agent for notifying local subscription changes to cluster
     */
    private ClusterNotificationAgent clusterNotificationAgent;

    /**
     * Persistent store storing message router, queue, binding
     * and subscription information
     */
    private AndesContextStore andesContextStore;

    /**
     * Indicates if the underlying context/message store is available or not. True if the store is unavailable.
     */
    private boolean storeUnavailable;

    /**
     * Create a AndesSubscription manager instance. This is a static class managing
     * subscriptions.
     *
     * @param subscriptionRegistry Registry storing subscriptions
     * @param andesContextStore    Persistent store storing message router, queue, binding
     *                             and subscription information
     */
    public AndesSubscriptionManager(SubscriptionRegistry subscriptionRegistry, AndesContextStore andesContextStore)
            throws AndesException {
        this.subscriptionRegistry = subscriptionRegistry;
        this.isNetworkPartitioned = false;
        this.subscriptionFactory = new AndesSubscriptionFactory();
        this.storageQueueRegistry = AndesContext.getInstance().getStorageQueueRegistry();
        this.andesContextStore = andesContextStore;
        this.localNodeId = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        storeUnavailable = false;

        CoordinationComponentFactory coordinationComponentFactory = new CoordinationComponentFactory();
        this.clusterNotificationAgent = coordinationComponentFactory.createClusterNotificationAgent();

        if (AndesContext.getInstance().isClusteringEnabled()) {
            // network partition detection works only when clustered.
            AndesContext.getInstance().getClusterAgent().addNetworkPartitionListener(10, this);
        }

        //Add subscribers gauge to metrics manager
        MetricManager.gauge(MetricsConstants.QUEUE_SUBSCRIBERS, Level.INFO, new QueueSubscriberGauge());
        //Add topic gauge to metrics manager
        MetricManager.gauge(MetricsConstants.TOPIC_SUBSCRIBERS, Level.INFO, new TopicSubscriberGauge());

        FailureObservingStoreManager.registerStoreHealthListener(this);
    }

    /**
     * Register a subscription lister.
     * It will be notified when a subscription change happened.
     *
     * @param listener subscription listener
     */
    public void addSubscriptionListener(SubscriptionListener listener) {
        subscriptionListeners.add(listener);
    }


    public void registerSubscription(AndesSubscription subscriptionToAdd) {
        subscriptionRegistry.registerSubscription(subscriptionToAdd);
    }

    public void addLocalSubscription(InboundSubscriptionEvent subscriptionRequest) throws AndesException {

        // We don't add Subscriptions when the minimum node count is not fulfilled
        if (isNetworkPartitioned) {
            throw new SubscriptionException("Cannot add new subscription due to network partition");
        }

        StorageQueue storageQueue = storageQueueRegistry
                .getStorageQueue(subscriptionRequest.getBoundStorageQueueName());

        AndesSubscription subscription = subscriptionFactory
                .createLocalSubscription(subscriptionRequest, storageQueue);

        //binding contains some validations. Thus register should happen after binding subscriber to queue
        storageQueue.bindSubscription(subscription, subscriptionRequest.getRoutingKey());
        registerSubscription(subscription);
        //Store the subscription
        try {
            andesContextStore.storeDurableSubscription(subscription);
        } catch (AndesStoreUnavailableException exception) {
            log.warn("Could not add subscription to the store since the store became unavailable", exception);
        }
        log.info("Add Local subscription " + subscription.getProtocolType() + " " + subscription.toString());

        notifySubscriptionListeners(subscription, ClusterNotificationListener.SubscriptionChange.Added);

        clusterNotificationAgent.notifySubscriptionsChange(subscription,
                ClusterNotificationListener.SubscriptionChange.Added);

    }

    /**
     * Create a remote subscription and register in subscription registry. This subscriber has no
     * physical connection in this node. It is not bound to any storage queue.
     *
     * @param subscriptionEvent Subscription sync request
     * @throws SubscriptionException
     */
    public void addRemoteSubscription(InboundSubscriptionSyncEvent subscriptionEvent) throws SubscriptionException {

        // We don't add Subscriptions when the minimum node count is not fulfilled
        if (isNetworkPartitioned) {
            throw new SubscriptionException("Cannot add new subscription due to network partition");
        }
        AndesSubscription remoteSubscription = new AndesSubscription(subscriptionEvent.getEncodedSubscription());
        registerSubscription(remoteSubscription);
        log.info("Sync subscription [create] " + remoteSubscription.getProtocolType() + " " + remoteSubscription
                .toString());

    }

    public void closeLocalSubscription(InboundSubscriptionEvent closeSubscriptionEvent) throws AndesException {

        UUID protocolChannel = closeSubscriptionEvent.getSubscriber().getProtocolChannelID();
        AndesSubscription subscription = getSubscriptionByProtocolChannel(protocolChannel);

        removeLocalSubscriptionAndNotify(subscription);
    }

    public void closeRemoteSubscription(InboundSubscriptionSyncEvent closeSubscriptionEvent) throws AndesException {
        AndesSubscription closedSubRepresentation =
                new AndesSubscription(closeSubscriptionEvent.getEncodedSubscription());

        UUID protocolChannel = closedSubRepresentation.getSubscriberConnection().getProtocolChannelID();
        AndesSubscription subscription = getSubscriptionByProtocolChannel(protocolChannel);

        subscriptionRegistry.removeSubscription(subscription);

        log.info("Sync subscription [close] " + subscription.getProtocolType() + " " + subscription.toString());
    }

    /**
     * Remove local subscription. Unbind subscription from queue,
     * remove from registry, notify local subscription listeners and notify
     * cluster on subscription close.
     *
     * @param subscription AndesSubscription to close
     * @throws AndesException
     */
    private void removeLocalSubscriptionAndNotify(AndesSubscription subscription) throws AndesException {

        subscriptionRegistry.removeSubscription(subscription);

        StorageQueue storageQueue = subscription.getStorageQueue();

        storageQueue.unbindSubscription(subscription);

        if (!storeUnavailable) {
            try {
                andesContextStore.removeDurableSubscription(subscription);
            } catch (AndesStoreUnavailableException exception) {
                log.warn("Could not remove subscription from store since the store is unavailable", exception);
            }
        } else {
            log.warn("Cannot not remove subscription from store since the store is non-operational");
        }
        notifySubscriptionListeners(subscription, ClusterNotificationListener.SubscriptionChange.Closed);

        clusterNotificationAgent.notifySubscriptionsChange(subscription,
                ClusterNotificationListener.SubscriptionChange.Closed);

        // If there are no subscriptions for this queue, then delete it
        if (!storageQueue.isDurable() && storageQueue.getBoundSubscriptions().isEmpty() ) {

            AndesContextInformationManager contextInformationManager = AndesContext.getInstance()
                    .getAndesContextInformationManager();

            contextInformationManager.deleteQueue(storageQueue);
        }

        log.info("Remove Local Subscription " + subscription.getProtocolType() + " " + subscription.toString());
    }

    /**
     * Get mock subscribers representing inactive durable topic subscriptions on broker.
     *
     * @return List of inactive
     */
    public List<AndesSubscription> getInactiveSubscriberRepresentations() {
        List<AndesSubscription> inactiveSubscriptions = new ArrayList<>();
        List<StorageQueue> storageQueues = AndesContext.getInstance().getStorageQueueRegistry().getAllStorageQueues();
        for (StorageQueue storageQueue : storageQueues) {
            boolean isQueueDurable = storageQueue.isDurable();
            if (isQueueDurable) {
                //only durable queues are kept bounded to message routers
                String messageRouterName = storageQueue.getMessageRouter().getName();
                if (AMQPUtils.TOPIC_EXCHANGE_NAME.equals(messageRouterName)) {
                    if (!getAllSubscriptionsByQueue(ProtocolType.AMQP, storageQueue.getName()).iterator().hasNext()) {
                        AndesSubscription inactiveSubscriber = new InactiveSubscriber(storageQueue.getName(),
                                storageQueue.getName(), storageQueue, ProtocolType.AMQP);
                        inactiveSubscriptions.add(inactiveSubscriber);
                    }
                } else if (MQTTUtils.MQTT_EXCHANGE_NAME.equals(messageRouterName)) {
                    if (!getAllSubscriptionsByQueue(ProtocolType.MQTT, storageQueue.getName()).iterator().hasNext()) {
                        AndesSubscription inactiveSubscriber = new InactiveSubscriber(storageQueue.getName(),
                                storageQueue.getName(), storageQueue, ProtocolType.MQTT);
                        inactiveSubscriptions.add(inactiveSubscriber);
                    }
                }
            }

        }
        return inactiveSubscriptions;
    }

    /**
     * Get filtered subscriptions (active/inactive) that matches to search criteria.
     *
     * @param isDurable true if searching for durable subscriptions
     * @param isActive true if searching for active subscriptions
     * @param protocolType protocol of the subscription
     * @param destinationType type of subscription (QUEUE/TOPIC/DURABLE_TOPIC)
     * @param bindingKeyPattern regex to match with binding key of subscriber
     * @param subscriptionIdPattern regex to match with ID of the subscriber
     * @param connectedNode id of the node to which the subscriber is connected to
     * @return Set of subscriptions filtered according to search criteria
     * @throws AndesException
     */
    public Set<AndesSubscription> getFilteredSubscriptions(boolean isDurable, boolean isActive, ProtocolType
            protocolType, DestinationType destinationType, String bindingKeyPattern, boolean isExactMatchBindingKey,
            String subscriptionIdPattern, boolean isExactMatchSubscriptionId, String connectedNode)
            throws AndesException {

        Set<AndesSubscription> filteredSubscriptions;

        if (isActive) {
            if (isExactMatchBindingKey) {
                filteredSubscriptions = getActiveSubscriptionsByExactBindingKeyMatch(isDurable, protocolType,
                        destinationType, bindingKeyPattern, connectedNode);
                filteredSubscriptions = filterActiveSubscriptionsBySubscriptionId(filteredSubscriptions,
                        subscriptionIdPattern, isExactMatchSubscriptionId);
            } else {
                filteredSubscriptions = getActiveSubscriptionsByTokenizedBindingKeyMatch(isDurable, protocolType,
                        destinationType, bindingKeyPattern, connectedNode);
                filteredSubscriptions = filterActiveSubscriptionsBySubscriptionId(filteredSubscriptions,
                        subscriptionIdPattern, isExactMatchSubscriptionId);
            }
        } else {
            if (isExactMatchBindingKey) {
                filteredSubscriptions = getInactiveSubscriptionsByByExactBindingKeyMatch(bindingKeyPattern);
                filteredSubscriptions = filterInactiveSubscriptionsBySubscriptionId(filteredSubscriptions,
                        protocolType, destinationType, subscriptionIdPattern, isExactMatchSubscriptionId);
            } else {
                filteredSubscriptions = getInactiveSubscriptionsByTokenizedBindingKeyMatch(bindingKeyPattern);
                filteredSubscriptions = filterInactiveSubscriptionsBySubscriptionId(filteredSubscriptions,
                        protocolType, destinationType, subscriptionIdPattern, isExactMatchSubscriptionId);
            }
        }

        return filteredSubscriptions;
    }

    /**
     * Get active subscriptions filtered by identifier pattern and exact binding key.
     *
     * @param isDurable true if searching for durable subscriptions
     * @param protocolType protocol of the subscription
     * @param destinationType type of subscription (QUEUE/TOPIC/DURABLE_TOPIC)
     * @param bindingKeyPattern regex to match with binding key of subscriber
     * @param connectedNode id of the node to which the subscriber is connected to
     * @return Set of subscriptions filtered according to search criteria
     * @throws AndesException
     */
    private Set<AndesSubscription> getActiveSubscriptionsByExactBindingKeyMatch(boolean isDurable,
            ProtocolType protocolType, DestinationType destinationType, String bindingKeyPattern, String connectedNode)
            throws AndesException {

        Set<AndesSubscription> filteredSubscriptions = new HashSet<>();
        String messageRouter = destinationType.getAndesMessageRouter();
        // query for CQ engine to get the active subscriptions based on the filtering parameters
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription.DURABILITY, isDurable), equal(AndesSubscription.PROTOCOL, protocolType),
                 equal(AndesSubscription.ROUTER_NAME, messageRouter),
                 equal(AndesSubscription.ROUTING_KEY, bindingKeyPattern.toLowerCase()));

        if(!SELECT_ALL_NODES.equals(connectedNode)){
            subscriptionQuery = and (subscriptionQuery, equal(AndesSubscription.NODE_ID, connectedNode));
        }

        Iterable<AndesSubscription> subscriptions = subscriptionRegistry.exucuteQuery(subscriptionQuery);

        for(AndesSubscription subscription : subscriptions){
            filteredSubscriptions.add(subscription);
        }
        return filteredSubscriptions;
    }

    /**
     * Get inactive subscriptions filtered by identifier pattern and exact binding key.
     *
     * @param bindingKeyPattern regex to match with binding key of subscriber
     * @return Set of subscriptions filtered according to search criteria
     * @throws AndesException
     */
    private Set<AndesSubscription> getInactiveSubscriptionsByByExactBindingKeyMatch(String bindingKeyPattern)
            throws AndesException {

        Set<AndesSubscription> filteredSubscriptions = new HashSet<>();
        List<AndesSubscription> allInactiveSubscriptions = getInactiveSubscriberRepresentations();

        for (AndesSubscription inactiveSubscription : allInactiveSubscriptions) {
            if(inactiveSubscription.getStorageQueue().getMessageRouterBindingKey().equalsIgnoreCase
                    (bindingKeyPattern)){
                filteredSubscriptions.add(inactiveSubscription);
            }
        }
        return filteredSubscriptions;
    }

    /**
     * Get active subscriptions filtered by identifier pattern and tokenized binding key.
     *
     * @param isDurable true if searching for durable subscriptions
     * @param protocolType protocol of the subscription
     * @param destinationType type of subscription (QUEUE/TOPIC/DURABLE_TOPIC)
     * @param bindingKeyPattern regex to match with binding key of subscriber
     * @param connectedNode id of the node to which the subscriber is connected to
     * @return Set of subscriptions filtered according to search criteria
     * @throws AndesException
     */
    private Set<AndesSubscription> getActiveSubscriptionsByTokenizedBindingKeyMatch(boolean isDurable,
            ProtocolType protocolType, DestinationType destinationType, String bindingKeyPattern, String
            connectedNode) throws AndesException {

        Set<AndesSubscription> filteredSubscriptions = new HashSet<>();
        String messageRouter = destinationType.getAndesMessageRouter();
        // query for CQ engine to get the active subscriptions based on the filtering parameters.
        // since there is a need for contains search in binding key, contains operation of CQ engine is used in query
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription.DURABILITY, isDurable), equal(AndesSubscription.PROTOCOL, protocolType),
                 equal(AndesSubscription.ROUTER_NAME, messageRouter),
                 contains(AndesSubscription.ROUTING_KEY, bindingKeyPattern.toLowerCase()));

        if(!SELECT_ALL_NODES.equals(connectedNode)){
            subscriptionQuery = and (subscriptionQuery, equal(AndesSubscription.NODE_ID, connectedNode));
        }

        Iterable<AndesSubscription> subscriptions = subscriptionRegistry.exucuteQuery(subscriptionQuery);

        for (AndesSubscription subscription : subscriptions) {
            filteredSubscriptions.add(subscription);
        }
        return filteredSubscriptions;
    }



    /**
     * Get inactive subscriptions filtered by identifier pattern and tokenized binding key.
     *
     * @param bindingKeyPattern regex to match with binding key of subscriber
     * @return Set of subscriptions filtered according to search criteria
     * @throws AndesException
     */
    private Set<AndesSubscription> getInactiveSubscriptionsByTokenizedBindingKeyMatch(String bindingKeyPattern)
            throws AndesException {

        Set<AndesSubscription> filteredSubscriptions = new HashSet<>();

        List<AndesSubscription> allInactiveSubscriptions = getInactiveSubscriberRepresentations();

        for (AndesSubscription inactiveSubscription : allInactiveSubscriptions) {
            if(StringUtils.containsIgnoreCase(inactiveSubscription.getStorageQueue().getMessageRouterBindingKey(),
                    bindingKeyPattern)){
                filteredSubscriptions.add(inactiveSubscription);
            }
        }
        return filteredSubscriptions;
    }

    /**
     * Filter inactive subscriptions.
     *
     * @param subscriptions subscription list for further filtering
     * @param protocolType protocol of the subscription
     * @param destinationType type of subscription (QUEUE/TOPIC/DURABLE_TOPIC)
     * @param subscriptionIdPattern regex to match with ID of the subscriber
     * @param isExactMatchSubscriptionId exact match of subscription id or not
     * @return Set of subscriptions filtered according to search criteria
     */
    private Set<AndesSubscription> filterInactiveSubscriptionsBySubscriptionId(Set<AndesSubscription> subscriptions,
            ProtocolType protocolType, DestinationType destinationType,String subscriptionIdPattern,
            boolean isExactMatchSubscriptionId){

        Set<AndesSubscription> filteredSubscriptions = new HashSet<>();
        String messageRouter = destinationType.getAndesMessageRouter();

        if(isExactMatchSubscriptionId){
            for (AndesSubscription inactiveSubscription : subscriptions) {
                if(inactiveSubscription.getStorageQueue().getMessageRouter().getName().equals(messageRouter)
                        && inactiveSubscription.getProtocolType().equals(protocolType)
                        && inactiveSubscription.getSubscriptionId().equalsIgnoreCase(subscriptionIdPattern)){
                    filteredSubscriptions.add(inactiveSubscription);
                }
            }
        }else{
            for (AndesSubscription inactiveSubscription : subscriptions) {
                if(inactiveSubscription.getStorageQueue().getMessageRouter().getName().equals(messageRouter)
                        && inactiveSubscription.getProtocolType().equals(protocolType)
                        && StringUtils.containsIgnoreCase(inactiveSubscription.getSubscriptionId(),
                        subscriptionIdPattern)){
                    filteredSubscriptions.add(inactiveSubscription);
                }
            }
        }
        return filteredSubscriptions;
    }

    /**
     * Filter active subscriptions by subscription id.
     *
     * @param subscriptions subscription list for further filtering
     * @param subscriptionIdPattern regex to match with ID of the subscriber
     * @param isExactMatchSubscriptionId exact match of subscription id or not
     * @return Set of subscriptions filtered according to search criteria
     */
    private Set<AndesSubscription> filterActiveSubscriptionsBySubscriptionId(Set<AndesSubscription> subscriptions,String
            subscriptionIdPattern, boolean isExactMatchSubscriptionId){

        Set<AndesSubscription> filteredSubscriptions = new HashSet<>();

        if(isExactMatchSubscriptionId) {
            for (AndesSubscription subscription : subscriptions) {
                if (subscriptionIdPattern.equalsIgnoreCase(subscription.getSubscriptionId())) {
                    filteredSubscriptions.add(subscription);
                } else if (subscription.isDurable() && subscription.getStorageQueue().getMessageRouter().getName()
                        .equals(AMQPUtils.TOPIC_EXCHANGE_NAME)) {
                    if (subscriptionIdPattern.equalsIgnoreCase(((DurableTopicSubscriber) subscription).getClientID())) {
                        filteredSubscriptions.add(subscription);
                    }
                }
            }
        } else{
            for (AndesSubscription subscription : subscriptions) {
                if (StringUtils.containsIgnoreCase(subscription.getSubscriptionId(), subscriptionIdPattern)) {
                    filteredSubscriptions.add(subscription);
                } else if (subscription.isDurable() && subscription.getStorageQueue().getMessageRouter().getName()
                        .equals(AMQPUtils.TOPIC_EXCHANGE_NAME)) {
                    if (StringUtils.containsIgnoreCase(((DurableTopicSubscriber) subscription).getClientID(),
                            subscriptionIdPattern)) {
                        filteredSubscriptions.add(subscription);
                    }
                }
            }
        }
        return filteredSubscriptions;
    }




    /**
     * Remove the subscription from subscriptionRegistry.
     *
     * @param channelID protocol channel ID
     * @param nodeID    ID of the node subscription bound to
     */
    public void removeSubscriptionFromRegistry(UUID channelID, String nodeID) throws AndesException {
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription.CHANNEL_ID, channelID), equal(AndesSubscription
                        .NODE_ID, nodeID));
        for (AndesSubscription sub : subscriptionRegistry.exucuteQuery(subscriptionQuery)) {
            removeLocalSubscriptionAndNotify(sub);
        }
    }


    private void notifySubscriptionListeners(AndesSubscription subscription,
                                             ClusterNotificationListener.SubscriptionChange changeType) throws
            AndesException {

        for (SubscriptionListener subscriptionListener : subscriptionListeners) {
            subscriptionListener.handleSubscriptionsChange(subscription, changeType);
        }
    }


    public AndesSubscription getSubscriptionByProtocolChannel(UUID channelID, ProtocolType
            protocolType) {
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription.CHANNEL_ID, channelID), equal(AndesSubscription
                        .NODE_ID, localNodeId), equal(AndesSubscription
                        .PROTOCOL, protocolType));
        return subscriptionRegistry.exucuteQuery(subscriptionQuery).iterator().next();
    }

    public AndesSubscription getSubscriptionByProtocolChannel(UUID channelID) {
        Query<AndesSubscription> subscriptionQuery = equal(AndesSubscription.CHANNEL_ID, channelID);
        Iterable<AndesSubscription> subscriptions = subscriptionRegistry.exucuteQuery(subscriptionQuery);
        Iterator<AndesSubscription> subIterator = subscriptions.iterator();
        if (subIterator.hasNext()) {
            return subIterator.next();
        } else {
            log.warn("No subscription found for channel ID " + channelID);
            return null;
        }
    }

    /**
     * Get the AndesSubscription by subscription ID.
     *
     * @param subscriptionId subscription ID to query
     * @return matching subscription
     */
    public AndesSubscription getSubscriptionById(String subscriptionId) {
        Query<AndesSubscription> subscriptionQuery = equal(AndesSubscription.SUB_ID, subscriptionId);
        Iterable<AndesSubscription> subscriptions = subscriptionRegistry.exucuteQuery(subscriptionQuery);
        Iterator<AndesSubscription> subIterator = subscriptions.iterator();
        if (subIterator.hasNext()) {
            return subIterator.next();
        } else {
            log.warn("No subscription found for subscription ID " + subscriptionId);
            return null;
        }
    }

    /**
     * Get all subscriptions connected to given node.
     *
     * @param nodeId Id of the node
     * @return Iterable over matching subscriptions
     */
    public Iterable<AndesSubscription> getSubscriptionsByNode(String nodeId) {
        Query<AndesSubscription> subscriptionQuery = equal(AndesSubscription.NODE_ID, nodeId);
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }


    public Iterable<AndesSubscription> getAllLocalSubscriptions(ProtocolType protocolType) {
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription
                        .NODE_ID, localNodeId), equal(AndesSubscription
                        .PROTOCOL, protocolType));
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    /**
     * Get all subscriptions connected locally
     *
     * @return list of AndesSubscription
     */
    public Iterable<AndesSubscription> getAllLocalSubscriptions() {
        Query<AndesSubscription> subscriptionQuery = (equal(AndesSubscription
                .NODE_ID, localNodeId));
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    /**
     * Get all subscriptions in cluster bound to given queue
     *
     * @param protocolType protocol of subscriber
     * @param storageQueueName name of queue subscriber is bound to
     * @return Iterable over selected subscriptions
     */
    public Iterable<AndesSubscription> getAllSubscriptionsByQueue(ProtocolType protocolType, String
            storageQueueName) {
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription
                        .PROTOCOL, protocolType), equal(AndesSubscription
                        .STORAGE_QUEUE_NAME, storageQueueName));
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    public Iterable<AndesSubscription> getAllLocalSubscriptionsByQueue(ProtocolType protocolType, String
            storageQueueName) {
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription
                        .NODE_ID, localNodeId), equal(AndesSubscription
                        .PROTOCOL, protocolType), equal(AndesSubscription
                        .STORAGE_QUEUE_NAME, storageQueueName));
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    public Iterable<AndesSubscription> getAllLocalSubscriptionsByRoutingKey(ProtocolType protocolType, String
            routingKey) {
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription
                        .NODE_ID, localNodeId), equal(AndesSubscription
                        .PROTOCOL, protocolType), equal(AndesSubscription
                        .ROUTING_KEY, routingKey));
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    public Iterable<AndesSubscription> getAllSubscriptions(ProtocolType protocolType) {
        Query<AndesSubscription> subscriptionQuery = equal(AndesSubscription
                .PROTOCOL, protocolType);
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    public Iterable<AndesSubscription> getAllSubscriptionsByQueue(String storageQueueName) {
        Query<AndesSubscription> subscriptionQuery =
                equal(AndesSubscription.STORAGE_QUEUE_NAME, storageQueueName);
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    public Iterable<AndesSubscription> getAllSubscriptionsByRoutingKey(ProtocolType protocolType, String
            routingKey) {
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription
                        .PROTOCOL, protocolType), equal(AndesSubscription
                        .ROUTING_KEY, routingKey));
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    public Iterable<AndesSubscription> getAllSubscriptionsByMessageRouter(ProtocolType protocolType, String
            messageRouterName) {
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription
                        .PROTOCOL, protocolType), equal(AndesSubscription
                        .ROUTER_NAME, messageRouterName));
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    public Iterable<AndesSubscription> getAllLocalSubscriptionsByMessageRouter(ProtocolType protocolType,
                                                                               String messageRouterName) {
        Query<AndesSubscription> subscriptionQuery = and
                (equal(AndesSubscription
                        .PROTOCOL, protocolType), equal(AndesSubscription
                        .ROUTER_NAME, messageRouterName), equal(AndesSubscription
                        .NODE_ID, localNodeId));
        return subscriptionRegistry.exucuteQuery(subscriptionQuery);
    }

    /**
     * Close all subscriptions belonging to a particular node. This is called
     * when a node of cluster dis-joint from a cluster or get killed. This call
     * closes subscriptions from local registry, update the DB, and notify other active
     * nodes. If subscriptions are local it will forcefully disconnect subscriber from server side.
     *
     * @param nodeId ID of the node
     * @throws AndesException on an issue removing subscription and notifying
     */
    public void closeAllActiveSubscriptionsOfNode(String nodeId) throws AndesException {
        Iterable<AndesSubscription> subscriptionsOfNode = getSubscriptionsByNode(nodeId);
        for (AndesSubscription sub : subscriptionsOfNode) {
            SubscriberConnection connectionInfo = sub.getSubscriberConnection();
            UUID channelID = connectionInfo.getProtocolChannelID();
            sub.closeConnection(channelID, nodeId);
            //simulate a local subscription remove. Notify the cluster
            removeLocalSubscriptionAndNotify(sub);
        }
    }

    public void closeAllLocalSubscriptionsBoundToQueue(String storageQueueName) throws AndesException {
        StorageQueue queue = AndesContext.getInstance().
                getStorageQueueRegistry().getStorageQueue(storageQueueName);
        List<AndesSubscription> subscriptions = queue.getBoundSubscriptions();
        for (AndesSubscription subscription : subscriptions) {
            SubscriberConnection connection = subscription.getSubscriberConnection();
            UUID channelID = connection.getProtocolChannelID();
            String nodeID = connection.getConnectedNode();
            if (nodeID.equals(localNodeId)) {
                subscription.closeConnection(channelID, nodeID);
                removeLocalSubscriptionAndNotify(subscription);
            }
        }
    }

    /**
     * Get Number of subscriptions cluster-wide by queue name.
     *
     * @param queueName    name of the queue
     * @param protocolType ProtocolType (AMQP/MQTT)
     * @return number of subscriptions
     * @throws AndesException
     */
    public int numberOfSubscriptionsInCluster(String queueName, ProtocolType protocolType)
            throws AndesException {
        Iterable<AndesSubscription> subscriptions = getAllSubscriptionsByQueue(protocolType, queueName);
        List<AndesSubscription> subscriptionList = new ArrayList<>();
        for (AndesSubscription subscription : subscriptions) {
            subscriptionList.add(subscription);
        }
        return subscriptionList.size();
    }


    /**
     * Forcefully disconnect all message consumers (/ subscribers) connected to
     * this node. Typically broker node should do take such a action when a
     * network partition happens ( since coordinator in other partition will
     * also start distributing slots (hence messages) which will lead to
     * inconsistent
     * state in both partitions. Even if there is a exception trying to
     * disconnect any of the connection this method will continue with other
     * connections.
     */
    public void forcefullyDisconnectAllLocalSubscriptions() throws AndesException {

        Iterable<AndesSubscription> localSubscriptions = getAllLocalSubscriptions();

        for (AndesSubscription localSubscription : localSubscriptions) {
            localSubscription.forcefullyDisconnectConnections();
        }

    }

    /**
     * Remove all Subscriber Connections and Subscriptions (where necessary) that is bound to the
     * queue specified.
     *
     * @param storageQueueName name of the storageQueue
     * @throws SubscriptionException
     */
    public void closeAllSubscriptionsBoundToQueue(String storageQueueName) throws AndesException {
        Query<AndesSubscription> subscriptionQuery = equal(AndesSubscription
                .STORAGE_QUEUE_NAME, storageQueueName);
        Iterable<AndesSubscription> subscriptions
                = subscriptionRegistry.exucuteQuery(subscriptionQuery);
        for (AndesSubscription subscription : subscriptions) {
            SubscriberConnection connection = subscription.getSubscriberConnection();
            UUID channelID = connection.getProtocolChannelID();
            String nodeID = connection.getConnectedNode();
            subscription.closeConnection(channelID, nodeID);
            //simulate a local subscription remove. Notify the cluster
            removeLocalSubscriptionAndNotify(subscription);
        }
    }

    public void closeAllActiveLocalSubscriptions() throws AndesException {
        closeAllActiveSubscriptionsOfNode(localNodeId);
    }

    /**
     * Notify cluster members with local subscriptions information after recovering from a split brain scenario
     *
     * @throws AndesException
     */
    public void updateSubscriptionsAfterClusterMerge() throws AndesException {
        clusterNotificationAgent.notifyAnyDBChange();
    }

    /**
     * Reload subscriptions from DB storage and update subscription registry. This is a two step process
     * 1. Sync the DB with the local subscriptions.
     * 2. Sync the subscription registry with updated DB
     */
    public void reloadSubscriptionsFromStorage() throws AndesException {
        Map<String, List<String>> results = AndesContext.getInstance().getAndesContextStore()
                .getAllStoredDurableSubscriptions();

        Set<AndesSubscription> dbSubscriptions = new HashSet<>();
        Set<AndesSubscription> localSubscriptions = new HashSet<>();
        Set<AndesSubscription> copyOfLocalSubscriptions = new HashSet<>();

        //get all local subscriptions in registry
        Iterable<AndesSubscription> registeredLocalSubscriptions = getAllLocalSubscriptions();
        for (AndesSubscription registeredLocalSubscription : registeredLocalSubscriptions) {
            localSubscriptions.add(registeredLocalSubscription);
        }

        copyOfLocalSubscriptions.addAll(localSubscriptions);

        //get all subscriptions in DB
        for (Map.Entry<String, List<String>> entry : results.entrySet()) {
            for (String subscriptionAsStr : entry.getValue()) {
                AndesSubscription subscription = new AndesSubscription(subscriptionAsStr);
                dbSubscriptions.add(subscription);
            }
        }

        //if DB does not have the local subscription add it
        localSubscriptions.removeAll(dbSubscriptions);
        for (AndesSubscription subscription : localSubscriptions) {

            //If there are 2 subscriptions with the same subscription identifier but a different connected node,
            // disconnect local subscription.
            for (AndesSubscription dbSubscription : dbSubscriptions) {
                if (subscription.getStorageQueue().equals(dbSubscription.getStorageQueue())
                    && !(subscription.getSubscriberConnection().getConnectedNode()
                        .equals(dbSubscription.getSubscriberConnection().getConnectedNode()))) {
                    SubscriberConnection connection = subscription.getSubscriberConnection();
                    log.warn("Conflicting subscriptions detected with same subscription id and different connected "
                             + "nodes. Thus disconnecting local subscription=" + subscription.toString());
                    subscription.closeConnection(connection.getProtocolChannelID(), connection.getConnectedNode());
                }
            }
            log.warn("Subscriptions are not in sync. Local Subscription available "
                    + "in subscription registry of node " + localNodeId
                    + " but not in DB. Thus adding to DB subscription="
                    + subscription.toString());
            andesContextStore.storeDurableSubscription(subscription);
        }

        //if DB has additional local subscription that are not in registry, delete it
        dbSubscriptions.removeAll(copyOfLocalSubscriptions);
        for (AndesSubscription dbSubscription : dbSubscriptions) {
            String nodeIDOfDBSub = dbSubscription.getSubscriberConnection().getConnectedNode();
            if (localNodeId.equals(nodeIDOfDBSub)) {
                log.warn("Subscriptions are not in sync. Local Subscription not available "
                        + "in subscription registry of node " + localNodeId
                        + " but is in DB. Thus removing from DB subscription= "
                        + dbSubscription.toString());
                andesContextStore.removeDurableSubscription(dbSubscription);
            }
        }

        //Now as DB is synced with local subscriptions, check with all subscriptions
        dbSubscriptions = new HashSet<>();
        Map<String, List<String>> newResults = AndesContext.getInstance().getAndesContextStore()
                .getAllStoredDurableSubscriptions();
        for (Map.Entry<String, List<String>> entry : newResults.entrySet()) {
            for (String subscriptionAsStr : entry.getValue()) {
                AndesSubscription subscription = new AndesSubscription(subscriptionAsStr);
                dbSubscriptions.add(subscription);
            }
        }

        Set<AndesSubscription> allMemorySubscriptions = new HashSet<>();
        Iterator<AndesSubscription> registeredSubscriptions = subscriptionRegistry.getAllSubscriptions();

        while (registeredSubscriptions.hasNext()) {
            allMemorySubscriptions.add(registeredSubscriptions.next());
        }

        //add and register subscriptions that are in DB but not in memory
        dbSubscriptions.removeAll(allMemorySubscriptions);
        for (AndesSubscription dbSubscription : dbSubscriptions) {
            log.warn("Subscriptions are not in sync. Subscription not available "
                    + "in subscription registry but is in DB. "
                    + "Thus adding subscription to registry="
                    + dbSubscription.toString());
            subscriptionRegistry.registerSubscription(dbSubscription);
        }

        //remove the registered subscriptions that are not in DB
        dbSubscriptions = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : newResults.entrySet()) {
            for (String subscriptionAsStr : entry.getValue()) {
                AndesSubscription subscription = new AndesSubscription(subscriptionAsStr);
                dbSubscriptions.add(subscription);
            }
        }
        allMemorySubscriptions.removeAll(dbSubscriptions);
        for (AndesSubscription memorySubscription : allMemorySubscriptions) {
            log.warn("Subscriptions are not in sync. Subscription is available "
                    + "in subscription registry but not in DB. "
                    + "Thus removing subscription from registry = "
                    + memorySubscription.toString());
            subscriptionRegistry.removeSubscription(memorySubscription);
        }

    }

    /**
     * Gauge will return total number of queue subscriptions for current node.
     */
    private class QueueSubscriberGauge implements Gauge<Integer> {
        @Override
        public Integer getValue() {
            int count = 0;
            for (AndesSubscription ignored : getAllLocalSubscriptionsByMessageRouter(ProtocolType.AMQP, AMQPUtils
                    .DIRECT_EXCHANGE_NAME)) {
                count = count + 1;
            }
            return count;
        }
    }

    /**
     * Gauge will return total number of topic subscriptions current node.
     */
    private class TopicSubscriberGauge implements Gauge {
        @Override
        public Integer getValue() {
            int count = 0;
            for (AndesSubscription ignored : getAllLocalSubscriptionsByMessageRouter(ProtocolType.AMQP, AMQPUtils
                    .TOPIC_EXCHANGE_NAME)) {
                count = count + 1;
            }
            return count;
        }
    }


    /**
     * {@inheritDoc}
     * <p>
     * In a event of a network partition (or nodes being offline, stopped,
     * crashed) if minimum node count becomes less than required
     * subscription manager will disconnect all consumers connected to this
     * node.
     * </p>
     */
    @Override
    public void minimumNodeCountNotFulfilled(int currentNodeCount) {
        synchronized (this) {
            isNetworkPartitioned = true;
        }
        log.warn("Minimum node count is below required, forcefully disconnecting all subscribers");
        try {
            forcefullyDisconnectAllLocalSubscriptions();
        } catch (AndesException e) {
            log.error("error occurred while forcefully disconnecting subscriptions", e);
        }
    }

    @Override
    public void minimumNodeCountFulfilled(int currentNodeCount) {
        isNetworkPartitioned = false;
    }

    @Override
    public void clusteringOutage() {
        log.warn("Clustering outage, forcefully disconnecting all subscribers");
        try {
            forcefullyDisconnectAllLocalSubscriptions();
        } catch (AndesException e) {
            log.error("error occurred while forcefully disconnecting subscriptions", e);
        }
    }

    @Override
    public void storeNonOperational(HealthAwareStore store, Exception ex) {
        log.warn("Store became non-operational. Subscription changes will not be reflected in the store until the "
                 + "store is available.");
        storeUnavailable = true;
    }

    @Override
    public void storeOperational(HealthAwareStore store) {
        log.info("Store became operational.");
        storeUnavailable = false;
    }
}
