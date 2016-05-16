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
package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.OrphanedSlotHandler;
import org.wso2.andes.kernel.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionListener;
import org.wso2.andes.subscription.BasicSubscription;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.subscription.SubscriptionEngine;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AndesSubscriptionManager implements NetworkPartitionListener {

    private static Log log = LogFactory.getLog(AndesSubscriptionManager.class);

    private SubscriptionEngine subscriptionEngine;

    /**
     * True when the minimum node count is not fulfilled, False otherwise
     */
    private volatile boolean isNetworkPartitioned;

    /**
     * listeners who are interested in local subscription changes
     */
    private List<SubscriptionListener> subscriptionListeners = new ArrayList<>();

    /**
     * this lock is to ensure that there is no concurrent cluster subscription
     * modifications happen. AndesRecoveryTask and Hazelcast notification based
     * subscription modifications can happen in parallel.
     * Fixing: https://wso2.org/jira/browse/MB-1213
     */
    private final ReadWriteLock clusterSubscriptionModifyLock = new ReentrantReadWriteLock();

    public void init() {
        isNetworkPartitioned = false;
        subscriptionEngine = AndesContext.getInstance().getSubscriptionEngine();
        
        //register subscription manager to listen to network partition events.
        if (AndesContext.getInstance().isClusteringEnabled()){
            // network partition detection works only when clustered.
            AndesContext.getInstance().getClusterAgent().addNetworkPartitionListener(10, this);
        }
        //adding subscription listeners
        addSubscriptionListener(new OrphanedMessageHandler());
        addSubscriptionListener(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));
        addSubscriptionListener(new OrphanedSlotHandler());
    }

    /**
     * Register a subscription lister
     * It will be notified when a subscription change happened
     *
     * @param listener subscription listener
     */
    public void addSubscriptionListener(SubscriptionListener listener) {
        subscriptionListeners.add(listener);
    }

    /**
     * Register a subscription for a Given Queue
     * This will handle the subscription addition task.
     * Also it will start a slot delivery worker thread to read
     * messages for the subscription
     *
     * @param localSubscription local subscription
     * @throws AndesException
     */
    public void addSubscription(LocalSubscription localSubscription) throws AndesException, SubscriptionAlreadyExistsException {
        // We don't add Subscriptions when the minimum node count is not fulfilled
        if (isNetworkPartitioned) {
            throw new AndesException("Cannot add new subscription due to network partition");
        }

        boolean durableTopicSubFoundAndUpdated = false;
        boolean hasActiveSubscriptions= false;
        List<LocalSubscription> mockSubscriptionList = new ArrayList<>();
        if(DestinationType.DURABLE_TOPIC == localSubscription.getDestinationType()) {

            Boolean allowSharedSubscribers = AndesConfigurationManager.readValue(AndesConfiguration.ALLOW_SHARED_SHARED_SUBSCRIBERS);

            /** get all subscriptions matching the subscription ID and see if there is one inactive subscription. If
            there is, we need to remove it, notify, add the new one and notify again. Reason is, subscription id of
            the new subscription is different */
            List<AndesSubscription> matchingSubscriptions = new ArrayList<>();
            Set<AndesSubscription> existingSubscriptions = subscriptionEngine
                    .getClusterSubscribersForDestination(localSubscription.getSubscribedDestination(),
                                    localSubscription.getProtocolType(), DestinationType.DURABLE_TOPIC);
            for (AndesSubscription existingSubscription : existingSubscriptions) {
                if(existingSubscription.isDurable()
                        && existingSubscription.getTargetQueue().equals(localSubscription.getTargetQueue())) {

                    if (existingSubscription.hasExternalSubscriptions()) {
                        hasActiveSubscriptions = true;
                        //An active subscription already exists
                        if (!allowSharedSubscribers) {
                            //not permitted
                            throw new SubscriptionAlreadyExistsException("A subscription already exists for Durable subscriptions on " +
                                    existingSubscription.getSubscribedDestination() + " with the queue " + existingSubscription.getTargetQueue());
                        }//else add the new subscription
                    } else{
                        matchingSubscriptions.add(existingSubscription);
                    }
                }
            }

            // If there are no matching active subscriptions
            if (!hasActiveSubscriptions) {
                LocalSubscription mockSubscription;
                for (AndesSubscription matchingSubscription : matchingSubscriptions) {
                    if (!matchingSubscription.hasExternalSubscriptions()) {
                        //delete the above subscription (only if subscription is activated from a different node -
                        // decided looking at subscription ID and the subscribed node)
                        if (!matchingSubscription.getSubscriptionID().equals(localSubscription.getSubscriptionID())
                                || !matchingSubscription.getSubscribedNode().equals(localSubscription.getSubscribedNode())) {
                            mockSubscription = convertClusterSubscriptionToMockLocalSubscription
                                    (matchingSubscription);
                            mockSubscription.close();
                            mockSubscriptionList.add(mockSubscription);
                        } else {
                            subscriptionEngine.updateLocalSubscription(localSubscription);
                            durableTopicSubFoundAndUpdated = true;
                        }

                    }
                }
            }

        }

        //store subscription in context store.
        if (!durableTopicSubFoundAndUpdated) {
            subscriptionEngine.createDisconnectOrRemoveLocalSubscription(localSubscription,
                    SubscriptionListener.SubscriptionChange.ADDED);
        }

        //start a slot delivery worker on the destination (or topicQueue) subscription refers
        SlotDeliveryWorkerManager slotDeliveryWorkerManager = SlotDeliveryWorkerManager.getInstance();
        slotDeliveryWorkerManager.startSlotDeliveryWorker(localSubscription.getStorageQueueName(),
                subscriptionEngine.getDestination(localSubscription), localSubscription.getProtocolType(), localSubscription.getDestinationType());

        //notify the local subscription change to listeners. For durable topic subscriptions this will update
        // existing inactive one if it matches
        notifyLocalSubscriptionHasChanged(localSubscription, SubscriptionListener.SubscriptionChange.ADDED);

        // Now remove the mock subscriptions. Removing should do after adding the new subscription
        // . Otherwise subscription list will be null at some point.

        if (0 != mockSubscriptionList.size()) {
            for (LocalSubscription mockSubscription : mockSubscriptionList) {
                subscriptionEngine.removeLocalSubscription(mockSubscription);
                notifyLocalSubscriptionHasChanged(mockSubscription, SubscriptionListener.SubscriptionChange.DELETED);
            }
        }
    }

    /**
     * Closing all subscription in the cluster except for durable subscriptions.
     *
     * @param nodeID id of the node
     * @throws AndesException
     */
    public void closeAllClusterSubscriptionsOfNode(String nodeID) throws AndesException {

        clusterSubscriptionModifyLock.writeLock().lock();
        try {
            Set<AndesSubscription> activeSubscriptions = subscriptionEngine.getActiveClusterSubscribersForNode(nodeID);

            if (!activeSubscriptions.isEmpty()) {
                for (AndesSubscription sub : activeSubscriptions) {
                    if (!(DestinationType.DURABLE_TOPIC == sub.getDestinationType())) {

                        LocalSubscription mockSubscription = convertClusterSubscriptionToMockLocalSubscription(sub);
                        mockSubscription.close();

                        /*
                         * Close and notify. This is like closing local subscribers of that node thus we need to notify
                         * to cluster.
                         */
                        subscriptionEngine.removeLocalSubscription(mockSubscription);
                        notifyLocalSubscriptionHasChanged(mockSubscription,
                                SubscriptionListener.SubscriptionChange.DELETED);
                    }
                }
            }
        } finally {
            clusterSubscriptionModifyLock.writeLock().unlock();
        }
    }


    /**
     * Closing all subscriptions locally except for durable subscriptions.
     *
     * @param nodeID id of the node
     * @throws AndesException
     */
    public void closeAllLocalSubscriptionsOfNode(String nodeID) throws AndesException {
        clusterSubscriptionModifyLock.writeLock().lock();
        try {
            Set<AndesSubscription> activeSubscriptions = subscriptionEngine.getClusterSubscribersForNode(nodeID);

            if (!activeSubscriptions.isEmpty()) {
                for (AndesSubscription sub : activeSubscriptions) {
                    if (!(DestinationType.DURABLE_TOPIC == sub.getDestinationType())) {

                        LocalSubscription mockSubscription = convertClusterSubscriptionToMockLocalSubscription(sub);
                        mockSubscription.close();
                        subscriptionEngine.removeSubscriptionDirectly(sub);
                        notifyClusterSubscriptionHasChanged(sub,
                                                                    SubscriptionListener.SubscriptionChange.DELETED);
                    }
                }
            }
        } finally {
            clusterSubscriptionModifyLock.writeLock().unlock();
        }
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
     *
     */
    public void forcefullyDisconnectAllLocalSubscriptionsOfNode() {

        Set<AndesSubscription> activeSubscriptions = subscriptionEngine.getActiveLocalSubscribersForNode();
       

        if (!activeSubscriptions.isEmpty()) {
            for (AndesSubscription sub : activeSubscriptions) {
                try {
                    if ( sub instanceof LocalSubscription ){
                        ((LocalSubscription) sub).forcefullyDisconnect();
                    }
                    
                } catch (AndesException disconnectError) {
                    log.error("error occurred while forcefullly disconnecting subscription: " +
                              sub.toString(), disconnectError);
                }
            }
        }

    }
    
 


    /**
     * check if any local active non durable subscription exists for a given topic consider
     * hierarchical subscription case as well
     *
     * @param boundTopicName name of the topic (bound destination)
     * @param protocolType The protocol of the destination
     * @return true if any subscription exists
     */
    public boolean checkIfActiveNonDurableLocalSubscriptionExistsForTopic(
            String boundTopicName, ProtocolType protocolType) throws AndesException {

        boolean subscriptionExists = false;
        Set<LocalSubscription> activeSubscriptions =
                subscriptionEngine.getActiveLocalSubscribers(boundTopicName, protocolType, DestinationType.TOPIC);
        for(LocalSubscription sub : activeSubscriptions) {
            if(!sub.isDurable()) {
                subscriptionExists = true;
                break;
            }
        }

        return subscriptionExists;
    }

    /**
     * close subscription
     *
     * @param subscription subscription to close
     * @throws AndesException
     */
    public void closeLocalSubscription(LocalSubscription subscription) throws AndesException {

        SubscriptionListener.SubscriptionChange changeType;
        /*
         * For durable topic subscriptions, mark this as a offline subscription.
         * When a new one comes with same subID, same topic it will become online again
         * Queue subscription representing durable topic will anyway deleted.
         * Topic subscription representing durable topic is deleted when binding is deleted
         */
        if(DestinationType.DURABLE_TOPIC == subscription.getDestinationType()) {
            /*
             * Last subscriptions is allowed mark as disconnected if last local
             * subscriptions to underlying queue is gone. Even if we look at cluster
             * subscriptions last subscriber must have the subscription ID of the closing
             * Local subscription as it is the only remaining one
             * Any subscription other than last subscription is deleted when it gone.
             */


            List<AndesSubscription> matchingSubscriptions = new ArrayList<>();
            Set<AndesSubscription> existingSubscriptions = subscriptionEngine
                    .getClusterSubscribersForDestination(subscription.getSubscribedDestination(),
                            subscription.getProtocolType(), DestinationType.TOPIC);
            for (AndesSubscription existingSubscription : existingSubscriptions) {
                if(existingSubscription.isDurable()
                        && existingSubscription.getTargetQueue().equals(subscription.getTargetQueue())) {
                    matchingSubscriptions.add(existingSubscription);
                }
            }

            changeType = SubscriptionListener.SubscriptionChange.DISCONNECTED;

        } else {
            changeType = SubscriptionListener.SubscriptionChange.DELETED;
        }

        subscription.close();
        subscriptionEngine.createDisconnectOrRemoveLocalSubscription(subscription, changeType);
        notifyLocalSubscriptionHasChanged(subscription, changeType);
    }

    /**
     * Delete all subscription entries bound for queue
     * @param boundQueueName queue name to delete subscriptions
     * @param protocolType The protocol which the queue belongs to
     * @param destinationType The destination type which the queue belongs to
     * @throws AndesException
     */
    public synchronized void deleteAllLocalSubscriptionsOfBoundQueue(String boundQueueName, ProtocolType protocolType, DestinationType destinationType) throws AndesException{
        Set<LocalSubscription> subscriptionsOfQueue = subscriptionEngine.getListOfLocalSubscriptionsBoundToQueue(
                boundQueueName, protocolType, destinationType);
        subscriptionsOfQueue.addAll(subscriptionEngine.getListOfLocalSubscriptionsBoundToQueue(
                boundQueueName, protocolType, DestinationType.DURABLE_TOPIC));
        for(LocalSubscription subscription : subscriptionsOfQueue) {
            subscription.close();
            subscriptionEngine.createDisconnectOrRemoveLocalSubscription(subscription, SubscriptionListener.SubscriptionChange.DELETED);
            notifyLocalSubscriptionHasChanged(subscription, SubscriptionListener.SubscriptionChange.DELETED);
        }
        if (log.isDebugEnabled()) {
            log.debug("Removed " + subscriptionsOfQueue.size() + " local subscriptions bound to queue: "
                      + boundQueueName);
        }
    }

    /**
     * Delete all cluster subscription entries bound for queue
     * @param boundQueueName queue name to delete subscriptions
     * @param protocolType The protocol which the queue belongs to
     * @param destinationType The destination type which the queue belongs to
     * @throws AndesException
     */
    public synchronized void deleteAllClusterSubscriptionsOfBoundQueue(String boundQueueName, ProtocolType protocolType, DestinationType destinationType) throws AndesException{
        Set<AndesSubscription> subscriptionsOfQueue = subscriptionEngine.getListOfClusterSubscriptionsBoundToQueue(
                boundQueueName, protocolType, destinationType);
        subscriptionsOfQueue.addAll(subscriptionEngine.getListOfClusterSubscriptionsBoundToQueue(
                boundQueueName, protocolType, DestinationType.DURABLE_TOPIC));
        for(AndesSubscription subscription : subscriptionsOfQueue) {
            subscriptionEngine.createDisconnectOrRemoveClusterSubscription(subscription, SubscriptionListener
                    .SubscriptionChange.DELETED);
        }
        subscriptionEngine.removeClusterSubscriptions(subscriptionsOfQueue);
        if (log.isDebugEnabled()) {
            log.debug("Removed " + subscriptionsOfQueue.size() + " cluster subscriptions bound to queue: "
                      + boundQueueName);
        }
    }

    /**
     * Update cluster subscription maps upon a change
     *
     * @param subscription subscription added, disconnected or removed
     * @param change       what the change is
     */
    public void updateClusterSubscriptionMaps(AndesSubscription subscription, SubscriptionListener.SubscriptionChange change) throws AndesException {
        subscriptionEngine.createDisconnectOrRemoveClusterSubscription(subscription, change);
    }

    /**
     * Reload subscriptions from DB storage and update cluster subscriptions in subscription store.
     */
    public void reloadSubscriptionsFromStorage() throws AndesException {

        clusterSubscriptionModifyLock.writeLock().lock();

        try {
            Map<String, List<String>> results = AndesContext.getInstance().getAndesContextStore()
                    .getAllStoredDurableSubscriptions();

            Set<AndesSubscription> dbSubscriptions = new HashSet<>();
            Set<AndesSubscription> memorySubscriptions = subscriptionEngine.getAllClusterSubscriptions();

            for (Map.Entry<String, List<String>> entry : results.entrySet()) {

                // Check for db subscriptions that are not available in memory and add them
                for (String subscriptionAsStr : entry.getValue()) {
                    BasicSubscription subscription = new BasicSubscription(subscriptionAsStr);
                    dbSubscriptions.add(subscription);

                }
            }
            dbSubscriptions = removeInvalidLocalSubscriptionsFromDB(dbSubscriptions);
            for (AndesSubscription subscription : dbSubscriptions) {
                    if (subscriptionEngine.isSubscriptionAvailable(subscription)) {
                        // Remove from list of memory subscriptions since this subscription is verified
                        memorySubscriptions.remove(subscription);

                        if (DestinationType.DURABLE_TOPIC == subscription.getDestinationType()){
                            //for durable topic subscriptions we need to update anyway since active state could have changed
                            subscriptionEngine.updateClusterSubscription(subscription);

                        }
                    } else {
                        // Subscription not available in subscription store, need to add
                        log.warn("Cluster Subscriptions are not in sync. Subscription not available in subscription "
                                + "store but exists in DB. Thus adding " + subscription);
                        subscriptionEngine.createDisconnectOrRemoveClusterSubscription(subscription, SubscriptionListener
                                .SubscriptionChange.ADDED);
                    }
                }


            // Iterate through all remaining subscriptions in memory and remove if not available in db
            for (AndesSubscription memorySubscription : memorySubscriptions) {
                if (!dbSubscriptions.contains(memorySubscription)) {
                    log.warn("Cluster Subscriptions are not in sync. Subscriptions exist in memory that are not "
                            + "available in db. Thus removing from memory " + memorySubscription);
                    subscriptionEngine.createDisconnectOrRemoveClusterSubscription(memorySubscription, SubscriptionListener
                            .SubscriptionChange.DELETED);
                }
            }
        } finally {
            clusterSubscriptionModifyLock.writeLock().unlock();
        }
    }

    /**
     * Remove the local subscriptions that are not present in the local subscriptions map but are present in the
     * database from the db. If there's a conflict between the active status of a subscription in the DB and in the
     * memory, update the DB with the one in the memory.
     *
     * @param dbSubscriptions The list of subscriptions in the db
     * @return The filtered list of DB subscriptions
     */
    private Set<AndesSubscription> removeInvalidLocalSubscriptionsFromDB(Set<AndesSubscription> dbSubscriptions)
            throws AndesException {
        String myNodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        Set<AndesSubscription> localSubscriptions = subscriptionEngine.getActiveLocalSubscribersForNode();
        Set<AndesSubscription> updatedDBSubscriptions = new HashSet<>();
        for (AndesSubscription subscription : dbSubscriptions) {
            //In the case of queue subscriptions and non-durable topic subscriptions, the subscription will be removed
            // if it does not exist in the local subscriptions map
            if (!(DestinationType.DURABLE_TOPIC == subscription.getDestinationType())) {
                boolean subscriptionFound = true;
                if (myNodeID.equals(subscription.getSubscribedNode())) {
                    if (!localSubscriptions.contains(subscription)) {
                        subscriptionEngine.removeSubscriptionDirectly(subscription);
                        log.info("Local Subscriptions are not in sync. Subscriptions exist in db that are not "
                                 + "available in memory. Thus removing from db " + subscription);
                        subscriptionFound = false;
                    }
                }
                if (subscriptionFound) {
                    updatedDBSubscriptions.add(subscription);
                }
            } else {
                //In the case of durable topic subscriptions, the subscription in the database will be modified with
                // hasExternalSubscribers = false when met with any conflict with the local subscriptions map
                if (myNodeID.equals(subscription.getSubscribedNode())) {
                    if (subscription.hasExternalSubscriptions()) {
                        if (!localSubscriptions.contains(subscription)) {
                            subscription.setHasExternalSubscriptions(false);
                            subscriptionEngine.updateLocalSubscriptionInDB(subscription);
                            log.info("Local Subscriptions are not in sync. Subscriptions exist in db that are not "
                                     + "available in memory. Thus updating the active status of " + subscription);
                        }
                    }
                }
                updatedDBSubscriptions.add(subscription);
            }
        }
        return updatedDBSubscriptions;
    }

    private void notifyLocalSubscriptionHasChanged(final LocalSubscription subscription, final SubscriptionListener
            .SubscriptionChange change) throws AndesException {
        for (final SubscriptionListener listener : subscriptionListeners) {
            listener.handleLocalSubscriptionsChanged(subscription, change);
        }
    }

    private void notifyClusterSubscriptionHasChanged(final AndesSubscription subscription, final SubscriptionListener
            .SubscriptionChange change) throws AndesException {
        for (final SubscriptionListener listener : subscriptionListeners) {
            listener.handleClusterSubscriptionsChanged(subscription, change);
        }
    }

    /**
     * Notify cluster members with local subscriptions information after recovering from a split brain scenario
     * @throws AndesException
     */
    public void updateSubscriptionsAfterClusterMerge() throws AndesException {
        HazelcastAgent.getInstance().notifyDBSyncEvent(new ClusterNotification("", "", ""));
    }

    /**
     * The durable subscriptions for the removed mb node are still marked as active when it
     * comes to fail-over. These subscriptions needs to be marked as disconnected.
     *
     * @param isCoordinator Whether the current node is the coordinator.
     * @param nodeID        The removed node ID.
     * @throws AndesException
     */
    public void deactivateClusterDurableSubscriptionsForNodeID(boolean isCoordinator, String nodeID)
                                                                            throws AndesException {
        clusterSubscriptionModifyLock.writeLock().lock();
        try {
            subscriptionEngine.deactivateClusterDurableSubscriptionsForNodeID(isCoordinator, nodeID);
        } finally {
            clusterSubscriptionModifyLock.writeLock().unlock();
        }
    }

    /**
     * This will set the status of all the active subscribers to inactive. Required when all the nodes of a cluster
     * go down with active subscribers. If the subscriptions were not set to inactive, the nodes that are coming
     * back will read the statuses of the subscribers as active and therefore, will not let the subscribers reconnect.
     */
    public void deactivateAllActiveSubscriptions() throws AndesException {

        clusterSubscriptionModifyLock.writeLock().lock();
        try {
            subscriptionEngine.deactivateAllActiveSubscriptions();
            log.info("Deactivated all active durable subscriptions");
        } finally {
            clusterSubscriptionModifyLock.writeLock().unlock();
        }
    }

    /**
     * Convert the given cluster subscription to a local subscription. This subscription cannot
     * be used to send messages. It has no channel associated with it. It can only be used to mock the local
     * subscription object.
     * @param clusterSubscription cluster subscription to convert
     * @return mock local subscription
     */
    private LocalSubscription convertClusterSubscriptionToMockLocalSubscription(AndesSubscription clusterSubscription) {

        String subscriptionID = clusterSubscription.getSubscriptionID();
        String destination = clusterSubscription.getSubscribedDestination();
        boolean isExclusive = clusterSubscription.isExclusive();
        boolean isDurable = clusterSubscription.isDurable();
        String subscribedNode = clusterSubscription.getSubscribedNode();
        long subscribedTime = clusterSubscription.getSubscribeTime();
        String targetQueue = clusterSubscription.getTargetQueue();
        String targetQueueOwner = clusterSubscription.getTargetQueueOwner();
        String targetQueueBoundExchange = clusterSubscription.getTargetQueueBoundExchangeName();
        String targetQueueBoundExchangeType = clusterSubscription.getTargetQueueBoundExchangeType();
        Short isTargetQueueAutoDeletable = clusterSubscription.ifTargetQueueBoundExchangeAutoDeletable();
        boolean hasExternalSubscriptions = clusterSubscription.hasExternalSubscriptions();
        DestinationType destinationType = clusterSubscription.getDestinationType();

        LocalSubscription localSubscription = new LocalSubscription(null,subscriptionID, destination, isExclusive, isDurable,
                subscribedNode, subscribedTime, targetQueue, targetQueueOwner, targetQueueBoundExchange,
                targetQueueBoundExchangeType, isTargetQueueAutoDeletable, hasExternalSubscriptions, destinationType);

        localSubscription.setProtocolType(clusterSubscription.getProtocolType());

        return localSubscription;

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
        isNetworkPartitioned = true;
        log.warn("Minimum node count is below required, forcefully disconnecting all subscribers");
        forcefullyDisconnectAllLocalSubscriptionsOfNode();
    }

    /**
     * {@inheritDoc}
     * No action required.
     */
    @Override
    public void minimumNodeCountFulfilled(int currentNodeCount) {
        isNetworkPartitioned = false;
        // No action required.
    }
}
