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
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.subscription.BasicSubscription;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AndesSubscriptionManager {

    private static Log log = LogFactory.getLog(AndesSubscriptionManager.class);

    private SubscriptionStore subscriptionStore;

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


    private static final String TOPIC_PREFIX = "topic.";
    private static final String QUEUE_PREFIX = "queue.";

    public void init() {
        subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
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
        boolean durableTopicSubFoundAndUpdated = false;
        boolean hasActiveSubscriptions= false;
        List<LocalSubscription> mockSubscriptionList = new ArrayList<LocalSubscription>();
        if(localSubscription.isDurable() && localSubscription.isBoundToTopic()) {

            Boolean allowSharedSubscribers = AndesConfigurationManager.readValue(AndesConfiguration.ALLOW_SHARED_SHARED_SUBSCRIBERS);

            /** get all subscriptions matching the subscription ID and see if there is one inactive subscription. If
            there is, we need to remove it, notify, add the new one and notify again. Reason is, subscription id of
            the new subscription is different */
            List<AndesSubscription> matchingSubscriptions = new ArrayList<>();
            Set<AndesSubscription> existingSubscriptions = subscriptionStore
                    .getClusterSubscribersForDestination(localSubscription.getSubscribedDestination(),
                                    true, AndesSubscription.SubscriptionType.AMQP);
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
                LocalSubscription mockSubscription = null;
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
                            subscriptionStore.updateLocalSubscriptionSubscription(localSubscription);
                            durableTopicSubFoundAndUpdated = true;
                        }

                    }
                }
            }

        }

        //store subscription in context store.
        if (!durableTopicSubFoundAndUpdated) {
            subscriptionStore.createDisconnectOrRemoveLocalSubscription(localSubscription,
                    SubscriptionListener.SubscriptionChange.ADDED);
        }

        //start a slot delivery worker on the destination (or topicQueue) subscription refers
        SlotDeliveryWorkerManager slotDeliveryWorkerManager = SlotDeliveryWorkerManager.getInstance();
        slotDeliveryWorkerManager.startSlotDeliveryWorker(localSubscription.getStorageQueueName(),
                subscriptionStore.getDestination(localSubscription));

        //notify the local subscription change to listeners. For durable topic subscriptions this will update
        // existing inactive one if it matches
        notifyLocalSubscriptionHasChanged(localSubscription, SubscriptionListener.SubscriptionChange.ADDED);

        // Now remove the mock subscriptions. Removing should do after adding the new subscription
        // . Otherwise subscription list will be null at some point.

        if (0 != mockSubscriptionList.size()) {
            for (LocalSubscription mockSubscription : mockSubscriptionList) {
                LocalSubscription removedSubscription = subscriptionStore.removeLocalSubscription
                        (mockSubscription);
                /** removed subscription is returned. If removed subscription is null this is not a actual local
                 subscription. We need to directly remove it. */
                if (null == removedSubscription) {
                    subscriptionStore.removeSubscriptionDirectly(mockSubscription);
                }
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
            Set<AndesSubscription> activeSubscriptions =
                    subscriptionStore.getActiveClusterSubscribersForNode(nodeID, true);
            activeSubscriptions.addAll(subscriptionStore.getActiveClusterSubscribersForNode(nodeID, false));

            if (!activeSubscriptions.isEmpty()) {
                for (AndesSubscription sub : activeSubscriptions) {
                    if (!(sub.isDurable() && sub.isBoundToTopic())) {

                        LocalSubscription mockSubscription = convertClusterSubscriptionToMockLocalSubscription(sub);
                        mockSubscription.close();

                        /*
                         * Close and notify. This is like closing local subscribers of that node thus we need to notify
                         * to cluster.
                         */
                        LocalSubscription removedSubscription =
                                subscriptionStore.removeLocalSubscription(mockSubscription);

                        if(null == removedSubscription) {
                            subscriptionStore.removeSubscriptionDirectly(sub);
                        }
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
            Set<AndesSubscription> activeSubscriptions = subscriptionStore.getActiveClusterSubscribersForNode(nodeID, true);
            activeSubscriptions.addAll(subscriptionStore.getActiveClusterSubscribersForNode(nodeID, false));

            if (!activeSubscriptions.isEmpty()) {
                for (AndesSubscription sub : activeSubscriptions) {
                    if (!(sub.isDurable() && sub.isBoundToTopic())) {

                        LocalSubscription mockSubscription = convertClusterSubscriptionToMockLocalSubscription(sub);
                        mockSubscription.close();
                        subscriptionStore.removeSubscriptionDirectly(sub);
                        notifyClusterSubscriptionHasChanged(mockSubscription, SubscriptionListener.SubscriptionChange
                                .DELETED);
                    }
                }
            }
        } finally {
            clusterSubscriptionModifyLock.writeLock().unlock();
        }
    }


    /**
     * Close all active local subscribers in the local node
     *
     * @throws AndesException
     */
    public void closeAllLocalSubscriptionsOfNode() throws AndesException {

        Set<LocalSubscription> activeSubscriptions = subscriptionStore.getActiveLocalSubscribers(true);
        activeSubscriptions.addAll(subscriptionStore.getActiveLocalSubscribers(false));

        if (!activeSubscriptions.isEmpty()) {
            for (LocalSubscription sub : activeSubscriptions) {
                closeLocalSubscription(sub);
            }
        }

    }

    /**
     * check if any local active non durable subscription exists for a given topic consider
     * hierarchical subscription case as well
     *
     * @param boundTopicName
     *         name of the topic (bound destination)
     * @return true if any subscription exists
     */
    public boolean checkIfActiveNonDurableLocalSubscriptionExistsForTopic(String boundTopicName)
                                                                             throws AndesException {
        boolean subscriptionExists = false;
        Set<LocalSubscription> activeSubscriptions = subscriptionStore.getActiveLocalSubscribers(boundTopicName, true);
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
        if(subscription.isBoundToTopic() && subscription.isDurable()) {
            Boolean allowSharedSubscribers =  AndesConfigurationManager.readValue
                    (AndesConfiguration.ALLOW_SHARED_SHARED_SUBSCRIBERS);
            /*
             * Last subscriptions is allowed mark as disconnected if last local
             * subscriptions to underlying queue is gone. Even if we look at cluster
             * subscriptions last subscriber must have the subscription ID of the closing
             * Local subscription as it is the only remaining one
             * Any subscription other than last subscription is deleted when it gone.
             */


            List<AndesSubscription> matchingSubscriptions = new ArrayList<>();
            Set<AndesSubscription> existingSubscriptions = subscriptionStore
                    .getClusterSubscribersForDestination(subscription.getSubscribedDestination(),
                            true, AndesSubscription.SubscriptionType.AMQP);
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
        subscriptionStore.createDisconnectOrRemoveLocalSubscription(subscription, changeType);
        notifyLocalSubscriptionHasChanged(subscription, changeType);
    }

    /**
     * Delete all subscription entries bound for queue
     * @param boundQueueName queue name to delete subscriptions
     * @throws AndesException
     */
    public synchronized void deleteAllLocalSubscriptionsOfBoundQueue(String boundQueueName) throws AndesException{
        Set<LocalSubscription> subscriptionsOfQueue = subscriptionStore.getListOfLocalSubscriptionsBoundToQueue(
                boundQueueName);
        for(LocalSubscription subscription : subscriptionsOfQueue) {
            subscription.close();
            subscriptionStore.createDisconnectOrRemoveLocalSubscription(subscription, SubscriptionListener.SubscriptionChange.DELETED);
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
     * @throws AndesException
     */
    public synchronized void deleteAllClusterSubscriptionsOfBoundQueue(String boundQueueName) throws AndesException{
        Set<AndesSubscription> subscriptionsOfQueue = subscriptionStore.getListOfClusterSubscriptionsBoundToQueue(
                boundQueueName);
        for(AndesSubscription subscription : subscriptionsOfQueue) {
            subscriptionStore.createDisconnectOrRemoveClusterSubscription(subscription, SubscriptionListener
                    .SubscriptionChange.DELETED);
        }
        subscriptionStore.removeClusterSubscriptions(subscriptionsOfQueue);
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
        subscriptionStore.createDisconnectOrRemoveClusterSubscription(subscription, change);
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

            for (Map.Entry<String, List<String>> entry : results.entrySet()) {

                // Check for db subscriptions that are not available in memory and add them
                for (String subscriptionAsStr : entry.getValue()) {
                    BasicSubscription subscription = new BasicSubscription(subscriptionAsStr);
                    dbSubscriptions.add(subscription);
                    boolean subscriptionAvailable = subscriptionStore.isSubscriptionAvailable(subscription);

                    if (!subscriptionAvailable) {
                        // Subscription not available in subscription store, need to add
                        log.warn("Cluster Subscriptions are not in sync. Subscription not available in subscription "
                                + "store but exists in DB. Thus adding " + subscription);
                        subscriptionStore.createDisconnectOrRemoveClusterSubscription(subscription, SubscriptionListener
                                .SubscriptionChange.ADDED);
                    } else if (subscription.isDurable() && subscription.isBoundToTopic()){
                        //for durable topic subscriptions we need to update anyway since active state could have changed
                        subscriptionStore.updateClusterSubscription(subscription);

                    }
                }
            }

            // Check for subscriptions in memory that are not available in db and remove them
            List<AndesSubscription> subscriptionsToRemove = new ArrayList<>();
            //queues
            Set<String> queues = subscriptionStore.getAllDestinationsOfSubscriptions(false);
            for (String queue : queues) {
                Set<AndesSubscription> memorySubscriptions = subscriptionStore.getAllClusterSubscriptionsWithoutWildcards
                        (queue, false);
                //check if each memory subscription is in DB. If not remove from memory
                if(null != memorySubscriptions) {
                    for (AndesSubscription memorySubscription : memorySubscriptions) {
                        if (!dbSubscriptions.contains(memorySubscription)) {
                            subscriptionsToRemove.add(memorySubscription);
                        }
                    }
                }
            }
            //topics
            Set<String> topics = subscriptionStore.getAllDestinationsOfSubscriptions(true);
            for (String topic : topics) {
                Set<AndesSubscription> memorySubscriptions = subscriptionStore.getAllClusterSubscriptionsWithoutWildcards
                        (topic, true);
                //check if each memory subscription is in DB. If not remove from memory
                if(null != memorySubscriptions) {
                    for (AndesSubscription memorySubscription : memorySubscriptions) {
                        if (!dbSubscriptions.contains(memorySubscription)) {
                            subscriptionsToRemove.add(memorySubscription);
                        }
                    }
                }
            }

            //perform delete of marked subscriptions
            for (AndesSubscription andesSubscription : subscriptionsToRemove) {
                log.warn("Cluster Subscriptions are not in sync. Subscriptions exist in memory that are not "
                        + "available in db. Thus removing from memory " + andesSubscription);
                subscriptionStore.createDisconnectOrRemoveClusterSubscription(andesSubscription, SubscriptionListener
                        .SubscriptionChange.DELETED);
            }
        } finally {
            clusterSubscriptionModifyLock.writeLock().unlock();
        }
    }

    private void notifyLocalSubscriptionHasChanged(final LocalSubscription subscription, final SubscriptionListener.SubscriptionChange change) throws AndesException {
        for (final SubscriptionListener listener : subscriptionListeners) {
            listener.handleLocalSubscriptionsChanged(subscription, change);
        }
    }

    private void notifyClusterSubscriptionHasChanged(final AndesSubscription subscription, final SubscriptionListener.SubscriptionChange change) throws AndesException {
        for (final SubscriptionListener listener : subscriptionListeners) {
            listener.handleClusterSubscriptionsChanged(subscription, change);
        }
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
            subscriptionStore.deactivateClusterDurableSubscriptionsForNodeID(isCoordinator, nodeID);
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
            subscriptionStore.deactivateAllActiveSubscriptions();
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
        boolean isBoundToTopic = clusterSubscription.isBoundToTopic();
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

        return new LocalSubscription(null,subscriptionID, destination, isBoundToTopic, isExclusive, isDurable,
                subscribedNode, subscribedTime, targetQueue, targetQueueOwner, targetQueueBoundExchange,
                targetQueueBoundExchangeType, isTargetQueueAutoDeletable, hasExternalSubscriptions);

    }
}
