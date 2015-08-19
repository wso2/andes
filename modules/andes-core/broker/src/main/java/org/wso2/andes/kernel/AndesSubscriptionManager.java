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
import org.wso2.andes.subscription.AMQPLocalSubscription;
import org.wso2.andes.subscription.BasicSubscription;
import org.wso2.andes.subscription.SubscriptionStore;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AndesSubscriptionManager {

    private static Log log = LogFactory.getLog(AndesSubscriptionManager.class);

    private SubscriptionStore subscriptionStore;

    private List<SubscriptionListener> subscriptionListeners = new ArrayList<>();

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
                    matchingSubscriptions.add(existingSubscription);
                }
            }

            //there is one inactive subscription (to represent inactive subscription). We need delete it
            if(matchingSubscriptions.size() == 1) {
                AndesSubscription matchingSubscription = matchingSubscriptions.get(0);
                if(!matchingSubscription.hasExternalSubscriptions()) {
                    //delete the above subscription
                    LocalSubscription mockSubscription = convertClusterSubscriptionToMockLocalSubscription
                            (matchingSubscription);
                    mockSubscription.close();
                    LocalSubscription removedSubscription = subscriptionStore.removeLocalSubscription
                            (mockSubscription);
                    /** removed subscription is returned. If removed subscription is null this is not a actual local
                    subscription. We need to directly remove it. */
                    if(null == removedSubscription) {
                        subscriptionStore.removeSubscriptionDirectly(mockSubscription);
                    }
                    notifyLocalSubscriptionHasChanged(mockSubscription, SubscriptionListener.SubscriptionChange.DELETED);


                } else {
                    //An active subscription already exists
                    if (!allowSharedSubscribers) {
                        //not permitted
                        throw new SubscriptionAlreadyExistsException("A subscription already exists for Durable subscriptions on " +
                                matchingSubscription.getSubscribedDestination() + " with the queue " + matchingSubscription.getTargetQueue());
                    } else {
                        //add the new subscription
                    }

                }
            } else if(matchingSubscriptions.size() > 1){

                //there are active subscriptions. just create a new one

            } else {

                //this is the very first subscription for the cluster. Just create one

            }
        }

        //store subscription in context store
        subscriptionStore.createDisconnectOrRemoveLocalSubscription(localSubscription, SubscriptionListener.SubscriptionChange.ADDED);

        //start a slot delivery worker on the destination (or topicQueue) subscription refers
        SlotDeliveryWorkerManager slotDeliveryWorkerManager = SlotDeliveryWorkerManager.getInstance();
        slotDeliveryWorkerManager.startSlotDeliveryWorker(localSubscription.getStorageQueueName(), subscriptionStore.getDestination(localSubscription));

        //notify the local subscription change to listeners
        notifyLocalSubscriptionHasChanged(localSubscription, SubscriptionListener.SubscriptionChange.ADDED);

    }

    /**
     * Closing all subscription in the cluster except for durable subscriptions.
     *
     * @param nodeID id of the node
     * @throws AndesException
     */
    public void closeAllClusterSubscriptionsOfNode(String nodeID) throws AndesException {

        Set<AndesSubscription> activeSubscriptions = subscriptionStore.getActiveClusterSubscribersForNode(nodeID, true);
        activeSubscriptions.addAll(subscriptionStore.getActiveClusterSubscribersForNode(nodeID, false));

        if (!activeSubscriptions.isEmpty()) {
            for (AndesSubscription sub : activeSubscriptions) {
                if (!(sub.isDurable() && sub.isBoundToTopic())) {

                    LocalSubscription mockSubscription = convertClusterSubscriptionToMockLocalSubscription(sub);
                    mockSubscription.close();

                    /**
                     * Close and notify. This is like closing local subscribers of that node thus we need to notify
                     * to cluster.
                     */
                    LocalSubscription removedSubscription = subscriptionStore.removeLocalSubscription(mockSubscription);

                    if(null == removedSubscription) {
                        subscriptionStore.removeSubscriptionDirectly(sub);
                    }
                    notifyLocalSubscriptionHasChanged(mockSubscription, SubscriptionListener.SubscriptionChange.DELETED);
                }
            }
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
        /**
         * For durable topic subscriptions, mark this as a offline subscription.
         * When a new one comes with same subID, same topic it will become online again
         * Queue subscription representing durable topic will anyway deleted.
         * Topic subscription representing durable topic is deleted when binding is deleted
         */
        if(subscription.isBoundToTopic() && subscription.isDurable()) {
            Boolean allowSharedSubscribers =  AndesConfigurationManager.readValue
                    (AndesConfiguration.ALLOW_SHARED_SHARED_SUBSCRIBERS);
            /**
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

            if (allowSharedSubscribers) {
                if (matchingSubscriptions.size() == 1) {
                    changeType = SubscriptionListener.SubscriptionChange.DISCONNECTED;
                } else {
                    changeType = SubscriptionListener.SubscriptionChange.DELETED;
                }
            } else {
                changeType = SubscriptionListener.SubscriptionChange.DISCONNECTED;
            }
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

        Map<String, List<String>> results = AndesContext.getInstance().getAndesContextStore()
                .getAllStoredDurableSubscriptions();

        for (Map.Entry<String, List<String>> entry : results.entrySet()) {
            String destination = entry.getKey();
            Set<AndesSubscription> dbSubscriptions = new HashSet<>();
            Set<AndesSubscription> memorySubscriptions = new HashSet<>();

            if (destination.startsWith(QUEUE_PREFIX)) {
                String destinationQueueName = destination.replace(QUEUE_PREFIX, "");
                memorySubscriptions.addAll(subscriptionStore.getClusterSubscribersForDestination
                        (destinationQueueName, false, AndesSubscription.SubscriptionType.AMQP));
            } else {
                String topicName = destination.replace(TOPIC_PREFIX, "");
                // Get all the subscriptions for the destination available in memory
                memorySubscriptions.addAll(subscriptionStore.getClusterSubscribersForDestination(topicName, true,
                        AndesSubscription.SubscriptionType.AMQP));
                memorySubscriptions.addAll(subscriptionStore.getClusterSubscribersForDestination(topicName, true,
                        AndesSubscription.SubscriptionType.MQTT));
            }

            // Check for db subscriptions that are not available in memory and add them
            for (String subscriptionAsStr : entry.getValue()) {
                BasicSubscription subscription = new BasicSubscription(subscriptionAsStr);
                dbSubscriptions.add(subscription);

                boolean subscriptionAvailable = subscriptionStore.isSubscriptionAvailable(subscription);

                if (!subscriptionAvailable) {
                    // Subscription not available in subscription store, need to add
                    log.warn("Cluster Subscriptions are not in sync. Subscription not available in subscription store" +
                            " but exists in DB. Thus adding " + subscription);
                    subscriptionStore.createDisconnectOrRemoveClusterSubscription(subscription, SubscriptionListener
                            .SubscriptionChange.ADDED);

                    notifyClusterSubscriptionHasChanged(subscription, SubscriptionListener.SubscriptionChange.ADDED);
                }
            }

            // Check for subscriptions in memory that are not available in db and remove them
            for (AndesSubscription subscription : memorySubscriptions) {
                if (!dbSubscriptions.contains(subscription)) {
                    log.warn("Cluster Subscriptions are not in sync. Subscriptions exist in memory that are not " +
                            "available in db. Thus adding to DB " + subscription);
                    subscriptionStore.createDisconnectOrRemoveClusterSubscription(subscription, SubscriptionListener
                            .SubscriptionChange.DELETED);
                    notifyClusterSubscriptionHasChanged(subscription, SubscriptionListener.SubscriptionChange.DELETED);
                }
            }
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
        subscriptionStore.deactivateClusterDurableSubscriptionsForNodeID(isCoordinator, nodeID);
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

        return new AMQPLocalSubscription(null, null, subscriptionID, destination,
                isBoundToTopic, isExclusive, isDurable, subscribedNode, subscribedTime, targetQueue,
                targetQueueOwner, targetQueueBoundExchange, targetQueueBoundExchangeType,isTargetQueueAutoDeletable,
                hasExternalSubscriptions);

    }
}
