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
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.subscription.BasicSubscription;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class AndesSubscriptionManager {

    private static Log log = LogFactory.getLog(AndesSubscriptionManager.class);

    //Hash map that keeps the unacked messages.
    private Map<AMQChannel, Map<Long, Semaphore>> unAckedMessagelocks =
            new ConcurrentHashMap<AMQChannel, Map<Long, Semaphore>>();

    private SubscriptionStore subscriptionStore;

    private List<SubscriptionListener> subscriptionListeners = new ArrayList<SubscriptionListener>();

    private static final String TOPIC_PREFIX = "topic.";
    private static final String QUEUE_PREFIX = "queue.";

    public void init() {
        subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        //adding subscription listeners
        addSubscriptionListener(new OrphanedMessageHandler());
        addSubscriptionListener(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));
        addSubscriptionListener(new OrphanedSlotHandler());
    }


    public Map<AMQChannel, Map<Long, Semaphore>> getUnAcknowledgedMessageLocks() {
        return unAckedMessagelocks;
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
                    //close and notify
                    subscriptionStore.createDisconnectOrRemoveClusterSubscription(sub, SubscriptionListener
                            .SubscriptionChange.DELETED);
                    //this is like closing local subscribers of that node thus we need to notify
                    // to cluster
                    notifyClusterSubscriptionHasChanged(sub, SubscriptionListener.SubscriptionChange.DELETED);
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
             * subscriptions to underlying queue is gone
             * Any subscription other than last subscription is deleted when it gone.
             */
            if (allowSharedSubscribers) {
                if (subscriptionStore.getActiveLocalSubscribers(subscription.getTargetQueue(), false).size() == 1) {
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
        try {
            subscriptionStore.createDisconnectOrRemoveLocalSubscription(subscription, changeType);
        } catch (SubscriptionAlreadyExistsException ignore) {
            // never thrown for close local subscription
        }
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
            try {
                subscriptionStore.createDisconnectOrRemoveLocalSubscription(subscription, SubscriptionListener.SubscriptionChange.DELETED);
            } catch (SubscriptionAlreadyExistsException ignore) {
                // never thrown for delete
            }
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
            Set<AndesSubscription> dbSubscriptions = new HashSet<AndesSubscription>();
            Set<AndesSubscription> memorySubscriptions = new HashSet<AndesSubscription>();

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
                    subscriptionStore.createDisconnectOrRemoveClusterSubscription(subscription, SubscriptionListener
                            .SubscriptionChange.ADDED);

                    notifyClusterSubscriptionHasChanged(subscription, SubscriptionListener.SubscriptionChange.ADDED);
                }
            }

            // Check for subscriptions in memory that are not available in db and remove them
            for (AndesSubscription subscription : memorySubscriptions) {
                if (!dbSubscriptions.contains(subscription)) {
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
}
