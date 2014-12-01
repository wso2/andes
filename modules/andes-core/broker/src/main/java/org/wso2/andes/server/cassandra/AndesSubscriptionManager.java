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
package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.slot.OrphanedSlotHandler;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.subscription.BasicSubscription;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.*;
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
    private static final String QUEUE_PREFIX = "destination.";


    public void init() {
        subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        //adding subscription listeners
        addSubscriptionListener(new OrphanedMessageHandler());
        addSubscriptionListener(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));
        if (AndesContext.getInstance().isClusteringEnabled()) {
            addSubscriptionListener(new OrphanedSlotHandler());
        }
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
    public void addSubscription(LocalSubscription localSubscription) throws AndesException {

        //store subscription in context store
        subscriptionStore.createDisconnectOrRemoveLocalSubscription(localSubscription, SubscriptionListener.SubscriptionChange.ADDED);

        //start a slot delivery worker on the destination (or topicQueue) subscription refers
        SlotDeliveryWorkerManager slotDeliveryWorkerManager = SlotDeliveryWorkerManager.getInstance();
        slotDeliveryWorkerManager.startSlotDeliveryWorker(localSubscription.getStorageQueueName(), localSubscription.getSubscribedDestination());

        //notify the local subscription change to listeners
        notifyLocalSubscriptionHasChanged(localSubscription, SubscriptionListener.SubscriptionChange.ADDED);

    }

    /**
     * Using cluster subscriptions find the local subscriptions of an node
     * and close all of them
     *
     * @param nodeID id of the node
     * @throws AndesException
     */
    public void closeAllClusterSubscriptionsOfNode(String nodeID) throws AndesException {

        List<AndesSubscription> activeSubscriptions = subscriptionStore.getActiveClusterSubscribersForNode(nodeID, true);
        activeSubscriptions.addAll(subscriptionStore.getActiveClusterSubscribersForNode(nodeID, false));

        if (!activeSubscriptions.isEmpty()) {
            for (AndesSubscription sub : activeSubscriptions) {
                //close and notify
                subscriptionStore.createDisconnectOrRemoveClusterSubscription(sub, SubscriptionListener.SubscriptionChange.DELETED);
                //this is like closing local subscribers of that node thus we need to notify to cluster
                notifyLocalSubscriptionHasChanged((LocalSubscription) sub, SubscriptionListener.SubscriptionChange.DELETED);
            }
        }

    }


    /**
     * Close all active local subscribers in the local node
     *
     * @throws AndesException
     */
    public void closeAllLocalSubscriptionsOfNode() throws AndesException {

        List<LocalSubscription> activeSubscriptions = subscriptionStore.getActiveLocalSubscribers(true);
        activeSubscriptions.addAll(subscriptionStore.getActiveLocalSubscribers(false));

        if (!activeSubscriptions.isEmpty()) {
            for (LocalSubscription sub : activeSubscriptions) {
                //close and notify
                subscriptionStore.createDisconnectOrRemoveLocalSubscription(sub, SubscriptionListener.SubscriptionChange.DELETED);
                notifyLocalSubscriptionHasChanged(sub, SubscriptionListener.SubscriptionChange.DELETED);
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
        List<LocalSubscription> activeSubscriptions = (List<LocalSubscription>) subscriptionStore.
                                                                           getActiveLocalSubscribers(
                                                                           boundTopicName,
                                                                           true);
        for(LocalSubscription sub : activeSubscriptions) {
            if(!sub.isDurable()) {
                subscriptionExists = true;
                break;
            }
        }

        return subscriptionExists;
    }

    /**
     * check if any local active non durable subscription exists
     *
     * @param isTopic if topic subscriptions are queried
     * @return if any subscription exists
     */
    public boolean checkIfActiveLocalNonDurableSubscriptionsExists(boolean isTopic) {
        List<LocalSubscription> activeSubscriptions = subscriptionStore.getActiveNonDurableLocalSubscribers(isTopic);
        return !activeSubscriptions.isEmpty();
    }

    /**
     * check if any cluster active subscription exists
     *
     * @param isTopic if topic subscriptions are queried
     * @return if any subscription exists
     */
    public boolean checkIfActiveClusterSubscriptionsExists(boolean isTopic) {
        List<AndesSubscription> activeSubscriptions = subscriptionStore.getActiveClusterSubscribersForNode(ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID(), isTopic);
        return !activeSubscriptions.isEmpty();
    }

    /**
     * close subscription
     *
     * @param subscription subscription to close
     * @throws AndesException
     */
    public void closeLocalSubscription(LocalSubscription subscription) throws AndesException {
        SubscriptionListener.SubscriptionChange chageType;
        /**
         * For durable topic subscriptions, mark this as a offline subscription.
         * When a new one comes with same subID, same topic it will become online again
         * Queue subscription representing durable topic will anyway deleted.
         * Topic subscription representing durable topic is deleted when binding is deleted
         */
        if(subscription.isBoundToTopic() && subscription.isDurable()) {
            chageType = SubscriptionListener.SubscriptionChange.DISCONNECTED;
        } else {
            chageType = SubscriptionListener.SubscriptionChange.DELETED;
        }
        subscriptionStore.createDisconnectOrRemoveLocalSubscription(subscription, chageType);
        notifyLocalSubscriptionHasChanged(subscription, chageType);
    }

    /**
     * Delete all subscription entries bound for queue
     * @param boundQueueName queue name to delete subscriptions
     * @throws AndesException
     */
    public void deleteSubscriptionsOfBoundQueue(String boundQueueName) throws AndesException{
        List<LocalSubscription> subscriptionsOfQueue = subscriptionStore.getListOfSubscriptionsBoundToQueue(boundQueueName);
        for(LocalSubscription subscription : subscriptionsOfQueue) {
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
     * Reload subscriptions from DB storage and update cluster subscription lists
     */
    public void reloadSubscriptionsFromStorage() throws AndesException {
        //this part will evaluate what is in DB with in-memory lists
        Map<String, List<String>> results = AndesContext.getInstance().getAndesContextStore().getAllStoredDurableSubscriptions();
        for (Map.Entry<String, List<String>> entry : results.entrySet()) {
            String destination = entry.getKey();
            List<AndesSubscription> newSubscriptionList = new ArrayList<AndesSubscription>();
            for (String subscriptionAsStr : entry.getValue()) {
                BasicSubscription subscription = new BasicSubscription(subscriptionAsStr);
                newSubscriptionList.add(subscription);
            }

            List<AndesSubscription> oldSubscriptionList;

            //existing destination subscriptions list
            if (destination.startsWith(QUEUE_PREFIX)) {
                String destinationQueueName = destination.replace(QUEUE_PREFIX, "");
                oldSubscriptionList = subscriptionStore.replaceClusterSubscriptionListOfDestination
                        (destinationQueueName, new ArrayList<AndesSubscription>(newSubscriptionList), false);
            }
            //existing topic subscriptions list
            else {
                String topicName = destination.replace(TOPIC_PREFIX, "");
                oldSubscriptionList = subscriptionStore.replaceClusterSubscriptionListOfDestination
                        (topicName, new ArrayList<AndesSubscription>(newSubscriptionList), true);
            }

            if (oldSubscriptionList == null) {
                oldSubscriptionList = Collections.emptyList();
            }

            //TODO may be there is a better way to do the subscription Diff
            if (subscriptionListeners.size() > 0) {
                List<AndesSubscription> duplicatedNewSubscriptionList = new ArrayList<AndesSubscription>(newSubscriptionList);
                /**
                 * for all subscriptions which are in store but ont in-memory simulate the incoming 'create'
                 * cluster notification for the subscription
                 */
                newSubscriptionList.removeAll(oldSubscriptionList);
                for (AndesSubscription subscription : newSubscriptionList) {
                    log.warn("Recovering node. Adding subscription " + subscription.toString());
                    notifyClusterSubscriptionHasChanged(subscription, SubscriptionListener.SubscriptionChange.ADDED);
                }

                /**
                 * for all subscriptions which are in-memory but not in store simulate the incoming 'delete'
                 * cluster notification for the subscription
                 */
                oldSubscriptionList.removeAll(duplicatedNewSubscriptionList);
                for (AndesSubscription subscription : oldSubscriptionList) {
                    log.warn("Recovering node. Removing subscription " + subscription.toString());
                    notifyClusterSubscriptionHasChanged(subscription, SubscriptionListener.SubscriptionChange.DELETED);
                }
            }
        }

        //this part will evaluate destinations that are in in-memory subscription lists but not in DB
        List<String> queues = subscriptionStore.getAllDestinationsOfSubscriptions(false);
        List<String> topics = subscriptionStore.getAllDestinationsOfSubscriptions(true);
        List<String> queuesInDB = new ArrayList<String>();
        List<String> topicsInDB = new ArrayList<String>();

        for (String destination : results.keySet()) {
            if (destination.startsWith(QUEUE_PREFIX)) {
                queuesInDB.add(destination.replace(QUEUE_PREFIX, ""));
            } else if (destination.startsWith(TOPIC_PREFIX)) {
                topicsInDB.add(destination.replace(TOPIC_PREFIX, ""));
            }
        }
        queues.removeAll(queuesInDB);
        topics.removeAll(topicsInDB);

        for (String queue : queues) {
            List<String> subscriptionsFromStore = results.get(QUEUE_PREFIX + queue);
            List<AndesSubscription> newSubscriptionList = new ArrayList<AndesSubscription>();
            if (subscriptionsFromStore != null) {
                for (String subscriptionAsStr : subscriptionsFromStore) {
                    BasicSubscription subscription = new BasicSubscription(subscriptionAsStr);
                    newSubscriptionList.add(subscription);
                }
            }
            List<AndesSubscription> oldSubscriptionList;
            oldSubscriptionList = subscriptionStore.replaceClusterSubscriptionListOfDestination
                    (queue, new ArrayList<AndesSubscription>(newSubscriptionList), false);
            if (oldSubscriptionList == null) {
                oldSubscriptionList = Collections.emptyList();
            }
            if (subscriptionListeners.size() > 0) {
                List<AndesSubscription> duplicatedNewSubscriptionList = new ArrayList<AndesSubscription>(newSubscriptionList);
                /**
                 * for all subscriptions which are in store but ont in-memory simulate the incoming 'create'
                 * cluster notification for the subscription
                 */
                newSubscriptionList.removeAll(oldSubscriptionList);
                for (AndesSubscription subscription : newSubscriptionList) {
                    log.warn("Recovering node. Adding subscription " + subscription.toString());
                    notifyClusterSubscriptionHasChanged(subscription, SubscriptionListener.SubscriptionChange.ADDED);
                }

                /**
                 * for all subscriptions which are in-memory but not in store simulate the incoming 'delete'
                 * cluster notification for the subscription
                 */
                oldSubscriptionList.removeAll(duplicatedNewSubscriptionList);
                for (AndesSubscription subscription : oldSubscriptionList) {
                    log.warn("Recovering node. Removing subscription " + subscription.toString());
                    notifyClusterSubscriptionHasChanged(subscription, SubscriptionListener.SubscriptionChange.DELETED);
                }
            }
        }
        for (String topic : topics) {
            List<String> subscriptionsFromStore = results.get(TOPIC_PREFIX + topic);
            List<AndesSubscription> newSubscriptionList = new ArrayList<AndesSubscription>();
            if (subscriptionsFromStore != null) {
                for (String subscriptionAsStr : subscriptionsFromStore) {
                    BasicSubscription subscription = new BasicSubscription(subscriptionAsStr);
                    newSubscriptionList.add(subscription);
                }
            }
            List<AndesSubscription> oldSubscriptionList;
            oldSubscriptionList = subscriptionStore.replaceClusterSubscriptionListOfDestination
                    (topic, new ArrayList<AndesSubscription>(newSubscriptionList), true);
            if (oldSubscriptionList == null) {
                oldSubscriptionList = Collections.emptyList();
            }
            if (subscriptionListeners.size() > 0) {
                List<AndesSubscription> duplicatedNewSubscriptionList = new ArrayList<AndesSubscription>(newSubscriptionList);
                /**
                 * for all subscriptions which are in store but ont in-memory simulate the incoming 'create'
                 * cluster notification for the subscription
                 */
                newSubscriptionList.removeAll(oldSubscriptionList);
                for (AndesSubscription subscription : newSubscriptionList) {
                    log.warn("Recovering node. Adding subscription " + subscription.toString());
                    notifyClusterSubscriptionHasChanged(subscription, SubscriptionListener.SubscriptionChange.ADDED);
                }

                /**
                 * for all subscriptions which are in-memory but not in store simulate the incoming 'delete'
                 * cluster notification for the subscription
                 */
                oldSubscriptionList.removeAll(duplicatedNewSubscriptionList);
                for (AndesSubscription subscription : oldSubscriptionList) {
                    log.warn("Recovering node. Removing subscription " + subscription.toString());
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


}
