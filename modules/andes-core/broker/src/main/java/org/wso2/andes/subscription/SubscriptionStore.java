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

package org.wso2.andes.subscription;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.AndesSubscription.SubscriptionType;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.SubscriptionAlreadyExistsException;
import org.wso2.andes.kernel.SubscriptionListener.SubscriptionChange;
import org.wso2.andes.mqtt.MQTTUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionStore {
    private static final String TOPIC_PREFIX = "topic.";

    private static final String QUEUE_PREFIX = "queue.";

    private static Log log = LogFactory.getLog(SubscriptionStore.class);

    /**
     * Keeps non-wildcard cluster topic subscriptions.
     */
    private Map<String, Set<AndesSubscription>> clusterTopicSubscriptionMap = new ConcurrentHashMap<String, Set<AndesSubscription>>();

    /**
     * Keeps non-wildcard cluster queue subscriptions.
     */
    private Map<String, Set<AndesSubscription>> clusterQueueSubscriptionMap = new ConcurrentHashMap<String, Set<AndesSubscription>>();

    //<destination, <subscriptionID,LocalSubscription>>
    private Map<String, Set<LocalSubscription>> localTopicSubscriptionMap = new ConcurrentHashMap<String, Set<LocalSubscription>>();
    private Map<String, Set<LocalSubscription>> localQueueSubscriptionMap = new ConcurrentHashMap<String, Set<LocalSubscription>>();

    /**
     * Channel wise indexing of local subscriptions for acknowledgement handling
     */
    private Map<UUID, LocalSubscription> channelIdMap = new ConcurrentHashMap<UUID, LocalSubscription>();

    private AndesContextStore andesContextStore;

    private ClusterSubscriptionProcessor clusterSubscriptionProcessor;

    public SubscriptionStore() throws AndesException {
        andesContextStore = AndesContext.getInstance().getAndesContextStore();
        clusterSubscriptionProcessor = ClusterSubscriptionProcessorBuilder.getBitMapClusterSubscriptionProcessor();
    }

    /**
     * CALLED BY UI ONLY.
     * get all CLUSTER subscription entries subscribed for a queue/topic
     *
     * @param destination queue/topic name
     * @param isTopic     is requesting topic subscriptions
     * @return list of andes subscriptions
     * @throws AndesException
     */
    public Set<AndesSubscription> getAllSubscribersForDestination(String destination, boolean isTopic,
                                                                  AndesSubscription.SubscriptionType
                                                                          subscriptionType) throws AndesException {
        // Returning empty set if requested map is empty
        Set<AndesSubscription> subscriptions = new HashSet<AndesSubscription>();
        if (isTopic) {
            Set<AndesSubscription> directSubscriptions = clusterTopicSubscriptionMap.get(destination);

            if (null != directSubscriptions) {
                subscriptions = directSubscriptions;
            }
            // Get wildcard subscriptions from bitmap
            subscriptions.addAll(clusterSubscriptionProcessor.getMatchingSubscriptions(destination, subscriptionType));
        } else {
            Set<AndesSubscription> queueSubscriptions = clusterQueueSubscriptionMap.get(destination);

            if (null != queueSubscriptions) {
                subscriptions = queueSubscriptions;
            }
        }


        return subscriptions;
    }

    /**
     * get all CLUSTER queues/topics where subscriptions are available
     *
     * @param isTopic TRUE if checking topics
     * @return Set of queues/topics
     */
    public Set<String> getAllDestinationsOfSubscriptions(boolean isTopic) {
        Set<String> destinations = new HashSet<String>();

        if (isTopic) {
            destinations.addAll(clusterTopicSubscriptionMap.keySet());
        } else {
            destinations.addAll(clusterQueueSubscriptionMap.keySet());
        }
        return destinations;
    }

    /**
     * get all (ACTIVE/INACTIVE) CLUSTER subscription entries subscribed for a queue/topic
     * hierarchical topic subscription mapping also happens here
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @param subscriptionType Type of the subscriptions
     * @return Set of andes subscriptions
     * @throws AndesException
     */
    public Set<AndesSubscription> getClusterSubscribersForDestination(String destination, boolean isTopic,
                                                                      SubscriptionType subscriptionType) throws
            AndesException {
        Set<AndesSubscription> subscriptions = new HashSet<AndesSubscription>();

        if (isTopic) {
            Set<AndesSubscription> clusterSubscriptions = clusterTopicSubscriptionMap.get(destination);

            if (null != clusterSubscriptions) {
                subscriptions.addAll(clusterSubscriptions);
            }

            // Get wildcard subscriptions
            subscriptions.addAll(clusterSubscriptionProcessor.getMatchingSubscriptions(destination, subscriptionType));

        } else {
            Set<AndesSubscription> queueSubscriptions = clusterQueueSubscriptionMap.get(destination);

            if (null != queueSubscriptions) {
                subscriptions = queueSubscriptions;
            }
        }

        return subscriptions;
    }

    /**
     * get all ACTIVE LOCAL subscription entries subscribed for a destination/topic
     * Hierarchical topic mapping is NOT considered here
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @return list of matching subscriptions
     */
    public Set<LocalSubscription> getActiveLocalSubscribers(String destination, boolean isTopic) throws AndesException {
        Set<LocalSubscription> localSubscriptionMap = getLocalSubscriptionMap(destination, isTopic);
        Set<LocalSubscription> list = new HashSet<LocalSubscription>();
        if (localSubscriptionMap != null) {
            list = getLocalSubscriptionMap(destination, isTopic);
        }

        Set<LocalSubscription> activeLocalSubscriptionList = new HashSet<LocalSubscription>();
        for (LocalSubscription localSubscription : list) {
            if (localSubscription.hasExternalSubscriptions()) {
                activeLocalSubscriptionList.add(localSubscription);
            }
        }
        return activeLocalSubscriptionList;
    }

    /**
     * Get all ACTIVE LOCAL subscription entries for destination (queue/topic)
     * hierarchical subscription mapping is NOT considered here
     *
     * @param destination queue or topic name
     * @return list of matching subscriptions
     * @throws AndesException
     */
    public Set<LocalSubscription> getActiveLocalSubscribersForQueuesAndTopics(String destination) throws
            AndesException {
        Set<LocalSubscription> allSubscriptions = getActiveLocalSubscribers(destination, false);
        allSubscriptions.addAll(getActiveLocalSubscribers(destination, true));

        Iterator<LocalSubscription> subscriptionIterator = allSubscriptions.iterator();

        while (subscriptionIterator.hasNext()) {
            LocalSubscription subscription = subscriptionIterator.next();

            if (!subscription.hasExternalSubscriptions()) {
                subscriptionIterator.remove();
            }
        }

        return allSubscriptions;
    }

    /**
     * Get local subscription given the channel id of subscription
     *
     * @param channelID id of the channel subscriber deals with
     * @return subscription object. Null if no match
     * @throws AndesException
     */
    public LocalSubscription getLocalSubscriptionForChannelId(UUID channelID) throws AndesException {
        return channelIdMap.get(channelID);
    }

    /**
     * get all ACTIVE CLUSTER subscription entries subscribed on a given node
     *
     * @param nodeID  id of the broker node
     * @param isTopic TRUE if checking topics
     * @return list of subscriptions
     */
    public Set<AndesSubscription> getActiveClusterSubscribersForNode(String nodeID, boolean isTopic) {
        Set<AndesSubscription> activeQueueSubscriptions = new HashSet<AndesSubscription>();
        Map<String, Set<AndesSubscription>> clusterSubscriptionMap = isTopic ? clusterTopicSubscriptionMap :
                clusterQueueSubscriptionMap;
        for (String destination : clusterSubscriptionMap.keySet()) {
            Set<AndesSubscription> subList = clusterSubscriptionMap.get(destination);
            for (AndesSubscription sub : subList) {
                if (sub.getSubscribedNode().equals(nodeID) && sub.hasExternalSubscriptions()) {
                    activeQueueSubscriptions.add(sub);
                }
            }
        }

        if (isTopic) {
            // Get wildcard subscriptions from bitmap. Only topics support wildcards.
            activeQueueSubscriptions.addAll(clusterSubscriptionProcessor.getActiveClusterSubscribersForNode(nodeID));
        }

        return activeQueueSubscriptions;
    }

    /**
     * get all ACTIVE LOCAL subscriptions for any queue/topic
     *
     * @param isTopic TRUE if checking topics
     * @return list of Local subscriptions
     */
    public Set<LocalSubscription> getActiveLocalSubscribers(boolean isTopic) {
        Set<LocalSubscription> activeQueueSubscriptions = new HashSet<LocalSubscription>();
        Map<String, Set<LocalSubscription>> localSubscriptionMap = isTopic ? localTopicSubscriptionMap :
                localQueueSubscriptionMap;
        for (String destination : localSubscriptionMap.keySet()) {
            Set<LocalSubscription> subscriptionSet = localSubscriptionMap.get(destination);
            activeQueueSubscriptions.addAll(subscriptionSet);
        }

        Iterator<LocalSubscription> subscriptionIterator = activeQueueSubscriptions.iterator();

        while (subscriptionIterator.hasNext()) {
            LocalSubscription subscription = subscriptionIterator.next();
            if (!subscription.hasExternalSubscriptions()) {
                subscriptionIterator.remove();
            }
        }

        return activeQueueSubscriptions;
    }

    /**
     * UI ONLY.
     * get number of active subscribers for queue/topic in CLUSTER
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @param subscriptionType Type of the subscriptions
     * @return number of subscriptions in cluster
     * @throws AndesException
     */
    public int numberOfSubscriptionsInCluster(String destination, boolean isTopic, SubscriptionType subscriptionType)
            throws
            AndesException {
        return getClusterSubscribersForDestination(destination, isTopic, subscriptionType).size();
    }

    /**
     * get a copy of local subscription list for a given queue/topic
     * hierarchical topic subscription mapping is NOT considered here
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @return a set of <subscription>
     */
    private Set<LocalSubscription> getLocalSubscriptionMap(String destination,
                                                           boolean isTopic) {
        Map<String, Set<LocalSubscription>> subscriptionMap = isTopic ? localTopicSubscriptionMap :
                localQueueSubscriptionMap;
        return subscriptionMap.get(destination);

    }

    /**
     * get all (active/inactive) CLUSTER subscriptions for a queue/topic.
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @param subscriptionType Type of the subscriptions
     * @return Set of subscriptions
     */
    private Set<AndesSubscription> getClusterSubscriptionList(String destination, boolean isTopic,
                                                              SubscriptionType subscriptionType) throws AndesException {
        Map<String, Set<AndesSubscription>> subscriptionMap = isTopic ? clusterTopicSubscriptionMap :
                clusterQueueSubscriptionMap;
        Set<AndesSubscription> clusterSubscriptions = subscriptionMap.get(destination);

        // Get wildcard subscriptions from bitmap
        if (isTopic) {
            clusterSubscriptions.addAll(clusterSubscriptionProcessor.getMatchingSubscriptions(destination,
                    subscriptionType));
        }
        return clusterSubscriptions;
    }

    /**
     * get all ACTIVE CLUSTER subscriptions for a queue/topic. For topics this will return
     * subscriptions whose destination is exactly matching to the given destination only.
     * (hierarchical mapping not considered)
     *
     * @param destination queue or topic name
     * @param isTopic     is destination a topic
     * @param subscriptionType Type of the subscriptions
     * @return Set of matching subscriptions
     */
    public Set<AndesSubscription> getActiveClusterSubscriptionList(String destination, boolean isTopic,
                                                                   SubscriptionType subscriptionType) throws
            AndesException {
        Set<AndesSubscription> activeSubscriptions = new HashSet<AndesSubscription>();
        Set<AndesSubscription> allSubscriptions = getClusterSubscriptionList(destination, isTopic, subscriptionType);
        if (null != allSubscriptions) {
            activeSubscriptions = allSubscriptions;
        }
        return activeSubscriptions;
    }

    /**
     * Check if a given subscription is already available in the subscription store.
     * Use to validate data of the subscription store.
     *
     * @param subscription The subscription to check for
     * @return True if available in the store
     * @throws AndesException
     */
    public boolean isSubscriptionAvailable(AndesSubscription subscription) throws AndesException {
        boolean subscriptionFound = false;
        String destination = subscription.getSubscribedDestination();
        if (subscription.isBoundToTopic()) {
            SubscriptionType subscriptionType = subscription.getSubscriptionType();
            if ((SubscriptionType.AMQP == subscriptionType && AMQPUtils.isWildCardSubscription(destination))
                    || (SubscriptionType.MQTT == subscriptionType && MQTTUtils.isWildCardSubscription(destination))) {
                subscriptionFound = clusterSubscriptionProcessor.isSubscriptionAvailable(subscription);
            } else {
                Set<AndesSubscription> directSubscriptions = clusterTopicSubscriptionMap.get(destination);

                if (null != directSubscriptions) {
                    subscriptionFound = directSubscriptions.contains(subscription);
                }
            }
        } else {
            Set<AndesSubscription> directSubscriptions = clusterQueueSubscriptionMap.get(destination);

            if (null != directSubscriptions) {
                subscriptionFound = directSubscriptions.contains(subscription);
            }
        }

        return subscriptionFound;
    }

    /**
     * Get ALL (ACTIVE + INACTIVE) local subscriptions whose bound queue is given
     *
     * @param queueName Queue name to search
     * @return List if matching subscriptions
     * @throws AndesException
     */
    public Set<LocalSubscription> getListOfLocalSubscriptionsBoundToQueue(String queueName) throws AndesException {
        Set<LocalSubscription> subscriptionsOfQueue = new HashSet<LocalSubscription>();
        Set<LocalSubscription> queueSubscriptionMap = localQueueSubscriptionMap.get(queueName);
        if (queueSubscriptionMap != null) {
            subscriptionsOfQueue.addAll(queueSubscriptionMap);
        }
        Map<String, Set<LocalSubscription>> topicSubscriptionMap = localTopicSubscriptionMap;
        for (String destination : topicSubscriptionMap.keySet()) {
            Set<LocalSubscription> topicSubsOfDest = topicSubscriptionMap.get(destination);
            if (topicSubsOfDest != null) {
                for (LocalSubscription sub : topicSubsOfDest) {
                    if (sub.getTargetQueue().equals(queueName)) {
                        subscriptionsOfQueue.add(sub);
                    }
                }
            }
        }

        return subscriptionsOfQueue;
    }

    /**
     * create disconnect or remove a cluster subscription entry.
     *
     * @param subscription subscription to add disconnect or remove
     * @param type         tye pf change
     * @throws AndesException
     */
    public synchronized void createDisconnectOrRemoveClusterSubscription(AndesSubscription subscription,
                                                                         SubscriptionChange type) throws
            AndesException {
        // Treat durable subscription for topic as a queue subscription. Therefore it is in
        // cluster queue subscription map
        boolean topicSubscriptionMap = subscription.isBoundToTopic();
        boolean wildCardSubscription = false;
        String destination = subscription.getSubscribedDestination();

        Map<String, Set<AndesSubscription>> clusterSubscriptionMap = null;

        if (topicSubscriptionMap) {
            // Check if this is a wildcard subscription
            if (AndesSubscription.SubscriptionType.AMQP == subscription.getSubscriptionType()) {
                wildCardSubscription = AMQPUtils.isWildCardSubscription(destination);
            } else if (AndesSubscription.SubscriptionType.MQTT == subscription.getSubscriptionType()) {
                wildCardSubscription = MQTTUtils.isWildCardSubscription(destination);
            }

            if (wildCardSubscription) {
                if (SubscriptionChange.ADDED == type) {
                    clusterSubscriptionProcessor.addWildCardSubscription(subscription);
                } else if (SubscriptionChange.DELETED == type) {
                    clusterSubscriptionProcessor.removeWildCardSubscription(subscription);
                }
            } else {
                clusterSubscriptionMap = clusterTopicSubscriptionMap;
            }

        } else {
            clusterSubscriptionMap = clusterQueueSubscriptionMap;
        }

        if (!wildCardSubscription) {
            Set<AndesSubscription> subscriptionList = clusterSubscriptionMap.get(destination);

            boolean subscriptionNotAvailable = true;

            if (null == subscriptionList) {
                subscriptionList = new LinkedHashSet<AndesSubscription>();
            } else {
                subscriptionNotAvailable = false;
                subscriptionList.remove(subscription);
            }

            if (SubscriptionChange.ADDED == type || SubscriptionChange.DISCONNECTED == type) {
                subscriptionList.add(subscription);

                if (subscriptionNotAvailable) {
                    clusterSubscriptionMap.put(destination, subscriptionList);
                }
            }

        }

        if (log.isDebugEnabled()) {
            log.debug("\n\tUpdated cluster subscription maps\n");
            this.printSubscriptionMap(clusterQueueSubscriptionMap);
            this.printSubscriptionMap(clusterTopicSubscriptionMap);
            log.debug("\n");
        }
    }

    /**
     * To print the subscription maps with the destinations
     *
     * @param map Map to be printed
     */
    private void printSubscriptionMap(Map<String, Set<AndesSubscription>> map) {
        for (Entry<String, Set<AndesSubscription>> entry : map.entrySet()) {
            log.debug("Destination: " + entry.getKey());
            for (AndesSubscription s : entry.getValue()) {
                log.debug("\t---" + s.encodeAsStr());
            }
        }
    }

    /**
     * Create,disconnect or remove local subscription
     *
     * @param subscription subscription to add/disconnect or remove
     * @param type         type of change
     * @throws AndesException
     * @throws org.wso2.andes.kernel.SubscriptionAlreadyExistsException
     */
    public synchronized void createDisconnectOrRemoveLocalSubscription(LocalSubscription subscription,
                                                                       SubscriptionChange type)
            throws AndesException, SubscriptionAlreadyExistsException {

        Boolean allowSharedSubscribers = AndesConfigurationManager.readValue(AndesConfiguration.ALLOW_SHARED_SHARED_SUBSCRIBERS);
        //We need to handle durable topic subscriptions
        boolean durableSubExists = false;
        boolean hasExternalSubscriptions = false;
        if (subscription.isDurable()) {

            // Check if an active durable subscription already in place. If so we should not accept the subscription
            // Scan all the destinations as the subscription can come for different topic
            for (Entry<String, Set<AndesSubscription>> entry : clusterTopicSubscriptionMap.entrySet()) {
                Set<AndesSubscription> existingSubscriptions = entry.getValue();
                if (existingSubscriptions != null && !existingSubscriptions.isEmpty()) {
                    for (AndesSubscription sub : existingSubscriptions) {
                        // Queue is durable and target queues are matched
                        if (sub.isDurable() && sub.getTargetQueue().equals(subscription.getTargetQueue())) {
                            durableSubExists = true;
                            // Target queue for durable topic subscription has an active subscriber
                            if (subscription.isBoundToTopic() && sub.hasExternalSubscriptions()) {
                                hasExternalSubscriptions = true;
                                break;
                            }
                        }
                    }
                }
                if (hasExternalSubscriptions) {
                    break;
                }
            }

            if (!hasExternalSubscriptions && type == SubscriptionChange.DISCONNECTED) {
                //when there are multiple subscribers possible with same clientID we keep only one
                //topic subscription record for all of them. Thus when closing there can be no subscriber
                //to close in multiple durable topic subscription case
                if (!allowSharedSubscribers) {
                    // We cannot guarantee that the subscription has not been removed before,
                    // if the server is in shutting down state. No need to throw this exception. Warning is enough.
                    log.warn("There is no active subscriber to close subscribed to " + subscription
                            .getSubscribedDestination() + " with the queue " + subscription.getTargetQueue());
                }
            } else if (hasExternalSubscriptions && type == SubscriptionChange.ADDED) {
                if (!allowSharedSubscribers) {
                    //not permitted
                    throw new SubscriptionAlreadyExistsException("A subscription already exists for Durable subscriptions on " +
                            subscription.getSubscribedDestination() + " with the queue " + subscription.getTargetQueue());
                }
            }

        }

        if (type == SubscriptionChange.ADDED || type == SubscriptionChange.DISCONNECTED) {

            String destinationQueue = getDestination(subscription);
            String destinationTopic = subscription.getSubscribedDestination();
            //Store the subscription
            String destinationIdentifier = (subscription.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX) + destinationQueue;
            String subscriptionID = subscription.getSubscribedNode() + "_" + subscription.getSubscriptionID();

            if (type == SubscriptionChange.ADDED && !durableSubExists) {
                andesContextStore.storeDurableSubscription(destinationIdentifier, subscriptionID, subscription.encodeAsStr());
                log.info("New local subscription " + type + " " + subscription.toString());
            } else {
                andesContextStore.updateDurableSubscription(destinationIdentifier, subscriptionID, subscription.encodeAsStr());
                log.info("New local subscription " + type + " " + subscription.toString());
            }

            //add or update local subscription map
            if (subscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.DIRECT_EXCHANGE_NAME)) {
                Set<LocalSubscription> localSubscriptions = localQueueSubscriptionMap.get(destinationQueue);
                if (null == localSubscriptions) {
                    localSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<LocalSubscription, Boolean>());
                }

                // Remove the old subscription object if available before inserting the new object since
                // the properties that are not used for equals and hash_code method might have been changed
                // which can lead to leave the saved object unchanged
                localSubscriptions.remove(subscription);
                localSubscriptions.add(subscription);
                localQueueSubscriptionMap.put(destinationQueue, localSubscriptions);

            } else if (subscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME)) {
                Set<LocalSubscription> localSubscriptions = localTopicSubscriptionMap.get(destinationTopic);
                if (null == localSubscriptions) {
                    // A concurrent hash map has been used since since subscriptions can be added and removed while
                    // iterating
                    localSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<LocalSubscription, Boolean>());
                }
                localSubscriptions.add(subscription);
                localTopicSubscriptionMap.put(destinationTopic, localSubscriptions);

            }

        } else if (type == SubscriptionChange.DELETED) {
            removeLocalSubscription(subscription);
            log.info("Local Subscription Removed " + subscription.toString());
        }
        // Update channel id map
        if (type == SubscriptionChange.ADDED) {
            channelIdMap.put(subscription.getChannelID(), subscription);
        } else {
            channelIdMap.remove(subscription.getChannelID());
        }

    }

    /**
     * To remove the local subscription
     * @param subscription Subscription to be removed
     * @return the removed local subscription
     * @throws AndesException
     */
    private LocalSubscription removeLocalSubscription(AndesSubscription subscription) throws AndesException {
        String destination = getDestination(subscription);
        String subscriptionID = subscription.getSubscriptionID();
        LocalSubscription subscriptionToRemove = null;
        //check queue local subscriptions
        Set<LocalSubscription> subscriptionList = getLocalSubscriptionMap(destination, false);
        if (null != subscriptionList) {
            Iterator<LocalSubscription> iterator = subscriptionList.iterator();
            while (iterator.hasNext()) {
                LocalSubscription currentSubscription = iterator.next();
                if (currentSubscription.equals(subscription)) {
                    subscriptionToRemove = currentSubscription;
                    iterator.remove();
                    break;
                }
            }
            if (subscriptionList.isEmpty()) {
                localQueueSubscriptionMap.remove(destination);
            }
        }
        //check topic local subscriptions
        if (null == subscriptionToRemove) {
            subscriptionList = getLocalSubscriptionMap(destination, true);
            if (null != subscriptionList) {
                Iterator<LocalSubscription> iterator = subscriptionList.iterator();
                while (iterator.hasNext()) {
                    LocalSubscription currentSubscription = iterator.next();
                    if (currentSubscription.equals(subscription)) {
                        subscriptionToRemove = currentSubscription;
                        iterator.remove();
                        break;
                    }
                }
                if (subscriptionList.isEmpty()) {
                    localTopicSubscriptionMap.remove(destination);
                }

            }
        }
        if (null != subscriptionToRemove) {
            String destinationIdentifier = (subscriptionToRemove.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX) + destination;
            andesContextStore.removeDurableSubscription(destinationIdentifier, subscription.getSubscribedNode() + "_" + subscriptionID);
            if (log.isDebugEnabled())
                log.debug("Subscription Removed Locally for  " + destination + "@" + subscriptionID + " " + subscriptionToRemove);
        } else {
            log.warn("Could not find an subscription ID " + subscriptionID + " under destination " + destination);
        }
        return subscriptionToRemove;
    }

    /**
     * Gets a list of ACTIVE and INACTIVE topics in cluster
     *
     * @return list of ACTIVE and INACTIVE topics in cluster
     */
    public List<String> getTopics() {
        return new ArrayList<String>(clusterTopicSubscriptionMap.keySet());
    }

    /**
     * Return destination based on subscription
     * Destination would be target queue if it is durable topic, otherwise it is queue or non durable topic
     *
     * @param subscription subscription to get destination
     * @return destination of subscription
     */
    public String getDestination(AndesSubscription subscription) {
        if (subscription.isBoundToTopic() && subscription.isDurable()) {
            return subscription.getTargetQueue();
        } else {
            return subscription.getSubscribedDestination();
        }
    }

    /**
     * Marks all the durable subscriptions for a specific node with "has external" false. Meaning
     * that the subscription is marked disconnected. The "has external" refers that the subscription
     * is active or not.
     *
     * @param isCoordinator True if current node is the coordinator, false otherwise.
     * @param nodeID The current node ID.
     * @throws AndesException Throw when updating the context store.
     */
    public void deactivateClusterDurableSubscriptionsForNodeID(boolean isCoordinator, String nodeID)
            throws AndesException {
        for (Set<AndesSubscription> andesSubscriptions : clusterTopicSubscriptionMap.values()) {
            for (AndesSubscription subscription : andesSubscriptions) {
                if (subscription.isDurable() && subscription.isBoundToTopic() && nodeID.equals
                        (subscription.getSubscribedNode()) && subscription.hasExternalSubscriptions()) {

                    // Marking the subscription as false
                    subscription.setHasExternalSubscriptions(false);

                    if (log.isDebugEnabled()) {
                        log.debug("Updating cluster map with subscription ID : " + subscription.getSubscriptionID() +
                                  " with has external as false.");
                    }

                    // Updating the context store by the coordinator
                    if (isCoordinator) {
                        String destinationQueue = getDestination(subscription);
                        String destinationIdentifier = (subscription.isBoundToTopic() ?
                                                        TOPIC_PREFIX : QUEUE_PREFIX) +
                                                       destinationQueue;
                        String subscriptionID = subscription.getSubscribedNode() + "_" +
                                                subscription.getSubscriptionID();
                        andesContextStore.updateDurableSubscription(destinationIdentifier,
                                subscriptionID, subscription.encodeAsStr());
                        if (log.isDebugEnabled()) {
                            log.debug("Updating context store with subscription ID : " + subscription
                                    .getSubscriptionID() + " with has external as false.");
                        }
                    }
                }
            }
        }
    }
}
