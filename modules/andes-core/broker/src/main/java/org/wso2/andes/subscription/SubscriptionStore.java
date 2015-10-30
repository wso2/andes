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
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.AndesSubscription.SubscriptionType;
import org.wso2.andes.kernel.SubscriptionListener.SubscriptionChange;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.carbon.metrics.manager.Gauge;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.MetricManager;

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
import java.util.HashMap;

public class SubscriptionStore {
    private static final String TOPIC_PREFIX = "topic.";

    private static final String QUEUE_PREFIX = "queue.";

    private static Log log = LogFactory.getLog(SubscriptionStore.class);

    /**
     * Keeps non-wildcard cluster topic subscriptions.
     */
    private Map<String, Set<AndesSubscription>> clusterTopicSubscriptionMap = new ConcurrentHashMap<>();

    /**
     * Keeps non-wildcard cluster queue subscriptions.
     */
    private Map<String, Set<AndesSubscription>> clusterQueueSubscriptionMap = new ConcurrentHashMap<>();

    //<destination, <subscriptionID,LocalSubscription>>
    private Map<String, Set<LocalSubscription>> localTopicSubscriptionMap = new ConcurrentHashMap<>();
    private Map<String, Set<LocalSubscription>> localQueueSubscriptionMap = new ConcurrentHashMap<>();

    /**
     * Channel wise indexing of local subscriptions for acknowledgement handling
     */
    private Map<UUID, LocalSubscription> channelIdMap = new ConcurrentHashMap<>();

    private AndesContextStore andesContextStore;

    private ClusterSubscriptionProcessor clusterSubscriptionProcessor;

    public SubscriptionStore() throws AndesException {
        andesContextStore = AndesContext.getInstance().getAndesContextStore();
        clusterSubscriptionProcessor = ClusterSubscriptionProcessorBuilder.getBitMapClusterSubscriptionProcessor();

        //Add subscribers gauge to metrics manager
        MetricManager.gauge(Level.INFO, MetricsConstants.QUEUE_SUBSCRIBERS, new QueueSubscriberGauge());
        //Add topic gauge to metrics manager
        MetricManager.gauge(Level.INFO, MetricsConstants.TOPIC_SUBSCRIBERS, new TopicSubscriberGauge());
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
        Set<AndesSubscription> subscriptions = new HashSet<>();
        if (isTopic) {
            Set<AndesSubscription> directSubscriptions = clusterTopicSubscriptionMap.get(destination);

            if (null != directSubscriptions) {
                subscriptions.addAll(directSubscriptions);
            }
            // Get wildcard subscriptions from bitmap
            subscriptions.addAll(clusterSubscriptionProcessor.getMatchingSubscriptions(destination, subscriptionType));
        } else {
            Set<AndesSubscription> queueSubscriptions = clusterQueueSubscriptionMap.get(destination);

            if (null != queueSubscriptions) {
                subscriptions.addAll(queueSubscriptions);
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
        Set<String> destinations = new HashSet<>();

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
        Set<AndesSubscription> subscriptions = new HashSet<>();

        if (isTopic) {
            Set<AndesSubscription> clusterSubscriptions = clusterTopicSubscriptionMap.get(destination);

            if (null != clusterSubscriptions) {
                //If there're cluster subscriptions we need to add them to the iterator
                Iterator<AndesSubscription> subscriptionIterator = clusterSubscriptions.iterator();
                //We use an iterator here, we need to ensure that there will not be any ConccurentModifaction issues
                //Due to the subscribers being added to the cluster while retrieving them
                while (subscriptionIterator.hasNext()) {
                    subscriptions.add(subscriptionIterator.next());
                }
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
        
        Set<LocalSubscription> activeLocalSubscriptionList = new HashSet<>();
        
        if (null != localSubscriptionMap ) {
            
            for (LocalSubscription localSubscription : localSubscriptionMap) {
                if (localSubscription.hasExternalSubscriptions()) {
                    activeLocalSubscriptionList.add(localSubscription);
                }
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
        Set<AndesSubscription> activeQueueSubscriptions = new HashSet<>();
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
        Set<LocalSubscription> activeQueueSubscriptions = new HashSet<>();
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
    private Set<LocalSubscription> getLocalSubscriptionMap(String destination, boolean isTopic) {
        Map<String, Set<LocalSubscription>> subscriptionMap = isTopic ? localTopicSubscriptionMap :
                localQueueSubscriptionMap;
        return subscriptionMap.get(destination);

    }

    /**
     * get all (active/inactive) CLUSTER subscriptions for a queue/topic.
     * <p/>
     * If it is for a Queue, the destination will be the name of the queue. If it is for a topic, the destination is
     * the name of the internal queue created for a subscriber.
     *
     * @param destination the name of the destination queue
     * @param isTopic     TRUE if checking topics
     * @return Set of subscriptions
     */
    private Set<AndesSubscription> getClusterSubscriptionList(String destination, boolean isTopic)
            throws AndesException {
        Set<AndesSubscription> clusterSubscriptions;

        if (isTopic) {

            // The clusterTopicSubscriptionMap contains the topic names as the set of keys and a set of
            // AndesSubscriptions destined to each topic. But here we need to find the subscriptions by the queue that
            // it is bound to. Thus, we cannot simply get the subscriptions using the keys in the
            // clusterTopicSubscriptionMap
            clusterSubscriptions = getClusterTopicSubscriptionsBoundToQueue(destination);
        } else {
            clusterSubscriptions = clusterQueueSubscriptionMap.get(destination);
        }
        return clusterSubscriptions;
    }

    /**
     * Method to retrieve  all subscriptions that are bound a given queue.
     * <p/>
     * The clusterTopicSubscriptionMap contains the topic names as the set of keys and a set of AndesSubscriptions
     * destined to each topic. But in reality, each topic subscription is bound to a queue with the subscriptionId as
     * the binding key. This method can be used to get all such subscriptions bound to a particular queue by providing
     * the queue name.
     *
     * @param queueName the queue name for which the subscriptions are bound
     * @return A set of subscriptions bound to the given queue name
     */
    private Set<AndesSubscription> getClusterTopicSubscriptionsBoundToQueue(String queueName) throws AndesException {

        Set<AndesSubscription> subscriptionsOfQueue = new HashSet<>();

        for (String destination : getTopics()) {
            // Get all AMQP subscriptions
            Set<AndesSubscription> topicSubsOfDest = getAllSubscribersForDestination(destination, true,
                    SubscriptionType.AMQP);
            for (AndesSubscription sub : topicSubsOfDest) {
                if (sub.getTargetQueue().equals(queueName)) {
                    subscriptionsOfQueue.add(sub);
                }
            }
            //Get all MQTT subscriptions
            topicSubsOfDest = getAllSubscribersForDestination(destination, true, SubscriptionType.MQTT);
            for (AndesSubscription sub : topicSubsOfDest) {
                if (queueName.equals(sub.getTargetQueue())) {
                    subscriptionsOfQueue.add(sub);
                }
            }
        }
        return subscriptionsOfQueue;
    }

    /**
     * Get all (ACTIVE/INACTIVE) CLUSTER subscriptions for a queue/topic. This call returns the object as it is
     * without matching wildcards
     * @param destination queue/topic name
     * @param isTopic TRUE if checking topics
     * @return Set of subscriptions
     * @throws AndesException
     */
    public Set<AndesSubscription> getAllClusterSubscriptionsWithoutWildcards(String destination, boolean isTopic)
            throws AndesException {
        Map<String, Set<AndesSubscription>> subscriptionMap = isTopic ? clusterTopicSubscriptionMap :
                clusterQueueSubscriptionMap;
        return subscriptionMap.get(destination);
    }

    /**
     * get all ACTIVE CLUSTER subscriptions for a queue.
     * <p/>
     * If it is for a Queue, the destination will be the name of the queue. If it is for a topic, the destination is
     * the name of the internal queue created for a subscriber.
     *
     * @param destination queue name of
     * @param isTopic     is destination is bound to topic
     * @return Set of matching subscriptions
     */
    public Set<AndesSubscription> getActiveClusterSubscriptionList(String destination, boolean isTopic)
            throws AndesException {
        Set<AndesSubscription> activeSubscriptions = new HashSet<>();
        Set<AndesSubscription> allSubscriptions = getClusterSubscriptionList(destination, isTopic);
        if (null != allSubscriptions) {
            if (isTopic){
                Iterator<AndesSubscription> iterator = allSubscriptions.iterator();
                while (iterator.hasNext()){
                    AndesSubscription currentSubscription = iterator.next();
                    if (!(currentSubscription.hasExternalSubscriptions())){
                        iterator.remove();
                    }
                }
            }
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
            if ((SubscriptionType.AMQP == subscriptionType && AMQPUtils.isWildCardDestination(destination))
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
        Set<LocalSubscription> subscriptionsOfQueue = new HashSet<>();
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
     * Get ALL (ACTIVE + INACTIVE) local subscriptions whose bound queue is given
     *
     * @param queueName Queue name to search
     * @return List if matching subscriptions
     * @throws AndesException
     */
    public Set<AndesSubscription> getListOfClusterSubscriptionsBoundToQueue(String queueName) throws AndesException {
        Set<AndesSubscription> subscriptionsOfQueue = new HashSet<>();
        Set<AndesSubscription> queueSubscriptionMap = clusterQueueSubscriptionMap.get(queueName);

        // Add queue subscriptions
        if (null != queueSubscriptionMap) {
            subscriptionsOfQueue.addAll(queueSubscriptionMap);
        }
        //Add topic subscriptions bound to queue name
        for (String destination : getTopics()) {

            // Get all AMQP subscriptions
            Set<AndesSubscription> topicSubsOfDest = getAllSubscribersForDestination(destination, true,
                    SubscriptionType.AMQP);
            for (AndesSubscription sub : topicSubsOfDest) {
                if (sub.getTargetQueue().equals(queueName)) {
                    subscriptionsOfQueue.add(sub);
                }
            }

            //Get all MQTT subscriptions
            topicSubsOfDest = getAllSubscribersForDestination(destination, true, SubscriptionType.MQTT);
            for (AndesSubscription sub : topicSubsOfDest) {
                if (sub.getTargetQueue().equals(queueName)) {
                    subscriptionsOfQueue.add(sub);
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
            wildCardSubscription = isWildCardSubscription(subscription);

            if (wildCardSubscription) {
                if (SubscriptionChange.ADDED == type) {
                    clusterSubscriptionProcessor.addWildCardSubscription(subscription);
                } else if (SubscriptionChange.DELETED == type) {
                    clusterSubscriptionProcessor.removeWildCardSubscription(subscription);
                }else if (SubscriptionChange.DISCONNECTED == type) {
                    clusterSubscriptionProcessor.updateWildCardSubscription(subscription);
                }
            } else {
                clusterSubscriptionMap = clusterTopicSubscriptionMap;
            }

        } else {
            clusterSubscriptionMap = clusterQueueSubscriptionMap;
        }

        if (!wildCardSubscription) {
            Set<AndesSubscription> subscriptionList = clusterSubscriptionMap.get(destination);
            if (null == subscriptionList) {
                subscriptionList = new LinkedHashSet<>();
            }
            //TODO: need to use a MAP here instead of a SET. Here we assume a subscription is not updated and added.
            if(SubscriptionChange.ADDED == type) {
                boolean subscriptionAdded = subscriptionList.add(subscription);
                if(!subscriptionAdded) {
                    Iterator<AndesSubscription> subscriptionIterator = subscriptionList.iterator();
                    while(subscriptionIterator.hasNext()) {
                        AndesSubscription andesSubscription = subscriptionIterator.next();
                        if(subscription.equals(andesSubscription)) {
                            andesSubscription.setHasExternalSubscriptions(true);
                            break;
                        }
                    }
                }
            } else if(SubscriptionChange.DISCONNECTED == type) {
                boolean subscriptionAdded = subscriptionList.add(subscription);
                if (!subscriptionAdded){
                    Iterator<AndesSubscription> subscriptionIterator = subscriptionList.iterator();
                    while(subscriptionIterator.hasNext()) {
                        AndesSubscription andesSubscription = subscriptionIterator.next();
                        if(subscription.equals(andesSubscription)) {
                            andesSubscription.setHasExternalSubscriptions(false);
                            break;
                        }
                    }
                } else {
                    log.warn("Cannot disconnect non-existing subscription");
                }

            } else if(SubscriptionChange.DELETED == type) {
                subscriptionList.remove(subscription);
            }

            clusterSubscriptionMap.put(destination, subscriptionList);

        }

        if (log.isDebugEnabled()) {
            log.debug("\n\tUpdated cluster subscription maps\n");
            this.printSubscriptionMap(clusterQueueSubscriptionMap);
            this.printSubscriptionMap(clusterTopicSubscriptionMap);
            log.debug("\n");
        }
    }

    /**
     * Update a subscription object with the given object.
     * Use to update subscription properties of already available subscriptions.
     *
     * @param subscription The subscription with updated properties
     * @throws AndesException
     */
    public void updateClusterSubscription(AndesSubscription subscription) throws AndesException {
        if (subscription.isBoundToTopic()) {
            if (isWildCardSubscription(subscription)) {
                clusterSubscriptionProcessor.updateWildCardSubscription(subscription);
            } else {
                Set<AndesSubscription> subscriptions = clusterTopicSubscriptionMap.get(subscription
                        .getSubscribedDestination());

                subscriptions.remove(subscription);
                subscriptions.add(subscription);
            }
        } else {
            Set<AndesSubscription> subscriptions = clusterQueueSubscriptionMap.get(subscription
                    .getSubscribedDestination());

            subscriptions.remove(subscription);
            subscriptions.add(subscription);
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
     */
    public synchronized void createDisconnectOrRemoveLocalSubscription(LocalSubscription subscription,
                                                                       SubscriptionChange type)
            throws AndesException {



        if (type == SubscriptionChange.ADDED || type == SubscriptionChange.DISCONNECTED) {

            String destinationQueue = getDestination(subscription);
            String destinationTopic = subscription.getSubscribedDestination();
            //Store the subscription
            String destinationIdentifier = (subscription.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX) + destinationTopic;
            String subscriptionID = subscription.getSubscribedNode() + "_" + subscription.getSubscriptionID();

            if (type == SubscriptionChange.ADDED) {
                andesContextStore.storeDurableSubscription(destinationIdentifier, subscriptionID, subscription.encodeAsStr());
                log.info("Local subscription " + type + " " + subscription.toString());
            } else { // @DISCONNECT
                updateLocalSubscriptionSubscription(subscription);
                log.info("Local subscription " + type + " " + subscription.toString());
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
            log.info("Local Subscription "  + type + " " + subscription.toString());
        }
        // Update channel id map
        if (type == SubscriptionChange.ADDED) {
            channelIdMap.put(subscription.getChannelID(), subscription);
        } else { //@DISCONNECT or REMOVE
            UUID channelIDOfSubscription = subscription.getChannelID();
            //when we delete the mock durable topic subscription it has no underlying channel
            if(null != channelIDOfSubscription) {
                channelIdMap.remove(channelIDOfSubscription);
            }
        }

    }

    /**
     * Update local subscription in database
     *
     * @param subscription  updated subscription
     * @throws AndesException
     */
    public void updateLocalSubscriptionSubscription(LocalSubscription subscription) throws AndesException {
        String destinationQueue = getDestination(subscription);
        String destinationTopic = subscription.getSubscribedDestination();
        //Update the subscription
        String destinationIdentifier = (subscription.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX) + destinationTopic;
        String subscriptionID = subscription.getSubscribedNode() + "_" + subscription.getSubscriptionID();

        andesContextStore.updateDurableSubscription(destinationIdentifier, subscriptionID, subscription.encodeAsStr());

        //update local subscription map
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

        UUID channelIDOfSubscription = subscription.getChannelID();
        channelIdMap.put(channelIDOfSubscription, subscription);
    }

    /**
     * Directly remove a subscription from store
     * @param subscriptionToRemove subscription to remove
     * @throws AndesException on an exception dealing with store
     */
    public void removeSubscriptionDirectly(AndesSubscription subscriptionToRemove) throws AndesException {
        String destination = subscriptionToRemove.getSubscribedDestination();
        String destinationIdentifier = (subscriptionToRemove.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX) + destination;
        andesContextStore.removeDurableSubscription(destinationIdentifier,
                subscriptionToRemove.getSubscribedNode() + "_" + subscriptionToRemove.getSubscriptionID());
        if(log.isDebugEnabled()) {
            log.debug("Directly removed cluster subscription subscription identifier = " + destinationIdentifier + " "
                      + "destination = " + destination);
        }
    }

    /**
     * To remove the local subscription
     * @param subscription Subscription to be removed
     * @return the removed local subscription
     * @throws AndesException
     */
    public LocalSubscription removeLocalSubscription(LocalSubscription subscription) throws AndesException {

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
            //if a topic subscription destination identifier for DB must be topic
            if(subscriptionToRemove.isBoundToTopic()) {
                destination = subscriptionToRemove.getSubscribedDestination();
            }
            String destinationIdentifier = (subscriptionToRemove.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX) + destination;
            andesContextStore.removeDurableSubscription(destinationIdentifier,
                    subscription.getSubscribedNode() + "_" + subscriptionID);
            if (log.isDebugEnabled()) {
                log.debug("Subscription Removed Locally for  " + destination + "@" + subscriptionID + " "
                          + subscriptionToRemove);
            }
        } else {
            log.warn("Could not find a local subscription ID " + subscriptionID + " under destination " + destination);
        }
        return subscriptionToRemove;
    }

    /**
     * Remove cluster subscriptions from database
     *
     * @param subscriptionToRemove The the set of andes subscriptions to be removed
     */
    public void removeClusterSubscriptions(Set<AndesSubscription> subscriptionToRemove) throws AndesException {
        Iterator<AndesSubscription> iterator = subscriptionToRemove.iterator();
        while (iterator.hasNext()) {
            AndesSubscription subscription = iterator.next();
            String destination = getDestination(subscription);
            if (!subscriptionToRemove.isEmpty()) {
                //if a topic subscription destination identifier for DB must be topic
                if (subscription.isBoundToTopic()) {
                    destination = subscription.getSubscribedDestination();
                }
                String destinationIdentifier =
                        (subscription.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX) + destination;
                andesContextStore.removeDurableSubscription(destinationIdentifier,
                        subscription.getSubscribedNode() + "_" + subscription.getSubscriptionID());
                if (log.isDebugEnabled()) {
                    log.debug("Subscription Removed for  " + destination + "@"
                              + subscription.getSubscriptionID() + " " + subscriptionToRemove);
                }
            } else {
                log.warn("Could not find a cluster subscription ID " + subscription.getSubscriptionID()
                         + " under destination " + destination);
            }
        }
    }

    /**
     * Gets a set of ACTIVE and INACTIVE topics in cluster
     *
     * @return set of ACTIVE and INACTIVE topics in cluster
     */
    public Set<String> getTopics() {
        Set<String> topics = new HashSet<>();

        // Add all destinations from direct subscriptions
        topics.addAll(clusterTopicSubscriptionMap.keySet());

        // Add all destination from wildcard subscriptions
        topics.addAll(clusterSubscriptionProcessor.getAllTopics());


        return topics;
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
                        String destinationQueue = subscription.getSubscribedDestination();
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

    /**
     * Marks all the durable subscriptions as inactive
     *
     * @throws AndesException Throw when updating the context store.
     */
    public void deactivateAllActiveSubscriptions() throws AndesException {

        Map<String, String> subscriptions = andesContextStore.getAllDurableSubscriptionsByID();
        Map<String, String> modifiedSubscriptions = new HashMap<>();

        boolean subscriptionExists = false;

        for (Map.Entry<String, String> entry : subscriptions.entrySet()) {

            if (log.isDebugEnabled()) {
                log.debug("Deactivating subscription with id: " + entry.getKey());
            }
            BasicSubscription subscription = new BasicSubscription(entry.getValue());

            //The HasExternalSubscriptions attribute of a subscription indicates whether the the subscription is active
            //Therefore, setting it to false makes the subscriptions inactive
            subscription.setHasExternalSubscriptions(subscriptionExists);

            modifiedSubscriptions.put(entry.getKey(), subscription.encodeAsStr());

        }

        //update all the stored durable subscriptions to be inactive
        andesContextStore.updateDurableSubscriptions(modifiedSubscriptions);
    }

    /**
     * Gauge will return total number of queue subscriptions for current node
     */
    private class QueueSubscriberGauge implements Gauge<Integer> {
        @Override
        public Integer getValue() {
            int count = 0;
            for (Set subscriptionSet : localQueueSubscriptionMap.values()) {
                count += subscriptionSet.size();
            }
            return count;

        }
    }

    /**
     * Gauge will return total number of topic subscriptions current node
     */
    private class TopicSubscriberGauge implements Gauge {
        @Override
        public Integer getValue() {
            int count = 0;
            for (Set subscriptionSet : localTopicSubscriptionMap.values()) {
                count += subscriptionSet.size();
            }
            return count;
        }
    }

    /**
     * Check if the given subscriber is subscribed with a wildcard subscription
     *
     * @param subscription The subscriber to check for wildcard subscription
     * @return True if subscribed ot a wildcard destination
     */
    private boolean isWildCardSubscription(AndesSubscription subscription) {
        boolean wildCardSubscription = false;
        String destination = subscription.getSubscribedDestination();

        if (AndesSubscription.SubscriptionType.AMQP == subscription.getSubscriptionType()) {
            wildCardSubscription = AMQPUtils.isWildCardDestination(destination);
        } else if (AndesSubscription.SubscriptionType.MQTT == subscription.getSubscriptionType()) {
            wildCardSubscription = MQTTUtils.isWildCardSubscription(destination);
        }

        return wildCardSubscription;
    }
}
