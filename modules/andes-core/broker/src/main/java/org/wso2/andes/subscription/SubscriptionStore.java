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
import org.wso2.andes.configuration.enums.TopicMatchingSelection;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.SubscriptionListener.SubscriptionChange;
import org.wso2.andes.mqtt.MQTTUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionStore {
    private static final String TOPIC_PREFIX = "topic.";

    private static final String QUEUE_PREFIX = "queue.";

    private static Log log = LogFactory.getLog(SubscriptionStore.class);

    //<routing key, List of local subscriptions>
    //TODO: hasitha - wrap this list by a map to reduce cost
    private Map<String, List<AndesSubscription>> clusterTopicSubscriptionMap = new ConcurrentHashMap<String, List<AndesSubscription>>();
    private Map<String, List<AndesSubscription>> clusterQueueSubscriptionMap = new ConcurrentHashMap<String, List<AndesSubscription>>();

    //<destination, <subscriptionID,LocalSubscription>>
    private Map<String, Map<String, LocalSubscription>> localTopicSubscriptionMap = new ConcurrentHashMap<String, Map<String, LocalSubscription>>();
    private Map<String, Map<String, LocalSubscription>> localQueueSubscriptionMap = new ConcurrentHashMap<String, Map<String, LocalSubscription>>();

    /**
     * Channel wise indexing of local subscriptions for acknowledgement handling
     */
    private Map<UUID, LocalSubscription> channelIdMap = new ConcurrentHashMap<UUID, LocalSubscription>();

    private AndesContextStore andesContextStore;

    private SubscriptionBitMapHandler subscriptionBitMapHandler;

    /**
     * To switch between topic matching methods
     * native or bitmap
     * if true bitmap, if false some other methods;
     */
    private boolean isBitmap;
    /**
     * If 0 native method
     * If 1 bitmap
     */

    private TopicMatchingSelection topicMatchingSelection;

    public SubscriptionStore() throws AndesException {
        andesContextStore = AndesContext.getInstance().getAndesContextStore();
        subscriptionBitMapHandler = new SubscriptionBitMapHandler();

        topicMatchingSelection = AndesConfigurationManager.readValue(AndesConfiguration.PERFORMANCE_TUNING_TOPIC_MATCHING_METHOD);

        if (topicMatchingSelection == TopicMatchingSelection.BITMAPS) {
            log.info("Bit map topic matching selected");
            isBitmap = true;
        } else {
            isBitmap = false;
        }
    }

    /**
     * Get all cluster subscription entries subscribed for a queue/topic.
     *
     * @param destination Queue/Topic name
     * @param isTopic     Is requesting topic subscriptions
     * @return List of andes subscriptions
     * @throws AndesException
     */
    public List<AndesSubscription> getAllSubscribersForDestination(String destination, boolean isTopic)
                                                                        throws AndesException {
        // returning an empty list if requested map is empty.
        if (isBitmap) {
            if (isTopic) {
                if (subscriptionBitMapHandler
                            .getAllClusteredSubscribedForDestination(destination) == null) {
                    return Collections.emptyList();
                } else {
                    return new ArrayList<AndesSubscription>(subscriptionBitMapHandler
                                                                    .getAllClusteredSubscribedForDestination(destination));
                }
            } else {
                if (clusterQueueSubscriptionMap.get(destination) == null) {
                    return Collections.emptyList();
                } else {
                    return new ArrayList<AndesSubscription>(clusterQueueSubscriptionMap
                                                                    .get(destination));
                }
            }
        } else {
            if (isTopic) {
                if (clusterTopicSubscriptionMap.get(destination) == null) {
                    return Collections.emptyList();
                } else {
                    return new ArrayList<AndesSubscription>(clusterTopicSubscriptionMap
                                                                    .get(destination));
                }
            } else {
                if (clusterQueueSubscriptionMap.get(destination) == null) {

                    return Collections.emptyList();
                } else {
                    return new ArrayList<AndesSubscription>(clusterQueueSubscriptionMap
                                                                    .get(destination));
                }
            }
        }
    }

    /**
     * get all CLUSTER queues/topics where subscriptions are available
     *
     * @param isTopic TRUE if checking topics
     * @return list of queues/topics
     */
    public List<String> getAllDestinationsOfSubscriptions(boolean isTopic) {
        if (isBitmap)
            return new ArrayList<String>(isTopic ? subscriptionBitMapHandler.getAllDestinationsOfSubscriptions() : clusterQueueSubscriptionMap.keySet());
        else
            return new ArrayList<String>(isTopic ? clusterTopicSubscriptionMap.keySet() : clusterQueueSubscriptionMap.keySet());
    }

    /**
     * get all (ACTIVE/INACTIVE) CLUSTER subscription entries subscribed for a queue/topic
     * hierarchical topic subscription mapping also happens here
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @return list of andes subscriptions
     * @throws AndesException
     */
    public List<AndesSubscription> getClusterSubscribersForDestination(String destination,
                                                                       boolean isTopic) throws AndesException {
        List<AndesSubscription> subscriptionList = new ArrayList<AndesSubscription>();

        if (isTopic) {
            // In topic scenario if this is a durable topic it's in cluster queue subscription map,
            // hence we need to check both maps
            if (isBitmap) {
                subscriptionList.addAll(subscriptionBitMapHandler.findMatchingClusteredSubscriptions(destination));
            } else {
                subscriptionList.addAll(getSubscriptionsInMap(destination,
                                                              clusterTopicSubscriptionMap, SUBSCRIPTION_TYPE.ALL));
            }

            // Get durable topic subscriptions from Queue map
            subscriptionList.addAll(getSubscriptionsInMap(destination,
                                                          clusterQueueSubscriptionMap, SUBSCRIPTION_TYPE.TOPIC_SUBSCRIPTION));
        } else {
            subscriptionList = getSubscriptionsInMap(destination,
                                                     clusterQueueSubscriptionMap, SUBSCRIPTION_TYPE.QUEUE_SUBSCRIPTION);
        }

        return subscriptionList;
    }

    /**
     * Get subscriptions related to destination. Get hierarchical topic scenario into consideration
     *
     * @param destination          queue topic
     * @param subMap               Map<String, List<AndesSubscription>>
     * @param filterBySubscription filter results by subscription type
     * @return List<AndesSubscription>
     */
    private List<AndesSubscription> getSubscriptionsInMap(String destination,
                                                          Map<String, List<AndesSubscription>> subMap,
                                                          SUBSCRIPTION_TYPE filterBySubscription) {
        List<AndesSubscription> subscriptionList = new ArrayList<AndesSubscription>();
        for (Map.Entry<String, List<AndesSubscription>> entry : subMap.entrySet()) {
            String subDestination = entry.getKey();
            if (AMQPUtils.isTargetQueueBoundByMatchingToRoutingKey(subDestination, destination)
                || MQTTUtils.isTargetQueueBoundByMatchingToRoutingKey(subDestination, destination)) {
                List<AndesSubscription> subscriptionsOfDestination = entry.getValue();
                if (null != subscriptionsOfDestination) {

                    switch (filterBySubscription) {
                        case TOPIC_SUBSCRIPTION:
                            // Check for durable topic subscriptions and add them
                            for (AndesSubscription andesSubscription : subscriptionsOfDestination) {
                                if (andesSubscription.isBoundToTopic() && andesSubscription.isDurable()) {
                                    subscriptionList.add(andesSubscription);
                                }
                            }
                            break;
                        case QUEUE_SUBSCRIPTION:
                            // Check for queue subscriptions and add them
                            for (AndesSubscription andesSubscription : subscriptionsOfDestination) {
                                if (!andesSubscription.isBoundToTopic()) {
                                    subscriptionList.add(andesSubscription);
                                }
                            }
                            break;
                        default:
                            subscriptionList.addAll(subscriptionsOfDestination);
                            break;
                    }

                }
            }
        }
        return subscriptionList;
    }

    /**
     * get all ACTIVE LOCAL subscription entries subscribed for a destination/topic
     * Hierarchical topic mapping is NOT considered here
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @return list of matching subscriptions
     */
    public Collection<LocalSubscription> getActiveLocalSubscribers(String destination, boolean isTopic) throws AndesException {
        Map<String, LocalSubscription> localSubscriptionMap = getLocalSubscriptionMap(destination, isTopic);
        Collection<LocalSubscription> list = new ArrayList<LocalSubscription>();
        if (localSubscriptionMap != null) {
            list = getLocalSubscriptionMap(destination, isTopic).values();
        }
        Collection<LocalSubscription> activeLocalSubscriptionList = new ArrayList<LocalSubscription>();
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
    public Collection<LocalSubscription> getActiveLocalSubscribersForQueuesAndTopics(String destination) throws AndesException {
        Collection<LocalSubscription> allSubscriptions = getActiveLocalSubscribers(destination, false);
        allSubscriptions.addAll(getActiveLocalSubscribers(destination, true));
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
    public List<AndesSubscription> getActiveClusterSubscribersForNode(String nodeID, boolean isTopic) {
        List<AndesSubscription> activeQueueSubscriptions = new ArrayList<AndesSubscription>();
        if (isBitmap) {
            if (!isTopic) {
                for (String destination : clusterQueueSubscriptionMap.keySet()) {
                    List<AndesSubscription> subList = clusterQueueSubscriptionMap.get(destination);
                    for (AndesSubscription sub : subList) {
                        if (sub.getSubscribedNode().equals(nodeID) && sub.hasExternalSubscriptions()) {
                            activeQueueSubscriptions.add(sub);
                        }
                    }
                }
            } else {
                Collection<Map<String, AndesSubscription>> map = subscriptionBitMapHandler.getClusteredSubscriptions();
                for (int i = 0; i < map.size(); i++) {
                    Iterator<Map<String, AndesSubscription>> iterator = map.iterator();

                    while (iterator.hasNext()) {
                        Map<String, AndesSubscription> andes = iterator.next();
                        List<AndesSubscription> subList = new ArrayList<AndesSubscription>(andes.values());
                        for (AndesSubscription sub : subList) {
                            if (sub.getSubscribedNode().equals(nodeID) && sub.hasExternalSubscriptions())
                                activeQueueSubscriptions.add(sub);
                        }

                    }
                }
            }
        } else {
            Map<String, List<AndesSubscription>> clusterSubscriptionMap = isTopic ? clusterTopicSubscriptionMap : clusterQueueSubscriptionMap;
            for (String destination : clusterSubscriptionMap.keySet()) {
                List<AndesSubscription> subList = clusterSubscriptionMap.get(destination);
                for (AndesSubscription sub : subList) {
                    if (sub.getSubscribedNode().equals(nodeID) && sub.hasExternalSubscriptions()) {
                        activeQueueSubscriptions.add(sub);
                    }
                }
            }
        }
        return activeQueueSubscriptions;
    }

    /**
     * get all ACTIVE LOCAL subscriptions for any queue/topic
     *
     * @param isTopic TRUE if checking topics
     * @return list of Local subscriptions
     */
    public List<LocalSubscription> getActiveLocalSubscribers(boolean isTopic) {
        List<LocalSubscription> activeQueueSubscriptions = new ArrayList<LocalSubscription>();
        if (isBitmap) {
            if (!isTopic) {
                for (String destination : localQueueSubscriptionMap.keySet()) {
                    Map<String, LocalSubscription> subMap = localQueueSubscriptionMap.get(destination);
                    for (String subID : subMap.keySet()) {
                        LocalSubscription sub = subMap.get(subID);
                        if (sub.hasExternalSubscriptions()) {
                            activeQueueSubscriptions.add(sub);
                        }
                    }
                }
            } else {

                Collection<Map<String, LocalSubscription>> map = subscriptionBitMapHandler.getLocalSubscriptions();
                for (int i = 0; i < map.size(); i++) {
                    Iterator<Map<String, LocalSubscription>> iterator = map.iterator();

                    while (iterator.hasNext()) {
                        Map<String, LocalSubscription> local = iterator.next();
                        List<LocalSubscription> subList = new ArrayList<LocalSubscription>(local.values());
                        for (LocalSubscription sub : subList) {
                            if (sub.hasExternalSubscriptions()) {
                                activeQueueSubscriptions.add(sub);
                            }
                        }
                    }
                }
            }
        } else {
            Map<String, Map<String, LocalSubscription>> localSubscriptionMap = isTopic ? localTopicSubscriptionMap : localQueueSubscriptionMap;
            for (String destination : localSubscriptionMap.keySet()) {
                Map<String, LocalSubscription> subMap = localSubscriptionMap.get(destination);
                for (String subID : subMap.keySet()) {
                    LocalSubscription sub = subMap.get(subID);
                    if (sub.hasExternalSubscriptions()) {
                        activeQueueSubscriptions.add(sub);
                    }
                }
            }
        }
        return activeQueueSubscriptions;
    }

    /**
     * get number of active subscribers for queue/topic in CLUSTER
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @return number of subscriptions in cluster
     * @throws AndesException
     */
    public int numberOfSubscriptionsInCluster(String destination, boolean isTopic) throws
                                                                                   AndesException {
        return getClusterSubscribersForDestination(destination, isTopic).size();
    }

    /**
     * get a copy of local subscription list for a given queue/topic
     * hierarchical topic subscription mapping is NOT considered here
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @return a map of <SubID,subscription>
     */
    public Map<String, LocalSubscription> getLocalSubscriptionMap(String destination,
                                                                  boolean isTopic) {
        if (isBitmap) {
            if (!isTopic)
                return localQueueSubscriptionMap.get(destination);
            return subscriptionBitMapHandler.getAllLocalSubscribedForDestination(destination);
        } else {
            Map<String, Map<String, LocalSubscription>> subscriptionMap = isTopic ? localTopicSubscriptionMap : localQueueSubscriptionMap;
            return subscriptionMap.get(destination);
        }
    }

    /**
     * get all (active/inactive) CLUSTER subscriptions for a queue/topic
     *
     * @param destination queue/topic name
     * @param isTopic     TRUE if checking topics
     * @return list of subscriptions
     */
    public List<AndesSubscription> getClusterSubscriptionList(String destination, boolean isTopic) {
        if (isBitmap) {
            if (!isTopic)
                return clusterQueueSubscriptionMap.get(destination);
            return subscriptionBitMapHandler.getAllClusteredSubscribedForDestination(destination);
        } else {
            Map<String, List<AndesSubscription>> subscriptionMap = isTopic ? clusterTopicSubscriptionMap : clusterQueueSubscriptionMap;
            return subscriptionMap.get(destination);
        }

    }

    /**
     * get all ACTIVE CLUSTER subscriptions for a queue/topic. For topics this will return
     * subscriptions whose destination is exactly matching to the given destination only.
     * (hierarchical mapping not considered)
     *
     * @param destination queue or topic name
     * @param isTopic     is destination a topic
     * @return list of matching subscriptions
     */
    public List<AndesSubscription> getActiveClusterSubscriptionList(String destination, boolean isTopic) {
        List<AndesSubscription> activeSubscriptions = new ArrayList<AndesSubscription>();
        List<AndesSubscription> allSubscriptions = getClusterSubscriptionList(destination, isTopic);
        if (null != allSubscriptions) {
            for (AndesSubscription sub : allSubscriptions) {
                if (sub.hasExternalSubscriptions()) {
                    activeSubscriptions.add(sub);
                }
            }
        }
        return activeSubscriptions;
    }

    /**
     * replace the whole CLUSTER subscription list for a given queue/topic
     *
     * @param destination queue/topic name
     * @param newSubList  new subscription list
     * @param isTopic     TRUE if checking topics
     * @return old CLUSTER subscription list
     */
    public List<AndesSubscription> replaceClusterSubscriptionListOfDestination(String destination, List<AndesSubscription> newSubList, boolean isTopic) {
        List<AndesSubscription> oldSubscriptionList;
        if (isBitmap) {
            if (!isTopic) {
                oldSubscriptionList = clusterQueueSubscriptionMap.put(destination, newSubList);
                if (oldSubscriptionList != null) {
                    return new ArrayList<AndesSubscription>(oldSubscriptionList);
                } else {
                    return new ArrayList<AndesSubscription>();
                }
            } else {
                return subscriptionBitMapHandler.getAllClusteredSubscriptions(destination, newSubList);
            }
        } else {
            Map<String, List<AndesSubscription>> subscriptionMap = isTopic ? clusterTopicSubscriptionMap : clusterQueueSubscriptionMap;
            oldSubscriptionList = subscriptionMap.put(destination, newSubList);
            if (oldSubscriptionList != null) {
                return new ArrayList<AndesSubscription>(oldSubscriptionList);
            } else {
                return new ArrayList<AndesSubscription>();
            }
        }
    }

    /**
     * Get ALL (ACTIVE + INACTIVE) local subscriptions whose bound queue is given
     *
     * @param queueName Queue name to search
     * @return List if matching subscriptions
     * @throws AndesException
     */
    public List<LocalSubscription> getListOfLocalSubscriptionsBoundToQueue(String queueName) throws AndesException {
        List<LocalSubscription> subscriptionsOfQueue = new ArrayList<LocalSubscription>();
        Map<String, LocalSubscription> queueSubscriptionMap = localQueueSubscriptionMap.get(queueName);
        if (queueSubscriptionMap != null) {
            subscriptionsOfQueue.addAll(queueSubscriptionMap.values());
        }
        if (isBitmap) {
            Collection<Map<String, LocalSubscription>> topicSubscriptions = subscriptionBitMapHandler.getLocalSubscriptions();

            for (Iterator<Map<String, LocalSubscription>> iterator = topicSubscriptions.iterator(); iterator.hasNext(); ) {
                Map<String, LocalSubscription> topicSubscription = iterator.next();
                List<LocalSubscription> localSubscriptions = new ArrayList<LocalSubscription>(topicSubscription.values());

                for (LocalSubscription localSubscription : localSubscriptions) {
                    if (localSubscription.getTargetQueue().equals(queueName))
                        subscriptionsOfQueue.add(localSubscription);
                }
            }
        } else {
            Map<String, Map<String, LocalSubscription>> topicSubscriptionMap = localTopicSubscriptionMap;
            for (String destination : topicSubscriptionMap.keySet()) {
                Map<String, LocalSubscription> topicSubsOfDest = topicSubscriptionMap.get(destination);
                if (topicSubsOfDest != null) {
                    for (String subID : topicSubsOfDest.keySet()) {
                        LocalSubscription sub = topicSubsOfDest.get(subID);
                        if (sub.getTargetQueue().equals(queueName)) {
                            subscriptionsOfQueue.add(sub);
                        }
                    }
                }
            }
        }
        return subscriptionsOfQueue;
    }

    /**
     * create disconnect or remove a cluster subscription entry
     *
     * @param subscription subscription to add disconnect or remove
     * @param type         type of change
     */
    public synchronized void createDisconnectOrRemoveClusterSubscription(AndesSubscription subscription, SubscriptionChange type) throws AndesException {
        if (isBitmap) {
            createDisconnectOrRemoveClusterSubscriptionUsingBitmap(subscription, type);
        } else {
            createDisconnectOrRemoveClusterSubscriptionUsingNativeMethod(subscription, type);
        }

        if (log.isDebugEnabled()) {
            log.debug("\n\tUpdated cluster subscription maps\n");
            this.printSubscriptionMap(clusterQueueSubscriptionMap);
            this.printSubscriptionMap(clusterTopicSubscriptionMap);
            log.debug("\n");
        }

    }

    /**
     * create disconnect or remove a cluster subscription entry using native method
     *
     * @param subscription subscription to add disconnect or remove
     * @param type         tye pf change
     * @throws AndesException
     */
    private synchronized void createDisconnectOrRemoveClusterSubscriptionUsingNativeMethod(AndesSubscription subscription, SubscriptionChange type) throws AndesException {
        Map<String, List<AndesSubscription>> clusterSubscriptionMap;
        if (subscription.isBoundToTopic()) {
            if (subscription.isDurable()) {
                // Treat durable subscription for topic as a queue subscription. Therefore its in
                // cluster queue subscription map
                clusterSubscriptionMap = clusterQueueSubscriptionMap;
            } else { // Topics
                clusterSubscriptionMap = clusterTopicSubscriptionMap;
            }
        } else { // Queues
            clusterSubscriptionMap = clusterQueueSubscriptionMap;
        }
        String destination = subscription.getSubscribedDestination();
        List<AndesSubscription> subscriptionList = clusterSubscriptionMap.get(destination);

        if (type == SubscriptionChange.ADDED) {
            if (subscriptionList != null) {
                //iterate and remove all similar subscriptions
                //TODO: hasitha - wrap this list by a map to reduce cost
                Iterator itr = subscriptionList.iterator();
                while (itr.hasNext()) {
                    AndesSubscription sub = (AndesSubscription) itr.next();
                    if (sub.equals(subscription)) {
                        itr.remove();
                    }
                }
                subscriptionList.add(subscription);

            } else {
                subscriptionList = new ArrayList<AndesSubscription>();
                subscriptionList.add(subscription);
                clusterSubscriptionMap.put(destination, subscriptionList);
            }
            log.debug("Added Subscription to map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());

        } else if (type == SubscriptionChange.DISCONNECTED) {
            if (subscriptionList == null) {
                subscriptionList = new ArrayList<AndesSubscription>();
            }
            Iterator itr = subscriptionList.iterator();
            while (itr.hasNext()) {
                AndesSubscription sub = (AndesSubscription) itr.next();
                if (sub.equals(subscription)) {
                    itr.remove();
                    break;
                }
            }
            subscriptionList.add(subscription);
            clusterSubscriptionMap.put(destination, subscriptionList);

            log.debug("Disconnected Subscription from map: " + subscription.encodeAsStr());

        } else if (type == SubscriptionChange.DELETED) {
            if (subscriptionList == null) {
                subscriptionList = new ArrayList<AndesSubscription>();
            }
            Iterator itr = subscriptionList.iterator();
            while (itr.hasNext()) {
                AndesSubscription sub = (AndesSubscription) itr.next();
                if (sub.equals(subscription)) {
                    itr.remove();
                    break;
                }
            }
            if (subscriptionList.size() == 0) {
                clusterSubscriptionMap.remove(destination);
            }
            log.debug("DELETED Subscription from map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());
        }

    }

    /**
     * create disconnect or remove a cluster subscription entry using bitmap method
     *
     * @param subscription subscription to add disconnect or remove
     * @param type         tyepe change
     * @throws AndesException
     */
    private synchronized void createDisconnectOrRemoveClusterSubscriptionUsingBitmap(AndesSubscription subscription, SubscriptionChange type) throws AndesException {
        Map<String, List<AndesSubscription>> clusterSubscriptionMap;
        boolean queuMap = true;
        List<AndesSubscription> subscriptionList;
        if (subscription.isBoundToTopic()) {
            if (subscription.isDurable()) {
                // Treat durable subscription for topic as a queue subscription. Therefore its in
                // cluster queue subscription map
                queuMap = true;
            } else { // Topics
                queuMap = false;
            }
        } else { // Queues
            queuMap = true;
        }

        String destination = subscription.getSubscribedDestination();
        if (queuMap) {
            subscriptionList = clusterQueueSubscriptionMap.get(destination);

            if (type == SubscriptionChange.ADDED) {
                if (subscriptionList != null) {
                    //iterate and remove all similar subscriptions
                    //TODO: hasitha - wrap this list by a map to reduce cost
                    Iterator itr = subscriptionList.iterator();
                    while (itr.hasNext()) {
                        AndesSubscription sub = (AndesSubscription) itr.next();
                        if (sub.equals(subscription)) {
                            itr.remove();
                        }
                    }
                    subscriptionList.add(subscription);

                } else {
                    subscriptionList = new ArrayList<AndesSubscription>();
                    subscriptionList.add(subscription);
                    clusterQueueSubscriptionMap.put(destination, subscriptionList);
                }
                log.debug("Added Subscription to map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());

            } else if (type == SubscriptionChange.DISCONNECTED) {
                if (subscriptionList == null) {
                    subscriptionList = new ArrayList<AndesSubscription>();
                }
                Iterator itr = subscriptionList.iterator();
                while (itr.hasNext()) {
                    AndesSubscription sub = (AndesSubscription) itr.next();
                    if (sub.equals(subscription)) {
                        itr.remove();
                        break;
                    }
                }
                subscriptionList.add(subscription);
                clusterQueueSubscriptionMap.put(destination, subscriptionList);

                log.debug("Disconnected Subscription from map: " + subscription.encodeAsStr());

            } else if (type == SubscriptionChange.DELETED) {
                if (subscriptionList == null) {
                    subscriptionList = new ArrayList<AndesSubscription>();
                }
                Iterator itr = subscriptionList.iterator();
                while (itr.hasNext()) {
                    AndesSubscription sub = (AndesSubscription) itr.next();
                    if (sub.equals(subscription)) {
                        itr.remove();
                        break;
                    }
                }
                if (subscriptionList.size() == 0) {
                    clusterQueueSubscriptionMap.remove(destination);
                }
                if (log.isDebugEnabled()) {
                    log.debug("DELETED Subscription from map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());
                }
            }
        } else {
            subscriptionList = subscriptionBitMapHandler.getAllClusteredSubscribedForDestination(destination);

            if (type == SubscriptionChange.ADDED) {
                if (subscriptionList != null) {
                    //iterate and remove all similar subscriptions
                    //TODO: hasitha - wrap this list by a map to reduce cost
                    Iterator itr = subscriptionList.iterator();
                    while (itr.hasNext()) {
                        AndesSubscription sub = (AndesSubscription) itr.next();
                        if (sub.equals(subscription)) {
                            subscriptionBitMapHandler.removeClusteredSubscription(sub.getSubscriptionID());
                        }
                    }
                    subscriptionBitMapHandler.addClusteredSubscription(subscription.getSubscribedDestination(), subscription);

                } else {
                    subscriptionBitMapHandler.addClusteredSubscription(subscription.getSubscribedDestination(), subscription);
                }
                if (log.isDebugEnabled())
                    log.debug("Added Subscription to map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());

            } else if (type == SubscriptionChange.DISCONNECTED) {
                if (subscriptionList == null) {
                    subscriptionList = new ArrayList<AndesSubscription>();
                }
                Iterator itr = subscriptionList.iterator();
                while (itr.hasNext()) {
                    AndesSubscription sub = (AndesSubscription) itr.next();
                    if (sub.equals(subscription)) {
                        subscriptionBitMapHandler.removeClusteredSubscription(sub.getSubscriptionID());
                        break;
                    }
                }
                subscriptionBitMapHandler.addClusteredSubscription(subscription.getSubscribedDestination(), subscription);

                if (log.isDebugEnabled())
                    log.debug("Disconnected Subscription from map: " + subscription.encodeAsStr());

            } else if (type == SubscriptionChange.DELETED) {
                if (subscriptionList == null) {
                    subscriptionList = new ArrayList<AndesSubscription>();
                }
                Iterator itr = subscriptionList.iterator();
                while (itr.hasNext()) {
                    AndesSubscription sub = (AndesSubscription) itr.next();
                    if (sub.equals(subscription)) {
                        subscriptionBitMapHandler.removeClusteredSubscription(sub.getSubscriptionID());
                        break;
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("DELETED Subscription from map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());
            }


        }

    }

    /**
     * To print the subscription maps with the destinations
     *
     * @param map Map to be printed
     */
    private void printSubscriptionMap(Map<String, List<AndesSubscription>> map) {
        for (Entry<String, List<AndesSubscription>> entry : map.entrySet()) {
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
            for (Entry<String, List<AndesSubscription>> entry : clusterQueueSubscriptionMap.entrySet()) {
                List<AndesSubscription> existingSubscriptions = entry.getValue();
                if (existingSubscriptions != null && !existingSubscriptions.isEmpty()) {
                    for (AndesSubscription sub : existingSubscriptions) {
                        // Queue is durable and target queues are matched
                        if (sub.isDurable() && sub.getTargetQueue().equals(subscription
                                                                                   .getTargetQueue())) {
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
                    throw new AndesException("There is no active subscriber to close subscribed to " + subscription.
                            getSubscribedDestination() + " with the queue " + subscription.getTargetQueue());
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
            //Store the subscription
            String destinationIdentifier = (subscription.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX) + destinationQueue;
            String subscriptionID = subscription.getSubscribedNode() + "_" + subscription.getSubscriptionID();

            if (type == SubscriptionChange.ADDED && !durableSubExists) {
                andesContextStore.storeDurableSubscription(destinationIdentifier, subscriptionID, subscription
                        .encodeAsStr());
                log.info("New local subscription " + type + " " + subscription.toString());
            } else {
                andesContextStore.updateDurableSubscription(destinationIdentifier, subscriptionID, subscription
                        .encodeAsStr());
                log.info("New local subscription " + type + " " + subscription.toString());
            }

            //add or update local subscription map
            if (subscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.DIRECT_EXCHANGE_NAME)) {
                Map<String, LocalSubscription> localSubscriptions = localQueueSubscriptionMap.get(destinationQueue);
                if (localSubscriptions == null) {
                    localSubscriptions = new ConcurrentHashMap<String, LocalSubscription>();
                }
                localSubscriptions.put(subscriptionID, subscription);
                localQueueSubscriptionMap.put(destinationQueue, localSubscriptions);

            } else if (subscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME)) {
                if (isBitmap) {
                    subscriptionBitMapHandler.addLocalSubscription(destinationQueue, subscription);
                } else {
                    Map<String, LocalSubscription> localSubscriptions = localTopicSubscriptionMap.get(destinationQueue);
                    if (localSubscriptions == null) {
                        localSubscriptions = new ConcurrentHashMap<String, LocalSubscription>();
                    }
                    localSubscriptions.put(subscriptionID, subscription);
                    localTopicSubscriptionMap.put(destinationQueue, localSubscriptions);
                }
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
        Map<String, LocalSubscription> subscriptionList = getLocalSubscriptionMap(destination, false);
        if (null != subscriptionList) {
            Iterator<LocalSubscription> iterator = subscriptionList.values().iterator();
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
                Iterator<LocalSubscription> iterator = subscriptionList.values().iterator();
                if (isBitmap) {
                    while (iterator.hasNext()) {
                        LocalSubscription currentSubscription = iterator.next();
                        if (currentSubscription.equals(subscription)) {
                            subscriptionToRemove = currentSubscription;

                            subscriptionBitMapHandler.removeLocalSubscription(subscriptionToRemove.getSubscriptionID());
                            break;
                        }
                    }

                } else {
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
        }
        if (null != subscriptionToRemove) {
            String destinationIdentifier = new StringBuffer().append((subscriptionToRemove
                                                                              .isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX))
                    .append(destination).toString();
            andesContextStore.removeDurableSubscription(destinationIdentifier, subscription.getSubscribedNode() + "_" + subscriptionID);
            if (log.isDebugEnabled())
                log.debug("Subscription Removed Locally for  " + destination + "@" + subscriptionID + " " + subscriptionToRemove);
        } else {
            throw new AndesException("Could not find an subscription ID " + subscriptionID + " under destination " + destination);
        }
        return subscriptionToRemove;
    }

    /**
     * @return list of ACTIVE and INACTIVE topics in cluster
     */
    public List<String> getTopics() {
        if (isBitmap)
            return new ArrayList<String>(subscriptionBitMapHandler.getAllDestinationsOfSubscriptions());
        else
            return new ArrayList<String>(clusterTopicSubscriptionMap.keySet());

    }

    /**
     * Enum to identify subscription type
     */
    private enum SUBSCRIPTION_TYPE {
        QUEUE_SUBSCRIPTION,
        TOPIC_SUBSCRIPTION,
        ALL
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
}
