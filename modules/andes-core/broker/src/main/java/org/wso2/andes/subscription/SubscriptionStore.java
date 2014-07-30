package org.wso2.andes.subscription;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.SubscriptionListener.SubscriptionChange;
import org.wso2.andes.server.util.AndesUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionStore {
    private static final String TOPIC_PREFIX = "topic.";


    private static final String QUEUE_PREFIX = "queue.";


    private static Log log = LogFactory.getLog(SubscriptionStore.class);

    //<routing key, List of local subscriptions>
    private Map<String, List<AndesSubscription>> clusterTopicSubscriptionMap = new ConcurrentHashMap<String, List<AndesSubscription>>();
    private Map<String, List<AndesSubscription>> clusterQueueSubscriptionMap = new ConcurrentHashMap<String, List<AndesSubscription>>();

    //<destination, <subscriptionID,LocalSubscription>>
    private Map<String, Map<String, LocalSubscription>> localTopicSubscriptionMap = new ConcurrentHashMap<String, Map<String, LocalSubscription>>();
    private Map<String, Map<String, LocalSubscription>> localQueueSubscriptionMap = new ConcurrentHashMap<String, Map<String, LocalSubscription>>();


    private AndesContextStore andesContextStore;


    public SubscriptionStore() throws AndesException {

        andesContextStore = AndesContext.getInstance().getAndesContextStore();
    }

    public List<AndesSubscription> getAllSubscribersForDestination(String destination, boolean isTopic) throws AndesException {
        return (isTopic ? clusterTopicSubscriptionMap.get(destination) : clusterQueueSubscriptionMap.get(destination));
    }


    public List<AndesSubscription> getActiveClusterSubscribersForDestination(String destination, boolean isTopic) throws AndesException {
        List<AndesSubscription> list = isTopic ? clusterTopicSubscriptionMap.get(destination) : clusterQueueSubscriptionMap.get(destination);
        List<AndesSubscription> subscriptionsHavingExternalsubscriber = new ArrayList<AndesSubscription>();
        if (list != null) {
            for (AndesSubscription subscription : list) {
                if (subscription.hasExternalSubscriptions()) {
                    subscriptionsHavingExternalsubscriber.add(subscription);
                }
            }
        }
        return subscriptionsHavingExternalsubscriber;
    }

    public Collection<LocalSubscription> getActiveLocalSubscribersForTopic(String topic) throws AndesException {
        Collection<LocalSubscription> list = getLocalSubscriptionList(topic, true).values();
        Collection<LocalSubscription> activeLocalSubscriptionList = new ArrayList<LocalSubscription>();
        for (LocalSubscription localSubscription : list) {
            if (localSubscription.hasExternalSubscriptions()) {
                activeLocalSubscriptionList.add(localSubscription);
            }
        }
        return activeLocalSubscriptionList;
    }

    public Collection<LocalSubscription> getActiveLocalSubscribersForQueue(String queue) {
        Collection<LocalSubscription> list = getLocalSubscriptionList(queue, false).values();
        Collection<LocalSubscription> activeLocalSubscriptionList = new ArrayList<LocalSubscription>();
        for (LocalSubscription localSubscription : list) {
            if (localSubscription.hasExternalSubscriptions()) {
                activeLocalSubscriptionList.add(localSubscription);
            }
        }
        return activeLocalSubscriptionList;
    }


    public int numberOfSubscriptionsForQueueAtNode(String queueName, String nodeID) throws AndesException {
        String requestedNodeQueue = AndesUtils.getNodeQueueNameForNodeId(nodeID);
        List<AndesSubscription> subscriptions = getActiveClusterSubscribersForDestination(queueName, false);
        int count = 0;
        if (subscriptions != null && !subscriptions.isEmpty()) {
            for (AndesSubscription sub : subscriptions) {
                if (sub.getSubscribedNode().equals(requestedNodeQueue)) {
                    count++;
                }
            }
        }
        return count;
    }

    public List<AndesSubscription> getActiveClusterSubscribersForNode(String nodeID, boolean isTopic) {
        List<AndesSubscription> activeQueueSubscriptions = new ArrayList<AndesSubscription>();
        String nodeQueueNameForNode = AndesUtils.getNodeQueueNameForNodeId(nodeID);
        Map<String, List<AndesSubscription>> clusterSubscriptionMap = isTopic ? clusterTopicSubscriptionMap : clusterQueueSubscriptionMap;
        for (String destination : clusterSubscriptionMap.keySet()) {
            List<AndesSubscription> subList = clusterSubscriptionMap.get(destination);
            for (AndesSubscription sub : subList) {
                if (sub.getSubscribedNode().equals(nodeQueueNameForNode) && sub.hasExternalSubscriptions()) {
                    activeQueueSubscriptions.add(sub);
                }
            }
        }

        return activeQueueSubscriptions;
    }

    public List<LocalSubscription> getActiveLocalSubscribersForNode(String nodeID, boolean isTopic) {
        List<LocalSubscription> activeQueueSubscriptions = new ArrayList<LocalSubscription>();
        String nodeQueueNameForNode = AndesUtils.getNodeQueueNameForNodeId(nodeID);
        Map<String, Map<String, LocalSubscription>> localSubscriptionMap = isTopic ? localTopicSubscriptionMap : localTopicSubscriptionMap;
        for (String destination : localSubscriptionMap.keySet()) {
            Map<String, LocalSubscription> subMap = localSubscriptionMap.get(destination);
            for (String subID : subMap.keySet()) {
                LocalSubscription sub = subMap.get(subID);
                if (sub.getSubscribedNode().equals(nodeQueueNameForNode) && sub.hasExternalSubscriptions()) {
                    activeQueueSubscriptions.add(sub);
                }
            }
        }

        return activeQueueSubscriptions;
    }

    public int numberOfSubscriptionsForQueueInCluster(String queueName) throws AndesException {
        return getActiveClusterSubscribersForDestination(queueName, false).size();
    }

    public Map<String, LocalSubscription> getLocalSubscriptionList(String destination, boolean isTopic) {
        Map<String, Map<String, LocalSubscription>> subscriptionMap = isTopic ? localTopicSubscriptionMap : localQueueSubscriptionMap;
        Map<String, LocalSubscription> list = subscriptionMap.get(destination);
        if (list == null) {
            list = new ConcurrentHashMap<String, LocalSubscription>();
            subscriptionMap.put(destination, list);
        }
        return list;
    }

    public ArrayList<String> getQueueListHavingSubscriptions(){
        ArrayList<String> queueList = new ArrayList<String>();
        if(!localQueueSubscriptionMap.keySet().isEmpty()){
            queueList = new ArrayList<String>(localQueueSubscriptionMap.keySet());
        }
        return queueList;

    }

    public List<AndesSubscription> getClusterSubscriptionList(String destination, boolean isTopic) {
        Map<String, List<AndesSubscription>> subscriptionMap = isTopic ? clusterTopicSubscriptionMap : clusterQueueSubscriptionMap;
        return subscriptionMap.get(destination);
    }

    public List<AndesSubscription> replaceClusterSubscriptionListOfDestination(String destination, List<AndesSubscription> newSubList, boolean isTopic) {
        Map<String, List<AndesSubscription>> subscriptionMap = isTopic ? clusterTopicSubscriptionMap : clusterQueueSubscriptionMap;
        log.info("===============Updated cluster subscription maps================");
        this.printSubscriptionMap(clusterQueueSubscriptionMap);
        this.printSubscriptionMap(clusterTopicSubscriptionMap);
        log.info("========================================================");
        return subscriptionMap.put(destination, newSubList);
    }

    public List<String> listQueues() {
        List<String> queues = new ArrayList<String>();
        for (AndesQueue queue : getDurableQueues()) {
            queues.add(queue.queueName);
        }
        return queues;
    }

    /**
     * get a List of nodes having subscriptions to the given destination queue
     *
     * @param queueName destination queue name
     * @return list of nodes
     */
    public Set<String> getNodeQueuesHavingSubscriptionsForQueue(String queueName) throws AndesException {
        List<AndesSubscription> nodesHavingSubscriptions4Queue = getActiveClusterSubscribersForDestination(queueName, false);
        HashSet<String> nodes = new HashSet<String>();
        for (AndesSubscription subscrption : nodesHavingSubscriptions4Queue) {
            nodes.add(subscrption.getSubscribedNode());
        }
        return nodes;
    }

    public Set<String> getNodeQueuesHavingSubscriptionsForTopic(String topicName) throws AndesException {
        List<AndesSubscription> nodesHavingSubscriptions4Topic = getActiveClusterSubscribersForDestination(topicName, true);
        HashSet<String> nodes = new HashSet<String>();
        for (AndesSubscription subscrption : nodesHavingSubscriptions4Topic) {
            nodes.add(subscrption.getSubscribedNode());
        }
        return nodes;
    }


    private List<AndesSubscription> getSubscriptionsOfNode(String nodeQueueName, List<AndesSubscription> subscriptionList) {
        List<AndesSubscription> subscriptionsOfNode = new ArrayList<AndesSubscription>();
        for (AndesSubscription sub : subscriptionList) {
            if (sub.getSubscribedNode().equals(nodeQueueName)) {
                subscriptionsOfNode.add(sub);
            }
        }
        return subscriptionsOfNode;
    }


    public Map<String, Integer> getSubscriptionCountInformation(String destination, boolean isTopic) throws AndesException {

        Map<String, Integer> nodeSubscriptionCountMap = new HashMap<String, Integer>();
        List<AndesSubscription> subscriptions = getActiveClusterSubscribersForDestination(destination, isTopic);
        for (AndesSubscription sub : subscriptions) {
            Integer count = nodeSubscriptionCountMap.get(sub.getSubscribedNode());
            if (count == null) {
                nodeSubscriptionCountMap.put(sub.getSubscribedNode(), 1);
            } else {
                nodeSubscriptionCountMap.put(sub.getSubscribedNode(), count + 1);
            }
        }
        return nodeSubscriptionCountMap;
    }

    /**
     * create disconnect or remove a cluster subscription entry
     *
     * @param subscription subscription to add disconnect or remove
     * @param type         type of change
     */
    public void createDisconnectOrRemoveClusterSubscription(AndesSubscription subscription, SubscriptionChange type) {

        boolean isTopic = subscription.isBoundToTopic();
        Map<String, List<AndesSubscription>> clusterSubscriptionMap = isTopic ? clusterTopicSubscriptionMap : clusterQueueSubscriptionMap;
        String destination = subscription.getSubscribedDestination();
        List<AndesSubscription> subscriptionList = clusterSubscriptionMap.get(destination);

        if (type == SubscriptionChange.Added) {
            if (subscriptionList != null) {
                boolean duplicate = false;
                for (AndesSubscription s : subscriptionList) {
                    if (s.getSubscriptionID().equals(subscription.getSubscriptionID()) && s.getSubscribedNode().equals(subscription.getSubscribedNode())) {
                        duplicate = true;
                        subscriptionList.remove(s);
                        subscriptionList.add(subscription);
                        break;
                    }
                }
                if (!duplicate) {
                    subscriptionList.add(subscription);
                }
            } else {
                subscriptionList = new ArrayList<AndesSubscription>();
                subscriptionList.add(subscription);
                clusterSubscriptionMap.put(destination, subscriptionList);
            }

            if (log.isInfoEnabled()) {
                log.info("Added Subscription to map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());
            }

        } else if (type == SubscriptionChange.Disconnected) {
            if (subscriptionList == null) {
                subscriptionList = new ArrayList<AndesSubscription>();
            }
            for (AndesSubscription s : subscriptionList) {
                if (s.getSubscriptionID().equals(subscription.getSubscriptionID()) && s.getSubscribedNode().equals(subscription.getSubscribedNode())) {
                    subscriptionList.remove(s);
                    subscriptionList.add(subscription);
                    break;
                }
            }
            clusterSubscriptionMap.put(destination, subscriptionList);

            log.info("Disconnected Subscription from map: " + subscription.encodeAsStr());

        } else if (type == SubscriptionChange.Deleted) {
            for (AndesSubscription s : subscriptionList) {
                if (s.getSubscriptionID().equals(subscription.getSubscriptionID()) && s.getSubscribedNode().equals(subscription.getSubscribedNode())) {
                    subscriptionList.remove(s);
                }
            }
            if (subscriptionList.size() == 0) {
                clusterSubscriptionMap.remove(destination);
            }
            log.info("Deleted Subscription from map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());
        }

        log.info("===============Updated cluster subscription maps================");
        this.printSubscriptionMap(clusterQueueSubscriptionMap);
        this.printSubscriptionMap(clusterTopicSubscriptionMap);
        log.info("========================================================");
    }


    private void printSubscriptionMap(Map<String, List<AndesSubscription>> map) {
        for (Entry<String, List<AndesSubscription>> entry : map.entrySet()) {
            log.info("Destination: " + entry.getKey());
            for (AndesSubscription s : entry.getValue()) {
                log.info("---" + s.encodeAsStr());
            }
        }
    }

    private void printLocalSubscriptionMap(Map<String, Map<String, LocalSubscription>> map) {
        for (Entry<String, Map<String, LocalSubscription>> entry : map.entrySet()) {
            log.info("Destination: " + entry.getKey());
            Map<String, LocalSubscription> mapForDestination = entry.getValue();
            for (Entry<String, LocalSubscription> sub : mapForDestination.entrySet()) {
                log.info("\t SubID: " + sub.getKey());
                log.info("---" + sub.getValue().encodeAsStr());
            }
        }
    }

    public void createDisconnectOrRemoveLocalSubscription(LocalSubscription subscrption, SubscriptionChange type) throws AndesException {
        Map<String, LocalSubscription> subscriptionList = getLocalSubscriptionList(subscrption.getSubscribedDestination(),
                subscrption.isBoundToTopic());
        //TODO:hasitha- review this
        //We need to handle durable topic subscriptions
        boolean hasDurableSubscriptionAlreadyInPlace = false;
        if (subscrption.isBoundToTopic() && subscrption.isDurable()) {
            /**
             * Check if an active durable topic subscription already in place. If so we should not accept the subscription
             */
            List<AndesSubscription> existingSubscriptions = clusterTopicSubscriptionMap.get(subscrption.getSubscribedDestination());
            if (existingSubscriptions != null && !existingSubscriptions.isEmpty()) {
                for (AndesSubscription sub : existingSubscriptions) {
                    if (sub.isDurable() &&
                            sub.getTargetQueue().equals(subscrption.getTargetQueue()) &&
                            sub.hasExternalSubscriptions()) {
                        hasDurableSubscriptionAlreadyInPlace = true;
                        break;
                    }
                }
            }

            if (!hasDurableSubscriptionAlreadyInPlace && type == SubscriptionChange.Disconnected) {
                throw new AndesException("There is no active subscriber to close subscribed to " + subscrption.getSubscribedDestination() + " with the queue " + subscrption.getTargetQueue());
            } else if (hasDurableSubscriptionAlreadyInPlace && type == SubscriptionChange.Added) {
                //not permitted
                throw new AndesException("A subscription already exists for Durable subscriptions on " + subscrption.getSubscribedDestination() + " with the queue " + subscrption.getTargetQueue());
            }

        }

        if (type == SubscriptionChange.Added || type == SubscriptionChange.Disconnected) {
            //add or update subscription to local map
            subscriptionList.put(subscrption.getSubscriptionID(), subscrption);
            String destinationQueue = subscrption.getSubscribedDestination();
            //Store the subscription
            String destinationIdentifier = new StringBuffer().append((subscrption.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX))
                    .append(destinationQueue).toString();
            String subscriptionID = subscrption.getSubscribedNode() + "_" + subscrption.getSubscriptionID();
            andesContextStore.storeDurableSubscription(destinationIdentifier, subscriptionID, subscrption.encodeAsStr());

            if (type == SubscriptionChange.Added) {
                log.info("New Local Subscription Added " + subscrption.toString());
            } else {
                log.info("New Local Subscription Disconnected " + subscrption.toString());
            }

            //add or update local subscription map
            if (subscrption.getTargetQueueBoundExchangeName().equals(AMQPUtils.DIRECT_EXCHANGE_NAME)) {
                Map<String, LocalSubscription> localSubscriptions = localQueueSubscriptionMap.get(destinationQueue);
                if (localSubscriptions == null) {
                    localSubscriptions = new ConcurrentHashMap<String, LocalSubscription>();
                    andesContextStore.addMessageCounterForQueue(destinationQueue);
                }
                localSubscriptions.put(subscriptionID, subscrption);
                localQueueSubscriptionMap.put(destinationQueue, localSubscriptions);

            } else if (subscrption.getTargetQueueBoundExchangeName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME)) {
                Map<String, LocalSubscription> localSubscriptions = localTopicSubscriptionMap.get(destinationQueue);
                if (localSubscriptions == null) {
                    localSubscriptions = new ConcurrentHashMap<String, LocalSubscription>();
                }
                localSubscriptions.put(subscriptionID, subscrption);
                localTopicSubscriptionMap.put(destinationQueue, localSubscriptions);
            }

        } else if (type == SubscriptionChange.Deleted) {
            removeLocalSubscriptionAndNotify(subscrption);
        }

        log.info("===============Updated local subscription maps================");
        this.printLocalSubscriptionMap(localQueueSubscriptionMap);
        this.printLocalSubscriptionMap(localTopicSubscriptionMap);
        log.info("========================================================");

    }

    private LocalSubscription removeLocalSubscriptionAndNotify(AndesSubscription subscrption) throws AndesException {
        String destination = subscrption.getSubscribedDestination();
        String subscriptionID = subscrption.getSubscriptionID();
        //check queue local subscriptions
        Map<String, LocalSubscription> subscriptionList = getLocalSubscriptionList(destination, false);
        Iterator<LocalSubscription> iterator = subscriptionList.values().iterator();
        LocalSubscription subscriptionToRemove = null;
        while (iterator.hasNext()) {
            LocalSubscription subscription = iterator.next();
            if (subscription.getSubscriptionID().equals(subscriptionID)) {
                subscriptionToRemove = subscription;
                iterator.remove();
                break;
            }
        }

        //check topic local subscriptions
        if (subscriptionToRemove == null) {
            subscriptionList = getLocalSubscriptionList(destination, true);
            iterator = subscriptionList.values().iterator();
            while (iterator.hasNext()) {
                LocalSubscription subscription = iterator.next();
                if (subscription.getSubscriptionID().equals(subscriptionID)) {
                    subscriptionToRemove = subscription;
                    iterator.remove();
                    break;
                }
            }
        }

        if (subscriptionToRemove != null) {
            String destinationIdentifier = new StringBuffer().append((subscriptionToRemove.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX))
                    .append(destination).toString();
            andesContextStore.removeDurableSubscription(destinationIdentifier, subscrption.getSubscribedNode() + "_" + subscriptionID);
            log.info("Subscription Removed Locally for  " + destination + "@" + subscriptionID + " " + subscriptionToRemove);
        } else {
            throw new AndesException("Could not find an subscription ID " + subscriptionID + " under destination " + destination
                    + " topic=" + localTopicSubscriptionMap + "\n" + localQueueSubscriptionMap + "\n");
        }
        return subscriptionToRemove;
    }


    /**
     * get bindings of durable queues. We only wanted to get bindings that are durable (bound to durable queues).
     * We identify bindings iterating through Subscriptions.
     * There a unique binding is identified by <exchange - queue - routing key(destination)>
     *
     * @return list of bindings
     */
    public List<AndesBinding> getDurableBindings() {
        HashMap<String, AndesBinding> bindings = new HashMap<String, AndesBinding>();
        for (String destination : clusterQueueSubscriptionMap.keySet()) {
            for (AndesSubscription subscription : clusterQueueSubscriptionMap.get(destination)) {
                String bindingIdentifier = new StringBuffer(subscription.getTargetQueue()).append("&").append(subscription.getSubscribedDestination()).toString();
                if (subscription.isDurable() && bindings.get(bindingIdentifier) == null) {
                    AndesQueue andesQueue = new AndesQueue(subscription.getTargetQueue(), subscription.getTargetQueueOwner(), subscription.isExclusive(), subscription.isDurable());
                    AndesBinding andesBinding = new AndesBinding(subscription.getTargetQueueBoundExchangeName(), andesQueue, subscription.getSubscribedDestination());
                    bindings.put(bindingIdentifier, andesBinding);
                }
            }
        }

        for (String destination : clusterTopicSubscriptionMap.keySet()) {
            for (AndesSubscription subscription : clusterTopicSubscriptionMap.get(destination)) {
                String bindingIdentifier = new StringBuffer(subscription.getTargetQueue()).append("&").append(subscription.getSubscribedDestination()).toString();
                if (subscription.isDurable() && bindings.get(bindingIdentifier) == null) {
                    AndesQueue andesQueue = new AndesQueue(subscription.getTargetQueue(), subscription.getTargetQueueOwner(), subscription.isExclusive(), subscription.isDurable());
                    AndesBinding andesBinding = new AndesBinding(subscription.getTargetQueueBoundExchangeName(), andesQueue, subscription.getSubscribedDestination());
                    bindings.put(bindingIdentifier, andesBinding);
                }
            }
        }

        return new ArrayList<AndesBinding>(bindings.values());
    }

    /**
     * @return list of topics in cluster
     */
    public List<String> getTopics() {
        return new ArrayList<String>(clusterTopicSubscriptionMap.keySet());
    }

    /**
     * We consider a queue exists in broker if there is a durable binding for that queue
     * This is for durable queues only.
     *
     * @return list of durable queues
     */
    public List<AndesQueue> getDurableQueues() {
        List<AndesQueue> queues = new ArrayList<AndesQueue>();
        List<AndesBinding> bindingList = getDurableBindings();
        for (AndesBinding binding : bindingList) {
            if (binding.boundExchangeName.equals(AMQPUtils.DIRECT_EXCHANGE_NAME)) {
                if (!queues.contains(binding.boundQueue)) {
                    queues.add(binding.boundQueue);
                }
            }
        }
        return queues;
    }

    public List<AndesExchange> getExchanges() throws AndesException {
        //return andesContextStore.getAllExchangesStored();
        HashMap<String, AndesExchange> exchanges = new HashMap<String, AndesExchange>();
        for (String destination : clusterQueueSubscriptionMap.keySet()) {
            for (AndesSubscription subscription : clusterQueueSubscriptionMap.get(destination)) {
                if (subscription.getTargetQueueBoundExchangeName() != null) {
                    String exchangeIdentifier = new StringBuffer(subscription.getTargetQueueBoundExchangeName()).append("&").append(subscription.getTargetQueueBoundExchangeType()).append("&").append(subscription.ifTargetQueueBoundExchangeAutoDeletable()).toString();
                    if (subscription.isDurable() && exchanges.get(exchangeIdentifier) == null) {
                        AndesExchange andesexchange = new AndesExchange(subscription.getTargetQueueBoundExchangeName(), subscription.getTargetQueueBoundExchangeType(), subscription.ifTargetQueueBoundExchangeAutoDeletable());
                        exchanges.put(exchangeIdentifier, andesexchange);
                    }
                }
            }
        }

        for (String destination : clusterTopicSubscriptionMap.keySet()) {
            for (AndesSubscription subscription : clusterTopicSubscriptionMap.get(destination)) {
                if (subscription.getTargetQueueBoundExchangeName() != null) {
                    String exchangeIdentifier = new StringBuffer(subscription.getTargetQueueBoundExchangeName()).append("&").append(subscription.getTargetQueueBoundExchangeType()).append("&").append(subscription.ifTargetQueueBoundExchangeAutoDeletable()).toString();
                    if (subscription.isDurable() && exchanges.get(exchangeIdentifier) == null) {
                        AndesExchange andesexchange = new AndesExchange(subscription.getTargetQueueBoundExchangeName(), subscription.getTargetQueueBoundExchangeType(), subscription.ifTargetQueueBoundExchangeAutoDeletable());
                        exchanges.put(exchangeIdentifier, andesexchange);
                    }
                }
            }
        }
        return new ArrayList<AndesExchange>(exchanges.values());
    }
}
