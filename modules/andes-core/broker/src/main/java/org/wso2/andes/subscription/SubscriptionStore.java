package org.wso2.andes.subscription;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.SubscriptionListener.SubscriptionChange;
import org.wso2.andes.pool.AndesExecuter;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.util.AndesUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionStore {
    private static final String TOPIC_PREFIX = "topic.";


	private static final String QUEUE_PREFIX = "queue.";


	private static Log log = LogFactory.getLog(SubscriptionStore.class);

    //<routing key, List of local subscriptions>
	private Map<String, List<Subscrption>> clusterTopicSubscriptionMap = new ConcurrentHashMap<String, List<Subscrption>>();
	private Map<String, List<Subscrption>> clusterQueueSubscriptionMap = new ConcurrentHashMap<String, List<Subscrption>>();

    //<destination, <subscriptionID,LocalSubscription>>
	private Map<String, Map<String, LocalSubscription>> localTopicSubscriptionMap = new ConcurrentHashMap<String, Map<String, LocalSubscription>>();
	private Map<String, Map<String, LocalSubscription>> localQueueSubscriptionMap = new ConcurrentHashMap<String, Map<String, LocalSubscription>>();

	private List<SubscriptionListener> subscriptionListeners = new ArrayList<SubscriptionListener>();

    private AndesContextStore andesContextStore;

	
	public SubscriptionStore() throws AndesException {

        andesContextStore = AndesContext.getInstance().getAndesContextStore();
        //adding subscription listeners
        addSubscriptionListener(new OrphanedMessagesDueToUnsubscriptionHandler());
        addSubscriptionListener(new ClusterwideSubscriptionChangeNotifier());
	}

    /**
     * get a list of subscribers (destination queue names) in the cluster for the given topic
     * @param topic name of topic
     * @return list of subscribers
     * @throws AndesException
     */
    public List<Subscrption> getClusterSubscribersForTopic(String topic) throws AndesException {
    	List<Subscrption> list = clusterTopicSubscriptionMap.get(topic);
		if( list != null){
			return list; 
		}else{
			return Collections.emptyList();
		}
    }

    
    public List<Subscrption> getActiveClusterSubscribersForDestination(String destination, boolean isTopic) throws AndesException {
    	List<Subscrption> list =  isTopic? clusterTopicSubscriptionMap.get(destination): clusterQueueSubscriptionMap.get(destination);
        List<Subscrption> subscriptionsHavingExternalsubscriber = new ArrayList<Subscrption>();
        if( list != null){
            for(Subscrption subscription : list) {
                if(subscription.hasExternalSubscriptions()) {
                    subscriptionsHavingExternalsubscriber.add(subscription);
                }
            }
        }
        return subscriptionsHavingExternalsubscriber;
    }

    public int numberOfSubscriptionsForQueueAtNode(String queueName, String nodeID) throws AndesException {
        String requestedNodeQueue = AndesUtils.getNodeQueueNameForNodeId(nodeID);
        List<Subscrption> subscriptions = getActiveClusterSubscribersForDestination(queueName, false);
        int count = 0;
        if(subscriptions != null && !subscriptions.isEmpty()) {
            for(Subscrption sub : subscriptions) {
                if(sub.getSubscribedNode().equals(requestedNodeQueue)) {
                    count++;
                }
            }
        }
        return count;
    }

    public int numberOfSubscriptionsForQueueInCluster(String queueName) throws AndesException {
         return getActiveClusterSubscribersForDestination(queueName, false).size();
    }

    public void updateSubscriptionMaps(Subscrption subscription, SubscriptionChange type){
        if(type == SubscriptionChange.Added){
            if(subscription.isBoundToTopic()){
                addSubscriptionToMap(subscription,clusterTopicSubscriptionMap);
            }else{
                addSubscriptionToMap(subscription, clusterQueueSubscriptionMap);
            }
            log.info("Added Subscription to map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());
        }
        else if(type == SubscriptionChange.Deleted){
            if(subscription.isBoundToTopic()){
                removeSubscriptionFromMap(subscription, clusterTopicSubscriptionMap);
            }else{
                removeSubscriptionFromMap(subscription, clusterQueueSubscriptionMap);
            }
            log.info("Deleted Subscription from map. queue name:" + subscription.getTargetQueue() + ", Type: " + subscription.getTargetQueueBoundExchangeType());
        }
        else if(type == SubscriptionChange.Disconnected){
            log.info("Encoded string: " + subscription.encodeAsStr());
            if(subscription.isBoundToTopic()){
                SubscriptionMap(subscription, clusterTopicSubscriptionMap);
            }else{
                SubscriptionMap(subscription, clusterQueueSubscriptionMap);
            }
        }
        log.info("===============Updated subscription maps================");
        this.printSubscriptionMap(clusterQueueSubscriptionMap);
        this.printSubscriptionMap(clusterTopicSubscriptionMap);
        log.info("========================================================");
    }
    
    public void reloadSubscriptionsFromStorage(){
    	try{
    		Map<String, List<String>> results = andesContextStore.getAllStoredDurableSubscriptions();
        	for(Entry<String, List<String>> entry: results.entrySet()){
        		String destination = entry.getKey(); 
        		List<Subscrption> subscriptionList = new ArrayList<Subscrption>();
        		for(String subscriptionAsStr: entry.getValue()){
        			BasicSubscription subscription = new BasicSubscription(subscriptionAsStr);
        			subscriptionList.add(subscription);
        		}
        		
        		List<Subscrption> oldSubscriptionList;
        		if(destination.startsWith(QUEUE_PREFIX)){
        			oldSubscriptionList = clusterQueueSubscriptionMap.put(destination.replace(QUEUE_PREFIX, ""), subscriptionList);
        		}else{
        			oldSubscriptionList = clusterTopicSubscriptionMap.put(destination.replace(TOPIC_PREFIX, ""), subscriptionList);
        		}
        		
        		if(oldSubscriptionList == null){
        			oldSubscriptionList = Collections.emptyList();
        		}
        		
        		//TODO may be there is a better way to do the subscription Diff
        		if(subscriptionListeners.size() > 0){
        			subscriptionList.removeAll(oldSubscriptionList);
    				for(Subscrption subscrption: subscriptionList){
    					notifyListeners(subscrption, false, SubscriptionChange.Added);
    				}
        			oldSubscriptionList.removeAll(subscriptionList);
    				for(Subscrption subscrption: oldSubscriptionList){
    					notifyListeners(subscrption, false, SubscriptionChange.Deleted);
    				}
        		}

                log.info("===============Updated subscription maps================");
                this.printSubscriptionMap(clusterQueueSubscriptionMap);
                this.printSubscriptionMap(clusterTopicSubscriptionMap);
                log.info("========================================================");
        	}
    	}catch(Exception ex){
    		log.error(ex);
    	}
    	
    	log.info("Reloaded cluster subscriptions >> \n\tqueues ="+ clusterQueueSubscriptionMap + "\n\ttopics ="+ clusterTopicSubscriptionMap);
    }
    
    public Collection<LocalSubscription> getLocalSubscribersForTopic(String topic) throws AndesException{
    	return getSubscriptionList(topic, true).values();
    }

    public Collection<LocalSubscription> getLocalSubscribersForQueue(String queue) throws AndesException{
    	return getSubscriptionList(queue, false).values();
    }

    public Collection<LocalSubscription> getActiveLocalSubscribersForTopic(String topic) throws AndesException{
        Collection<LocalSubscription> list = getSubscriptionList(topic, true).values();
        Collection<LocalSubscription> activeLocalSubscriptionList = new ArrayList<LocalSubscription>();
        for(LocalSubscription localSubscription : list) {
            if(localSubscription.hasExternalSubscriptions()) {
                activeLocalSubscriptionList.add(localSubscription);
            }
        }
        return activeLocalSubscriptionList;
    }

    public Collection<LocalSubscription> getActiveLocalSubscribersForQueue(String queue) throws AndesException{
        Collection<LocalSubscription> list = getSubscriptionList(queue, false).values();
        Collection<LocalSubscription> activeLocalSubscriptionList = new ArrayList<LocalSubscription>();
        for(LocalSubscription localSubscription : list) {
            if(localSubscription.hasExternalSubscriptions()) {
                activeLocalSubscriptionList.add(localSubscription);
            }
        }
        return activeLocalSubscriptionList;
    }

    public void addLocalSubscription(LocalSubscription subscription) throws AndesException {
         createDisconnectOrRemoveLocalSubscription(subscription, SubscriptionChange.Added);
    }

    public void closeLocalSubscription(LocalSubscription subscription) throws AndesException {
         createDisconnectOrRemoveLocalSubscription(subscription, SubscriptionChange.Disconnected);
    }

    public void removeAllSubscriptionsRepresentingBinding(String destination, String targetQueue) throws AndesException{
        Map<String, LocalSubscription> subscriptionList = getSubscriptionList(destination, true);

        //Find all topic subscriptions with this target queue and routing key - we will find only one
        Iterator<LocalSubscription> topicSubscriptionItr = subscriptionList.values().iterator();
        while(topicSubscriptionItr.hasNext()){
            LocalSubscription subscription = topicSubscriptionItr.next();
            if(subscription.getTargetQueue().equals(targetQueue)){
                removeLocalSubscriptionAndNotify(destination, subscription.getSubscriptionID());
                break;
            }
        }

        //remove any queue subscriptions subscribed to this - we might find more than one
        Map<String, LocalSubscription> queueSubscriptionList = getSubscriptionList(targetQueue, false);
        Iterator<LocalSubscription> queueSubscriptionItr = queueSubscriptionList.values().iterator();
        while(queueSubscriptionItr.hasNext()){
            LocalSubscription subscription = queueSubscriptionItr.next();
            removeLocalSubscriptionAndNotify(targetQueue, subscription.getSubscriptionID());
        }

    }

    /**
     * Using cluster subscriptions find the local subscriptions of an node
     * and close all of them
     * @param nodeID id of the node
     * @param isTopic is to close topic subscriptions or queue subscriptions
     * @throws AndesException
     */
    public void closeAllClusterSubscriptionsOfNode(String nodeID, boolean isTopic) throws AndesException {
        Set<String> destinations = isTopic? clusterTopicSubscriptionMap.keySet(): clusterQueueSubscriptionMap.keySet();
        for(String destination : destinations) {
            List<Subscrption> subscriptionsOfDestination = isTopic? clusterTopicSubscriptionMap.get(destination):
                    clusterQueueSubscriptionMap.get(destination);
            String nodeQueueName = isTopic? AndesUtils.getTopicNodeQueueNameForNodeId(nodeID) : AndesUtils.getNodeQueueNameForNodeId(nodeID);
            List<Subscrption> subscriptionsOfNode = getSubscriptionsOfNode(nodeQueueName,subscriptionsOfDestination);
            if(subscriptionsOfNode !=null && !subscriptionsOfNode.isEmpty()) {
                for(Subscrption sub : subscriptionsOfNode) {
                    //remove and notify
                    removeLocalSubscriptionAndNotify(sub.getSubscribedDestination(),sub.getSubscriptionID());
                }
            }
        }
    }

    /**
     * close all local subscriptions of node
     * @param isTopic is to close topic subscriptions or queue subscriptions
     * @throws AndesException
     */
    public void closeAllLocalSubscriptionsOfNode(boolean isTopic) throws AndesException {
        Set<String> destinations = isTopic? localTopicSubscriptionMap.keySet(): localQueueSubscriptionMap.keySet();
        for(String destination : destinations) {
            Map<String, LocalSubscription> subscriptionsOfDestination = isTopic? localTopicSubscriptionMap.get(destination):
                    localQueueSubscriptionMap.get(destination);
            if(subscriptionsOfDestination != null && !subscriptionsOfDestination.isEmpty()) {
                for(Subscrption sub : subscriptionsOfDestination.values()) {
                    //remove and notify
                    removeLocalSubscriptionAndNotify(sub.getSubscribedDestination(),sub.getSubscriptionID());
                }
            }
        }
    }

    private void createDisconnectOrRemoveLocalSubscription(LocalSubscription subscrption, SubscriptionChange type) throws AndesException {
            Map<String, LocalSubscription> subscriptionList = getSubscriptionList(subscrption.getSubscribedDestination(),
                    subscrption.isBoundToTopic());
            //TODO:hasitha- review this
            //We need to handle durable topic subscriptions
            boolean hasDurableSubscriptionAlreadyInPlace = false;
            if (subscrption.isBoundToTopic() && subscrption.isDurable()) {
                /**
                 * Check if an active durable topic subscription already in place. If so we should not accept the subscription
                 */
                List<Subscrption> existingSubscriptions = clusterTopicSubscriptionMap.get(subscrption.getSubscribedDestination());
                if(existingSubscriptions != null && !existingSubscriptions.isEmpty()) {
                    for(Subscrption sub : existingSubscriptions) {
                        if (sub.isDurable() &&
                                sub.getTargetQueue().equals(subscrption.getTargetQueue()) &&
                                sub.hasExternalSubscriptions()) {
                            hasDurableSubscriptionAlreadyInPlace = true;
                            break;
                        }
                    }
                }

                if(!hasDurableSubscriptionAlreadyInPlace && type == SubscriptionChange.Disconnected) {
                    //close subscription
                    //createDisconnectOrRemoveLocalSubscription(subscrption, type);
                    throw new AndesException("There is no active subscriber to close subscribed to " + subscrption.getSubscribedDestination() + " with the queue " + subscrption.getTargetQueue());
                } else if(hasDurableSubscriptionAlreadyInPlace && type == SubscriptionChange.Added) {
                    //not permitted
                    throw new AndesException("A subscription already exists for Durable subscriptions on " + subscrption.getSubscribedDestination() + " with the queue " + subscrption.getTargetQueue());
                }

            }

            if (type ==SubscriptionChange.Added || type ==SubscriptionChange.Disconnected) {
                //add or update subscription to local map
                subscriptionList.put(subscrption.getSubscriptionID(), subscrption);
                String destinationQueue = subscrption.getSubscribedDestination();
                //Store the subscription
                String destinationIdentifier = new StringBuffer().append((subscrption.isBoundToTopic() ? TOPIC_PREFIX : QUEUE_PREFIX))
                        .append(destinationQueue).toString();
                String subscriptionID = subscrption.getSubscriptionID();
                andesContextStore.storeDurableSubscription(destinationIdentifier, subscriptionID, subscrption.encodeAsStr());

                if(type == SubscriptionChange.Added) {
                    log.info("New Local Subscription Added " + subscrption);
                } else {
                    log.info("New Local Subscription Disconnected " + subscrption);
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

                if(type == SubscriptionChange.Added) {
                    notifyListeners(subscrption, true, SubscriptionChange.Added);
                } else {
                    notifyListeners(subscrption, true, SubscriptionChange.Disconnected);
                }

            } else if(type == SubscriptionChange.Deleted){
                removeLocalSubscriptionAndNotify(subscrption.getSubscribedDestination(), subscrption.getSubscriptionID());
            }

    }
    
    private LocalSubscription removeLocalSubscriptionAndNotify(String destination, String subscriptionID) throws AndesException{
    	long start = System.currentTimeMillis();
            //check queue local subscriptions
    		Map<String, LocalSubscription> subscriptionList = getSubscriptionList(destination, false);
			Iterator<LocalSubscription> iterator = subscriptionList.values().iterator();
			LocalSubscription subscriptionToRemove = null; 
			while(iterator.hasNext()){
				LocalSubscription subscription = iterator.next();
				if(subscription.getSubscriptionID().equals(subscriptionID)){
					subscriptionToRemove = subscription;
					iterator.remove();
					break;
				}
			}

            //check topic local subscriptions
			if(subscriptionToRemove == null){
				subscriptionList = getSubscriptionList(destination, true);
				iterator = subscriptionList.values().iterator();
				while(iterator.hasNext()){
					LocalSubscription subscription = iterator.next();
					if(subscription.getSubscriptionID().equals(subscriptionID)){
						subscriptionToRemove = subscription;
						iterator.remove();
						break;
					}
				}
			}
			
			if(subscriptionToRemove != null){
				String destinationIdentifier = new StringBuffer().append((subscriptionToRemove.isBoundToTopic()? TOPIC_PREFIX:QUEUE_PREFIX))
						.append(destination).toString();
                andesContextStore.removeDurableSubscription(destinationIdentifier,subscriptionID);
				log.info("Subscription Removed Locally for  "+ destination + "@" +  subscriptionID + " "+ subscriptionToRemove);
				notifyListeners(subscriptionToRemove, true, SubscriptionChange.Deleted);
			}else{
				throw new AndesException("Could not find an subscription ID "+ subscriptionID + " under destination " + destination 
						+ " topic=" + localTopicSubscriptionMap + "\n" + localQueueSubscriptionMap + "\n"); 
			}
	    	System.out.println("removing Local Subscription done, took "+ (System.currentTimeMillis() - start));
			return subscriptionToRemove;
    }

    private void notifyListeners(final Subscrption subscrption, final boolean local, final SubscriptionChange change){
		for(final SubscriptionListener listener: subscriptionListeners){
			AndesExecuter.runAsync(new Runnable() {
				@Override
				public void run() {
					if(local){
                        if(log.isDebugEnabled()) {
                            log.debug("TRACING>> Notifying local subscription change to the cluster " + subscrption.toString());
                        }
						listener.notifyLocalSubscriptionHasChanged((LocalSubscription)subscrption, change);	
					}else{
						listener.notifyClusterSubscriptionHasChanged(subscrption, change);
					}
				}
			});
		}
    }
    
    
    public void addSubscriptionListener(SubscriptionListener listener){
    	subscriptionListeners.add(listener);
    }
    
    private Map<String,LocalSubscription> getSubscriptionList(String destination, boolean isTopic){
    	Map<String, Map<String, LocalSubscription>> subscriptionMap = isTopic? localTopicSubscriptionMap: localQueueSubscriptionMap;
    	Map<String,LocalSubscription> list = subscriptionMap.get(destination);
    	if(list == null){
    		list = new ConcurrentHashMap<String,LocalSubscription>();
    		subscriptionMap.put(destination, list); 
    	}
    	return list;
    }
    
    public List<String> listQueues(){
        List<String> queues = new ArrayList<String>();
        for(AndesQueue queue : getDurableQueues()) {
             queues.add(queue.queueName);
        }
    	return  queues;
    }
    
    /**
     * get a List of nodes having subscriptions to the given destination queue
     * @param queueName destination queue name
     * @return list of nodes
     */
    public Set<String> getNodeQueuesHavingSubscriptionsForQueue(String queueName) throws AndesException{
        List<Subscrption> nodesHavingSubscriptions4Queue = getActiveClusterSubscribersForDestination(queueName, false);
        HashSet<String> nodes = new HashSet<String>();
        for(Subscrption subscrption: nodesHavingSubscriptions4Queue){
        	nodes.add(subscrption.getSubscribedNode());
        }
        return nodes;
    }
    
    public Set<String> getNodeQueuesHavingSubscriptionsForTopic(String topicName) throws AndesException{
        List<Subscrption> nodesHavingSubscriptions4Topic = getClusterSubscribersForTopic(topicName);
        HashSet<String> nodes = new HashSet<String>();
        for(Subscrption subscrption: nodesHavingSubscriptions4Topic){
        	nodes.add(subscrption.getSubscribedNode());
        }
        return nodes;
    }



    private  List<Subscrption> getSubscriptionsOfNode(String nodeQueueName, List<Subscrption> subscriptionList){
        List<Subscrption> subscriptionsOfNode = new ArrayList<Subscrption>();
        for(Subscrption sub : subscriptionList) {
            if(sub.getSubscribedNode().equals(nodeQueueName)) {
                subscriptionsOfNode.add(sub);
            }
        }
        return subscriptionsOfNode;
    }


    public Map<String, Integer> getSubscriptionCountInformation(String destination, boolean isTopic) throws AndesException {

        Map<String,Integer> nodeSubscriptionCountMap = new HashMap<String, Integer>();
        List<Subscrption> subscriptions = getActiveClusterSubscribersForDestination(destination, isTopic);
        for(Subscrption sub : subscriptions) {
            Integer count = nodeSubscriptionCountMap.get(sub.getSubscribedNode());
            if(count == null) {
                nodeSubscriptionCountMap.put(sub.getSubscribedNode(),1);
            }  else {
                nodeSubscriptionCountMap.put(sub.getSubscribedNode(), count + 1);
            }
        }
        return nodeSubscriptionCountMap;
    }

    /**
     * get bindings of durable queues. We only wanted to get bindings that are durable (bound to durable queues).
     * We identify bindings iterating through Subscriptions.
     * There a unique binding is identified by <exchange - queue - routing key(destination)>
     * @return list of bindings
     */
    public List<AndesBinding> getDurableBindings() {
        HashMap<String,AndesBinding> bindings = new HashMap<String, AndesBinding>();
        for(String destination : clusterQueueSubscriptionMap.keySet()) {
             for(Subscrption subscription : clusterQueueSubscriptionMap.get(destination)) {
                 String bindingIdentifier = new StringBuffer(subscription.getTargetQueue()).append("&").append(subscription.getSubscribedDestination()).toString();
                 if(subscription.isDurable() && bindings.get(bindingIdentifier) == null) {
                       AndesQueue andesQueue = new AndesQueue(subscription.getTargetQueue(),subscription.getTargetQueueOwner(),subscription.isExclusive(),subscription.isDurable());
                       AndesBinding andesBinding = new AndesBinding(subscription.getTargetQueueBoundExchangeName(),andesQueue,subscription.getSubscribedDestination());
                     bindings.put(bindingIdentifier, andesBinding);
                 }
             }
        }

        for(String destination : clusterTopicSubscriptionMap.keySet()) {
            for(Subscrption subscription : clusterTopicSubscriptionMap.get(destination)) {
                String bindingIdentifier = new StringBuffer(subscription.getTargetQueue()).append("&").append(subscription.getSubscribedDestination()).toString();
                if(subscription.isDurable() && bindings.get(bindingIdentifier) == null) {
                    AndesQueue andesQueue = new AndesQueue(subscription.getTargetQueue(),subscription.getTargetQueueOwner(),subscription.isExclusive(),subscription.isDurable());
                    AndesBinding andesBinding = new AndesBinding(subscription.getTargetQueueBoundExchangeName(),andesQueue,subscription.getSubscribedDestination());
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
     * @return list of durable queues
     */
    public List<AndesQueue> getDurableQueues()  {
        List<AndesQueue> queues = new ArrayList<AndesQueue>();
        List<AndesBinding> bindingList = getDurableBindings();
        for(AndesBinding binding : bindingList) {
            if(binding.boundExchangeName.equals(AMQPUtils.DIRECT_EXCHANGE_NAME)) {
                if(!queues.contains(binding.boundQueue)) {
                    queues.add(binding.boundQueue);
                }
            }
        }
        return queues;
    }

    public void removeQueue(String destinationQueueName, boolean isExclusive) throws AndesException{
            //check if there are active subscribers in cluster
            if(!isExclusive && getNodeQueuesHavingSubscriptionsForQueue(destinationQueueName).size() > 0) {
                throw new AndesException("There are Subscriptions for This Queue in Cluster. Stop Them First");
            }  else if(isExclusive) {
                if(getActiveLocalSubscribersForQueue(destinationQueueName).size() > 0) {
                    throw new AndesException("There is an active Exclusive Subscriptions for This Queue in Current Node. Stop it First");
                }
            }

            //remove all queue subscriptions and notify
            Map<String,LocalSubscription> localQueueSubscriptions = localQueueSubscriptionMap.get(destinationQueueName);
            if(localQueueSubscriptions != null && localQueueSubscriptions.values().size() >0) {
                for(LocalSubscription sub : localQueueSubscriptions.values()) {
                    removeLocalSubscriptionAndNotify(sub.getSubscribedDestination(),sub.getSubscriptionID());
                }
            }
            //remove message counter
            andesContextStore.removeMessageCounterForQueue(destinationQueueName);

            //remove topic local subscriptions and notify
            Set<String> destinations = localTopicSubscriptionMap.keySet();
            for(String destination : destinations) {
                Map<String,LocalSubscription> topicSubscriptions = localTopicSubscriptionMap.get(destination);
                if(topicSubscriptions != null && !topicSubscriptions.values().isEmpty()) {
                    for(LocalSubscription sub : topicSubscriptions.values()) {
                        if(sub.getTargetQueue().equals(destinationQueueName)) {
                            removeLocalSubscriptionAndNotify(destinationQueueName, sub.getSubscriptionID());
                            break;
                        }
                    }
                }
            }

            ClusterResourceHolder.getInstance().getSubscriptionManager().handleMessageRemovalFromNodeQueue(destinationQueueName);
    }

    public void createExchange(AndesExchange exchange) throws AndesException{
/*        try {
            String value = exchange.exchangeName + "|" + exchange.type + "|" + exchange.autoDelete;
            andesContextStore.storeExchangeInformation(exchange.exchangeName, value);
        } catch (Exception e) {
            throw new AndesException("Error in creating exchange " + exchange.exchangeName, e);
        }*/
        //todo:we do not currently create exchanges.
    }

    public List<AndesExchange> getExchanges() throws AndesException {
        //return andesContextStore.getAllExchangesStored();
        HashMap<String,AndesExchange> exchanges = new HashMap<String, AndesExchange>();
        for(String destination : clusterQueueSubscriptionMap.keySet()) {
            for(Subscrption subscription : clusterQueueSubscriptionMap.get(destination)) {
                String exchangeIdentifier = new StringBuffer(subscription.getTargetQueueBoundExchangeName()).append("&").append(subscription.getTargetQueueBoundExchangeType()).append("&").append(subscription.ifTargetQueueBoundExchangeAutoDeletable()).toString();
                if(subscription.isDurable() && exchanges.get(exchangeIdentifier) == null) {
                    AndesExchange andesexchange = new AndesExchange(subscription.getTargetQueueBoundExchangeName(),subscription.getTargetQueueBoundExchangeType(),subscription.ifTargetQueueBoundExchangeAutoDeletable());
                    exchanges.put(exchangeIdentifier, andesexchange);
                }
            }
        }

        for(String destination : clusterTopicSubscriptionMap.keySet()) {
            for(Subscrption subscription : clusterTopicSubscriptionMap.get(destination)) {
                String exchangeIdentifier = new StringBuffer(subscription.getTargetQueueBoundExchangeName()).append("&").append(subscription.getTargetQueueBoundExchangeType()).append("&").append(subscription.ifTargetQueueBoundExchangeAutoDeletable()).toString();
                if(subscription.isDurable() && exchanges.get(exchangeIdentifier) == null) {
                    AndesExchange andesexchange = new AndesExchange(subscription.getTargetQueueBoundExchangeName(),subscription.getTargetQueueBoundExchangeType(),subscription.ifTargetQueueBoundExchangeAutoDeletable());
                    exchanges.put(exchangeIdentifier, andesexchange);
                }
            }
        }
        return new ArrayList<AndesExchange>(exchanges.values());
    }

    public void deleteExchange(AndesExchange exchange) throws AndesException {
        //andesContextStore.deleteExchangeInformation(exchange.exchangeName);
        //todo: we do not currently delete exchanges. We have only direct and topic
    }

    private void SubscriptionMap(Subscrption subscription, Map<String, List<Subscrption>> map){
        String destinationIdentifier = subscription.getSubscribedDestination();
        List<Subscrption> subscriptionList = map.get(destinationIdentifier);
        if(subscriptionList == null){
            subscriptionList = new ArrayList<Subscrption>();
        }

        for(Subscrption s:subscriptionList){
            if(s.getSubscriptionID().equals(subscription.getSubscriptionID())){
                subscriptionList.remove(s);
                subscriptionList.add(subscription);
                break;
            }
        }
        map.put(destinationIdentifier, subscriptionList);
    }

    private void addSubscriptionToMap(Subscrption subscription, Map<String, List<Subscrption>> map){
        String destinationIdentifier = subscription.getSubscribedDestination();
        List<Subscrption> subscriptionList = map.get(destinationIdentifier);
        if(subscriptionList == null){
            subscriptionList = new ArrayList<Subscrption>();
        }

        boolean duplicate = false;
        for(Subscrption s:subscriptionList){
            if(s.getSubscriptionID().equals(subscription.getSubscriptionID())){
                duplicate = true;
                subscriptionList.remove(s);
                subscriptionList.add(subscription);
                break;
            }
        }
        if(!duplicate){
            subscriptionList.add(subscription);
        }
        map.put(destinationIdentifier, subscriptionList);
    }

    private void removeSubscriptionFromMap(Subscrption subscription, Map<String, List<Subscrption>> map) {
        String destinationIdentifier = subscription.getSubscribedDestination();
        List<Subscrption> subscriptionList = map.get(destinationIdentifier);
        for(Subscrption s: subscriptionList){
            if(s.getSubscriptionID().equals(subscription.getSubscriptionID())){
                subscriptionList.remove(s);
            }
        }
        if(subscriptionList == null || subscriptionList.size() == 0){
            map.remove(destinationIdentifier);
        } else {
            map.put(destinationIdentifier, subscriptionList);
        }
    }

    private void printSubscriptionMap(Map<String, List<Subscrption>> map){
        for(Entry<String, List<Subscrption>> entry: map.entrySet()){
            log.info("Key: " + entry.getKey());
            for (Subscrption s:entry.getValue()){
                log.info("---" + s.encodeAsStr());
            }
        }
    }
}
