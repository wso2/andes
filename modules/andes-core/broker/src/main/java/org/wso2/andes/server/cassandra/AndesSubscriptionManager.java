/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.*;
import org.wso2.andes.mqtt.MQTTLocalSubscription;
import org.wso2.andes.pool.AndesExecuter;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.BasicSubscription;
import org.wso2.andes.subscription.ClusterwideSubscriptionChangeNotifier;
import org.wso2.andes.subscription.OrphanedMessagesDueToUnsubscriptionHandler;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class AndesSubscriptionManager {

    private static Log log = LogFactory.getLog(AndesSubscriptionManager.class);

    //Hash map that keeps the unacked messages.
    private Map<AMQChannel, Map<Long, Semaphore>> unAckedMessagelocks =
            new ConcurrentHashMap<AMQChannel, Map<Long, Semaphore>>();

    private Map<AMQChannel,QueueSubscriptionAcknowledgementHandler> acknowledgementHandlerMap =
            new ConcurrentHashMap<AMQChannel,QueueSubscriptionAcknowledgementHandler>();

    private SubscriptionStore subscriptionStore;

    private List<SubscriptionListener> subscriptionListeners = new ArrayList<SubscriptionListener>();

    private static final String TOPIC_PREFIX = "topic.";
    private static final String QUEUE_PREFIX = "queue.";


    public void init()  {
        subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        //adding subscription listeners
        addSubscriptionListener(new OrphanedMessagesDueToUnsubscriptionHandler());
        addSubscriptionListener(new ClusterwideSubscriptionChangeNotifier());
        //subscriptionStore.reloadSubscriptionsFromStorage();
    }


    /**
     * Register a subscription lister
     * It will be notified when a subscription change happened
     * @param listener subscription listener
     */
    public void addSubscriptionListener(SubscriptionListener listener){
        subscriptionListeners.add(listener);
    }

    /**
     * Register a subscription for a Given Queue
     * This will handle the subscription addition task.
     * @param localSubscription local subscription
     * @throws AndesException
     */
    public void addSubscription(LocalSubscription localSubscription) throws AndesException {

        log.info("Added subscription: " + localSubscription.toString());

        //create a local subscription and notify
        subscriptionStore.createDisconnectOrRemoveLocalSubscription(localSubscription, SubscriptionListener.SubscriptionChange.Added);
        notifyListeners(localSubscription, true, SubscriptionListener.SubscriptionChange.Added);

        if (localSubscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.DIRECT_EXCHANGE_NAME) && localSubscription.hasExternalSubscriptions()) {
            String globalQueueName = AndesUtils.getGlobalQueueNameForDestinationQueue(localSubscription.getTargetQueue());
            ClusterResourceHolder.getInstance().getClusterManager().getGlobalQueueManager().resetGlobalQueueWorkerIfRunning(globalQueueName);
        } else if (localSubscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME) && localSubscription.hasExternalSubscriptions()) {
            //now we have a subscription on this node. Start a topicDeliveryWorker if one has not started
            if (!ClusterResourceHolder.getInstance().getTopicDeliveryWorker().isWorking()) {
                ClusterResourceHolder.getInstance().getTopicDeliveryWorker().setWorking();
            }
        } else if (localSubscription instanceof MQTTLocalSubscription) {
            //now we have a MQTT subscription on this node. Start a topicDeliveryWorker if one has not started
            if (!ClusterResourceHolder.getInstance().getTopicDeliveryWorker().isWorking()) {
                ClusterResourceHolder.getInstance().getTopicDeliveryWorker().setWorking();
            }
        }
    }

    /**
     * Using cluster subscriptions find the local subscriptions of an node
     * and close all of them
     * @param nodeID id of the node
     * @throws AndesException
     */
    public void closeAllClusterSubscriptionsOfNode(String nodeID) throws AndesException {

        List<AndesSubscription> activeSubscriptions = subscriptionStore.getActiveClusterSubscribersForNode(nodeID, true);
        activeSubscriptions.addAll(subscriptionStore.getActiveClusterSubscribersForNode(nodeID, false));

        if(!activeSubscriptions.isEmpty()) {
            for(AndesSubscription sub : activeSubscriptions) {
                //close and notify
                subscriptionStore.createDisconnectOrRemoveClusterSubscription(sub, SubscriptionListener.SubscriptionChange.Disconnected);
                //TODO:Notify to the cluster
            }
        }

    }


    /**
     * Close all active local subscribers in the node
     * @param nodeID node id
     * @throws AndesException
     */
    public void closeAllLocalSubscriptionsOfNode(String nodeID) throws AndesException {

        List<LocalSubscription> activeSubscriptions = subscriptionStore.getActiveLocalSubscribersForNode(nodeID, true);
        activeSubscriptions.addAll(subscriptionStore.getActiveLocalSubscribersForNode(nodeID, false));

        if(!activeSubscriptions.isEmpty()) {
            for(LocalSubscription sub : activeSubscriptions) {
                //close and notify
                subscriptionStore.createDisconnectOrRemoveLocalSubscription(sub, SubscriptionListener.SubscriptionChange.Disconnected);
                notifyListeners(sub, true, SubscriptionListener.SubscriptionChange.Disconnected);
            }
        }

    }

    /**
     * close subscription
     * @param subscription subscription to close
     * @throws AndesException
     */
    public void closeSubscription(LocalSubscription subscription) throws AndesException{
        log.info("Closed subscription: " + subscription.toString());
        subscriptionStore.createDisconnectOrRemoveLocalSubscription(subscription, SubscriptionListener.SubscriptionChange.Disconnected);
        notifyListeners(subscription, true, SubscriptionListener.SubscriptionChange.Disconnected);
    }

    public Map<AMQChannel, Map<Long, Semaphore>> getUnAcknowledgedMessageLocks() {
        return unAckedMessagelocks;
    }

    public Map<AMQChannel, QueueSubscriptionAcknowledgementHandler> getAcknowledgementHandlerMap() {
        return acknowledgementHandlerMap;
    }

    /**
     * remove all messages from global queue addressed to destination queue
     * @param destinationQueueName destination queue to match
     * @throws AndesException
     */
    @Deprecated()
    public void handleMessageRemovalFromGlobalQueue(String destinationQueueName) throws AndesException {
        // todo need to remove
        int numberOfMessagesRemoved = 0;
        //there can be messages still in global queue which needs to be removed
        numberOfMessagesRemoved += MessagingEngine.getInstance().removeMessagesOfQueue(destinationQueueName);
        log.info("Removed " + numberOfMessagesRemoved + " Messages From Global Queue Addressed to Queue " + destinationQueueName);
    }

    /**
     * remove all messages from node queue addressed to the given destination queue
     * @param destinationQueueName  destination queue name to match
     * @throws AndesException
     */
    public void handleMessageRemovalFromNodeQueue(String destinationQueueName) throws AndesException {

        int numberOfMessagesRemoved = 0;

        //remove any in-memory messages accumulated for the queue
        MessagingEngine.getInstance().removeInMemoryMessagesAccumulated(destinationQueueName);

        //there can be non-acked messages in the node queue addressed to the destination queue
        numberOfMessagesRemoved += MessagingEngine.getInstance().removeMessagesOfQueue(destinationQueueName);

        log.info("Removed " + numberOfMessagesRemoved + " Messages From Node Queue Addressed to Queue " + destinationQueueName);
    }

    /**
     * Update cluster subscription maps upon a change
     * @param subscription subscription added, disconnected or removed
     * @param type  what the change is
     */
    //TODO:MOVE
    public void updateClusterSubscriptionMaps(AndesSubscription subscription, SubscriptionListener.SubscriptionChange type){

        subscriptionStore.createDisconnectOrRemoveClusterSubscription(subscription, type);

    }

    /**
     * Reload subscriptions from DB storage and update cluster subscription lists
     */
    public void reloadSubscriptionsFromStorage(){
        try{
            Map<String, List<String>> results = AndesContext.getInstance().getAndesContextStore().getAllStoredDurableSubscriptions();
            for(Map.Entry<String, List<String>> entry: results.entrySet()){
                String destination = entry.getKey();
                List<AndesSubscription> newSubscriptionList = new ArrayList<AndesSubscription>();
                for(String subscriptionAsStr: entry.getValue()){
                    BasicSubscription subscription = new BasicSubscription(subscriptionAsStr);
                    newSubscriptionList.add(subscription);
                }

                List<AndesSubscription> oldSubscriptionList;
                if(destination.startsWith(QUEUE_PREFIX)){
                    String destinationQueueName =  destination.replace(QUEUE_PREFIX, "");
                    oldSubscriptionList = subscriptionStore.replaceClusterSubscriptionListOfDestination(destinationQueueName, newSubscriptionList, false);
                }else{
                    String topicName = destination.replace(TOPIC_PREFIX, "");
                    oldSubscriptionList = subscriptionStore.replaceClusterSubscriptionListOfDestination(topicName, newSubscriptionList, false);
                }

                if(oldSubscriptionList == null){
                    oldSubscriptionList = Collections.emptyList();
                }

                //TODO may be there is a better way to do the subscription Diff
                if(subscriptionListeners.size() > 0){
                    newSubscriptionList.removeAll(oldSubscriptionList);
                    for(AndesSubscription subscrption: newSubscriptionList){
                        notifyListeners(subscrption, false, SubscriptionListener.SubscriptionChange.Added);
                    }
                    oldSubscriptionList.removeAll(newSubscriptionList);
                    for(AndesSubscription subscrption: oldSubscriptionList){
                        notifyListeners(subscrption, false, SubscriptionListener.SubscriptionChange.Deleted);
                    }
                }
            }

        }catch(Exception ex){
            log.error(ex);
        }
    }

    /**
     * Remove all the subscriptions representing bindings
     * @param destination  binding key
     * @param targetQueue  queue whose binding should be removed
     * @throws AndesException
     */
    public void removeAllSubscriptionsRepresentingBinding(String destination, String targetQueue) throws AndesException{
        Map<String, LocalSubscription> subscriptionList = subscriptionStore.getLocalSubscriptionList(destination, true);

        //Find all topic subscriptions with this target queue and routing key - we will find only one
        Iterator<LocalSubscription> topicSubscriptionItr = subscriptionList.values().iterator();
        while(topicSubscriptionItr.hasNext()){
            LocalSubscription subscription = topicSubscriptionItr.next();
            if(subscription.getTargetQueue().equals(targetQueue)){
                subscriptionStore.createDisconnectOrRemoveLocalSubscription(subscription, SubscriptionListener.SubscriptionChange.Deleted);
                notifyListeners(subscription, true, SubscriptionListener.SubscriptionChange.Deleted);
                break;
            }
        }

        //remove any queue subscriptions subscribed to this - we might find more than one
        Map<String, LocalSubscription> queueSubscriptionList = subscriptionStore.getLocalSubscriptionList(targetQueue, false);
        Iterator<LocalSubscription> queueSubscriptionItr = queueSubscriptionList.values().iterator();
        while(queueSubscriptionItr.hasNext()){
            LocalSubscription subscription = queueSubscriptionItr.next();
            subscriptionStore.createDisconnectOrRemoveLocalSubscription(subscription, SubscriptionListener.SubscriptionChange.Deleted);
            notifyListeners(subscription, true, SubscriptionListener.SubscriptionChange.Deleted);
        }

    }

    /**
     * Remove all subscriptions representing a queue
     * @param destinationQueueName  queue name
     * @param isExclusive  if the queue is exclusive
     * @throws AndesException
     */
    public void removeSubscriptionsRepresentingQueue(String destinationQueueName, boolean isExclusive) throws AndesException{
        //check if there are active subscribers in cluster
        if(!isExclusive && subscriptionStore.getNodeQueuesHavingSubscriptionsForQueue(destinationQueueName).size() > 0) {
            throw new AndesException("There are Subscriptions for This Queue in Cluster. Stop Them First");
        }  else if(isExclusive) {
            if(subscriptionStore.getActiveLocalSubscribersForQueue(destinationQueueName).size() > 0) {
                throw new AndesException("There is an active Exclusive Subscriptions for This Queue in Current Node. Stop it First");
            }
        }

        //remove all queue subscriptions and notify
        Map<String,LocalSubscription> localQueueSubscriptions = subscriptionStore.getLocalSubscriptionList(destinationQueueName, false);
        if(localQueueSubscriptions != null && localQueueSubscriptions.values().size() >0) {
            for(LocalSubscription sub : localQueueSubscriptions.values()) {
                subscriptionStore.createDisconnectOrRemoveLocalSubscription(sub, SubscriptionListener.SubscriptionChange.Deleted);
                notifyListeners(sub, true, SubscriptionListener.SubscriptionChange.Deleted);
            }
        }

        //remove message counter
        AndesContext.getInstance().getAndesContextStore().removeMessageCounterForQueue(destinationQueueName);

        //remove topic local subscriptions and notify
        List<String> topics  = subscriptionStore.getTopics();
        for(String destination : topics) {
            Map<String,LocalSubscription> localTopicSubscriptions = subscriptionStore.getLocalSubscriptionList(destination, true);
            if(localTopicSubscriptions != null && localTopicSubscriptions.values().size() >0) {
                for(LocalSubscription sub : localTopicSubscriptions.values()) {
                    if(sub.getTargetQueue().equals(destinationQueueName)) {
                        subscriptionStore.createDisconnectOrRemoveLocalSubscription(sub, SubscriptionListener.SubscriptionChange.Deleted);
                        notifyListeners(sub, true, SubscriptionListener.SubscriptionChange.Deleted);
                    }
                }
            }
        }

        //caller should remove messages from global queue
        handleMessageRemovalFromGlobalQueue(destinationQueueName);
        //delete messages from node queue of the removed queue
        handleMessageRemovalFromNodeQueue(destinationQueueName);
    }

    public void deleteSubscriptionsRepresentingExchange(AndesExchange exchange) throws AndesException {
        //andesContextStore.deleteExchangeInformation(exchange.exchangeName);
        //todo: we do not currently delete exchanges. We have only direct and topic
    }

    public void createSubscriptionRepresentingExchange(AndesExchange exchange) throws AndesException{
/*        try {
            String value = exchange.exchangeName + "|" + exchange.type + "|" + exchange.autoDelete;
            andesContextStore.storeExchangeInformation(exchange.exchangeName, value);
        } catch (Exception e) {
            throw new AndesException("Error in creating exchange " + exchange.exchangeName, e);
        }*/
        //todo:we do not currently create exchanges.
    }

    /**
     * Notify subscription change to the cluster
     * @param subscription  created, disconnected or removed subscription
     * @param local   local subscription change or a cluster subscription change
     * @param change if this is a create/disconnect or a delete
     */
    private void notifyListeners(final AndesSubscription subscription, final boolean local, final SubscriptionListener.SubscriptionChange change){
        for(final SubscriptionListener listener: subscriptionListeners){
            AndesExecuter.runAsync(new Runnable() {
                @Override
                public void run() {
                    if (local) {
                        if (log.isDebugEnabled()) {
                            log.debug("TRACING>> Notifying local subscription change to the cluster " + subscription.toString());
                        }
                        listener.notifyLocalSubscriptionHasChanged((LocalSubscription) subscription, change);
                    } else {
                        listener.notifyClusterSubscriptionHasChanged(subscription, change);
                    }
                }
            });
        }
    }

}
