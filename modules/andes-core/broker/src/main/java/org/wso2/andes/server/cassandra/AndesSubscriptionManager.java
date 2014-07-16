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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.SubscriptionStore;

public class AndesSubscriptionManager {

    private static Log log = LogFactory.getLog(AndesSubscriptionManager.class);

    //Hash map that keeps the unacked messages.
    private Map<AMQChannel, Map<Long, Semaphore>> unAckedMessagelocks =
            new ConcurrentHashMap<AMQChannel, Map<Long, Semaphore>>();

    private Map<AMQChannel,QueueSubscriptionAcknowledgementHandler> acknowledgementHandlerMap =
            new ConcurrentHashMap<AMQChannel,QueueSubscriptionAcknowledgementHandler>();

    private SubscriptionStore subscriptionStore;


    public void init()  {
        subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        subscriptionStore.reloadSubscriptionsFromStorage();
    }


    /**
     * Register a subscription for a Given Queue
     * This will handle the subscription addition task.
     * @param localSubscription local subscription
     * @throws AndesException
     */
    public void addSubscription(LocalSubscription localSubscription) throws AndesException {

        log.info("Added subscription: " + localSubscription.toString());
        SubscriptionStore subscriptionStore = AndesContext.getInstance().getSubscriptionStore();

        subscriptionStore.addLocalSubscription(localSubscription);
        if (localSubscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.DIRECT_EXCHANGE_NAME)) {
            String globalQueueName = AndesUtils.getGlobalQueueNameForDestinationQueue(localSubscription.getTargetQueue());
            ClusterResourceHolder.getInstance().getClusterManager().getGlobalQueueManager().resetGlobalQueueWorkerIfRunning(globalQueueName);
        } else if (localSubscription.getTargetQueueBoundExchangeName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME)) {
            //now we have a subscription on this node. Start a topicDeliveryWorker if one has not started
            if (!ClusterResourceHolder.getInstance().getTopicDeliveryWorker().isWorking()) {
                ClusterResourceHolder.getInstance().getTopicDeliveryWorker().setWorking();
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
		SubscriptionStore subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        subscriptionStore.closeLocalSubscription(subscription);
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
    public void handleMessageRemovalFromGlobalQueue(String destinationQueueName) throws AndesException {
        int numberOfMessagesRemoved = 0;
        //there can be messages still in global queue which needs to be removed
        String globalQueueName = AndesUtils.getGlobalQueueNameForDestinationQueue(destinationQueueName);
        QueueAddress globalQueueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE, globalQueueName);
        numberOfMessagesRemoved += MessagingEngine.getInstance().removeMessagesOfQueue(globalQueueAddress, destinationQueueName);
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
        String nodeQueueName = MessagingEngine.getMyNodeQueueName();
        QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueueName);
        numberOfMessagesRemoved += MessagingEngine.getInstance().removeMessagesOfQueue(nodeQueueAddress, destinationQueueName);

        log.info("Removed " + numberOfMessagesRemoved + " Messages From Node Queue Addressed to Queue " + destinationQueueName);
    }

}
