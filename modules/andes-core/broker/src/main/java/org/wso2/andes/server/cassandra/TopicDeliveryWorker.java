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
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * <code>TopicDeliveryWorker</code>
 * Handle the task of publishing messages to all the subscribers
 * of a topic
 */
public class TopicDeliveryWorker extends Thread {
    private long lastDeliveredMessageID = 0;
    private boolean working = false;
    private String id;
    private String topicNodeQueueName;
    private SubscriptionStore subscriptionStore;

    private SequentialThreadPoolExecutor messagePublishingExecutor = null;

    private static Log log = LogFactory.getLog(TopicDeliveryWorker.class);

    public TopicDeliveryWorker() {

        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        this.topicNodeQueueName = AndesUtils.getTopicNodeQueueName();
        this.id = topicNodeQueueName;
        messagePublishingExecutor = new SequentialThreadPoolExecutor((ClusterResourceHolder.getInstance().getClusterConfiguration().
                getPublisherPoolSize()), "TopicMessagePublishingExecutor");
        this.start();
        this.setWorking();
    }

    /**
     * 1. Get messages for the queue from last delivered message id
     * 2. Enqueue the retrived message to the queue
     * 3. Remove delivered messaged IDs from the data base
     */
    @Override
    public void run() {
           //todo: hasitha - reimplement or merge with Queue Delivery Worker
    }

    /**
     * Enqueue a given message to all subscriber queues bound to TOPIC_EXCHANGE matching with routing key
     *
     * @param message AMQ message
     */
    private void enqueueMessage(AndesMessageMetadata message) {
        try {
            /**
             * There can be more than one binding to the same topic
             * We need to publish the message to the exact matching queues
             * */
            String routingKey = message.getDestination();
            Collection<LocalSubscription> localSubscribersForTopic = subscriptionStore.getActiveLocalSubscribers(routingKey, true);
            for (LocalSubscription subscription : localSubscribersForTopic) {
                deliverAsynchronously(subscription, message);
            }
        } catch (AndesException e) {
            //TODO:hasitha - do not we have to re-try?
        }
    }

    private void deliverAsynchronously(final LocalSubscription subscription, final AndesMessageMetadata message) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    if (subscription.isActive()) {
                        if (MessageExpirationWorker.isExpired(message.getExpirationTime())) {
                            return;
                        }
                        (subscription).sendMessageToSubscriber(message);
                    }
                } catch (Throwable e) {
                    log.error("Error while delivering message ", e);
                }
            }
        };
        messagePublishingExecutor.submit(r, (subscription.getTargetQueue() + subscription.getSubscriptionID()).hashCode());
    }

    /**
     * get if topic delivery task active
     *
     * @return isWorking
     */
    public boolean isWorking() {
        return working;
    }

    /**
     * set topic delivery task active
     */
    public void setWorking() {
        working = true;
    }

    public void stopWorking() {
        working = false;
    }
}
