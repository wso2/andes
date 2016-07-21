/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel.router;


import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Message router for routing MQTT protocol messages
 */
public class MQTTMessageRouter extends AndesMessageRouter {

    /**
     * Matcher used to match topic routing keys and identify
     * storage queues to enqueue messages considering their
     * binding keys (considering wildcards etc)
     */
    private TopicRoutingMatcher topicMatcher;

    /**
     * Create a MQTTMessageRouter. This has specific message routing for
     * MQTT messages, matching of topic subscriptions based on wildcards.
     *
     * @param name       name of the router
     * @param type       type of the router
     * @param autoDelete if router should be auto-delete
     */
    public MQTTMessageRouter(String name, String type, boolean autoDelete) {
        super(name, type, autoDelete);
        this.topicMatcher = new TopicRoutingMatcher(ProtocolType.MQTT);
    }

    /**
     * Create a MQTTMessageRouter based on encoded routing information
     *
     * @param encodedRouterInfo encoded routing as string
     */
    public MQTTMessageRouter(String encodedRouterInfo) {
        super(encodedRouterInfo);
        this.topicMatcher = new TopicRoutingMatcher(ProtocolType.MQTT);
    }

    /**
     * Unbind queue from topic message router. Here following are considered
     * 1. Subscriptions share storage queue. Thus unbind should only be done
     * if there is no active subscriber bound to queue
     * 2. If queue is durable, this is for a durable topic subscription. Thus
     * do not unbind the queue so that MB will collect messages for that queue
     *
     * @param bindingKey binding key
     * @param queue      storage queue to unbind
     * @throws AndesException
     */
    public void removeMapping(String bindingKey, StorageQueue queue) throws AndesException {
        if (queue.getBoundedSubscriptions().isEmpty()) {
            List<StorageQueue> boundQueues = routingMap.get(bindingKey);
            if (boundQueues != null && !boundQueues.isEmpty()) {
                boundQueues.remove(queue);
            }
            onUnbindingQueue(queue);

            if (autoDelete) {
                AndesContext.getInstance().getMessageRouterRegistry().removeMessageRouter(name);
            }
        }
    }

    /**
     * Get a set of queues matched to routing key.
     * This matcher will use MQTT specific wildcards and do the matching.
     *
     * @param incomingMessage message to route
     * @return Set of StorageQueues matched
     */
    @Override
    public Set<StorageQueue> getMatchingStorageQueues(AndesMessage incomingMessage) {
        String messageRoutingKey = incomingMessage.getMetadata().getDestination();
        int qosLevel = incomingMessage.getMetadata().getQosLevel();
        Set<StorageQueue> matchingQueues = topicMatcher.getMatchingStorageQueues(messageRoutingKey);
        /*
         * QOS level 0 messages should not be persisted for subscribers with clean session = false who are inactive
         * at the moment
         */
        if(0 == qosLevel) {
            Iterator<StorageQueue> queueIterator = matchingQueues.iterator();
            while (queueIterator.hasNext()) {
                StorageQueue matchingQueue = queueIterator.next();
                if(matchingQueue.isDurable() && matchingQueue.getBoundedSubscriptions().isEmpty()) {
                    queueIterator.remove();
                }
            }
        }
        return matchingQueues;
    }

    /**
     * Perform on binding a new queue to message router
     *
     * @param queue queue bound
     * @throws AndesException
     */
    @Override
    public void onBindingQueue(StorageQueue queue) throws AndesException {
        topicMatcher.addStorageQueue(queue);
    }

    /**
     * Perform on unbinding a queue from message router
     *
     * @param queue queue un bound
     */
    @Override
    public void onUnbindingQueue(StorageQueue queue) {
        topicMatcher.removeStorageQueue(queue);
    }
}
