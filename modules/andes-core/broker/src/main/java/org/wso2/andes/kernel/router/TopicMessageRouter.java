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

import java.util.List;
import java.util.Set;

/**
 * Message router for roting AMQP topic messages
 */
public class TopicMessageRouter extends AndesMessageRouter {

    /**
     * Matcher used to match topic routing keys and identify
     * storage queues to enqueue messages considering their
     * binding keys
     */
    private TopicRoutingMatcher topicMatcher;

    /**
     * Create a TopicMessageRouter. This has specific message routing for
     * AMQP topic messages, matching of topic subscriptions based on wildcards etc.
     *
     * @param name       name of the router
     * @param type       type of the router
     * @param autoDelete if router should be auto-delete
     */
    public TopicMessageRouter(String name, String type, boolean autoDelete) {
        super(name, type, autoDelete);
        this.topicMatcher = new TopicRoutingMatcher(ProtocolType.AMQP);
    }

    /**
     * Create a TopicMessageRouter based on encoded routing information
     *
     * @param encodedRouterInfo encoded routing as string
     */
    public TopicMessageRouter(String encodedRouterInfo) {
        super(encodedRouterInfo);
        this.topicMatcher = new TopicRoutingMatcher(ProtocolType.AMQP);
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
        if (queue.getBoundSubscriptions().isEmpty() && !queue.isDurable()) {
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
     * {@inheritDoc}
     */
    @Override
    public Set<StorageQueue> getMatchingStorageQueues(AndesMessage incomingMessage) {
        String messageRoutingKey = incomingMessage.getMetadata().getDestination();
        return topicMatcher.getMatchingStorageQueues(messageRoutingKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onBindingQueue(StorageQueue queue) throws AndesException {
        topicMatcher.addStorageQueue(queue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onUnbindingQueue(StorageQueue queue) {
        topicMatcher.removeStorageQueue(queue);
    }

}
