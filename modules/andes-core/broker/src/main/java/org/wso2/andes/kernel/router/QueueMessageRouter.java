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
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Message router for selecting storage queues to route messages to
 */
public class QueueMessageRouter extends AndesMessageRouter {

    /**
     * Create a QueueMessageRouter instance
     *
     * @param name       name of the message router
     * @param type       type of the message router
     * @param autoDelete true if message router should be removed when all queues are unbound
     */
    public QueueMessageRouter(String name, String type, boolean autoDelete) {
        super(name, type, autoDelete);
    }

    /**
     * Create a QueueMessageRouter instance with string encoded information. Encoding is specified at
     * {@link AndesMessageRouter#encodeAsString()}
     *
     * @param encodedRouterInfo encoded information
     */
    public QueueMessageRouter(String encodedRouterInfo) {
        super(encodedRouterInfo);
    }


    /**
     * Remove queue from message router. This has to be done
     * only if queue has no subscriptions
     *
     * @param bindingKey binding key
     * @param queue      storage queue to unbind
     * @throws AndesException when there are active subscriptions
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
        } else {
            throw new AndesException("Cannot unbind a queue from message router " +
                    "which has active subscriptions");
        }
    }

    /**
     * Get all storage queues mapped to given routingKey
     *
     * @param incomingMessage message to route
     * @return matching storage queues
     */
    @Override
    public Set<StorageQueue> getMatchingStorageQueues(AndesMessage incomingMessage) {
        String messageRoutingKey = incomingMessage.getMetadata().getDestination();
        return new HashSet<>(routingMap.get(messageRoutingKey));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onBindingQueue(StorageQueue queue) throws AndesException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onUnbindingQueue(StorageQueue queue) {

    }
}
