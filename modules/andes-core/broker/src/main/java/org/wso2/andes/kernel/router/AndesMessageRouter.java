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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Andes Message Router is responsible for routing messages
 * to relevant storage queues inside andes kernel. It is abstract and
 * extended implementations has specific message routing functionality.
 */
public abstract class AndesMessageRouter {

    /**
     * We handle the topic case by sharing messages of storage queue between subscribers
     * for same node. Thus for a node, for a given routing key, there will be one storage queue. But
     * there can be durable topic subscribers for same topic. Hence multiple.
     */
    protected Map<String, List<StorageQueue>> routingMap;

    /**
     * Name of the exchange
     */
    protected String name;

    /**
     * Type of the exchange.
     */
    protected String type;

    /**
     * Auto-delete flag. true if router is removed automatically
     * when all queues are detached
     */
    protected boolean autoDelete;

    /**
     * Create an instance of Message Router
     *
     * @param name       name of the message router
     * @param type       type of the router
     * @param autoDelete true if router is removed automatically when all queues are detached
     */
    public AndesMessageRouter(String name, String type, boolean autoDelete) {
        this.name = name;
        this.type = type;
        this.autoDelete = autoDelete;
        this.routingMap = new HashMap<>();
    }

    /**
     * Create an instance of Message Router
     *
     * @param messageRouterAsStr message router as encoded string
     */
    public AndesMessageRouter(String messageRouterAsStr) {
        String[] propertyToken = messageRouterAsStr.split(",");
        for (String pt : propertyToken) {
            String[] tokens = pt.split("=");
            switch (tokens[0]) {
                case "messageRouterName":
                    this.name = tokens[1];
                    break;
                case "type":
                    this.type = tokens[1];
                    break;
                case "autoDelete":
                    this.autoDelete = Boolean.parseBoolean(tokens[1]);
                    break;
            }
        }
        this.routingMap = new HashMap<>();
    }


    public String getName() {
        return name;
    }


    public String getType() {
        return type;
    }

    /**
     * Get if message router is auto-deletable (delete when all queues are detached)
     *
     * @return true if auto-deletable
     */
    public boolean isAutoDelete() {
        return autoDelete;
    }

    /**
     * Bind a given queue to message router
     *
     * @param bindingKey binding key by which queue is bound. Message router will
     *                   evaluate eligibility of routing the incoming messages to queue
     *                   using this key.
     * @param queue      queue to bind to the router
     * @throws AndesException
     */
    public void addMapping(String bindingKey, StorageQueue queue) throws AndesException {
        List<StorageQueue> boundQueues = routingMap.get(bindingKey);
        if (null == boundQueues) {
            List<StorageQueue> boundedQueues = new ArrayList<>(1);
            boundedQueues.add(queue);
            routingMap.put(bindingKey, boundedQueues);
        } else {
            boundQueues.add(queue);
        }
        onBindingQueue(queue);
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
        if (queue.getBoundSubscriptions().isEmpty()) {
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
     * Get all different binding keys queues are
     * bound to this message router
     *
     * @return list of binding keys
     */
    public List<String> getAllBindingKeys() {
        return new ArrayList<>(routingMap.keySet());
    }

    /**
     * Get all queues bound to this message router
     *
     * @return List of queues
     */
    public List<StorageQueue> getAllBoundQueues() {
        List<StorageQueue> queues = new ArrayList<>();
        for (List<StorageQueue> storageQueues : routingMap.values()) {
            for (StorageQueue storageQueue : storageQueues) {
                queues.add(storageQueue);
            }
        }
        return queues;
    }

    /**
     * Get names of all queues bound to this message router
     *
     * @return List of queues
     */
    public List<String> getNamesOfAllQueuesBound() {
        List<String> queueNames = new ArrayList<>();
        List<StorageQueue> queuesList = getAllBoundQueues();
        for (StorageQueue queue : queuesList) {
            queueNames.add(queue.getName());
        }
        return queueNames;
    }

    /**
     * Get list of storage queues a message with given routing key should be
     * enqueued. This logic is specific to the routing implementation.
     * @param incomingMessage message to be routed
     * @return a set of matching queues
     */
    public abstract Set<StorageQueue> getMatchingStorageQueues(AndesMessage incomingMessage);

    /**
     * Perform task when a queue is bound to message router
     *
     * @param queue queue bound
     * @throws AndesException
     */
    public abstract void onBindingQueue(StorageQueue queue) throws AndesException;

    /**
     * Perform task when a queue is un-bound
     * from message router
     *
     * @param queue queue un bound
     */
    public abstract void onUnbindingQueue(StorageQueue queue);


    /**
     * Encode object to a String representation
     *
     * @return String with message router information.
     */
    public String encodeAsString() {
        return "messageRouterName=" + name
                + ",type=" + type
                + ",autoDelete=" + autoDelete;
    }

    public String toString() {
        return encodeAsString();
    }

    public boolean equals(Object o) {
        if (o instanceof AndesMessageRouter) {
            AndesMessageRouter c = (AndesMessageRouter) o;
            if (this.name.equals(c.name) &&
                    this.type.equals(c.type) &&
                    this.autoDelete == c.autoDelete) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(name).
                append(type).
                append(autoDelete).
                toHashCode();
    }
}
