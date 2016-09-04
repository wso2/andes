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

package org.wso2.andes.kernel;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.io.Serializable;

/**
 * This class represents a binding required for AMQP transport. Binding
 * represents the mapping between {@link org.wso2.andes.kernel.subscription.StorageQueue} and
 * {@link org.wso2.andes.kernel.router.AndesMessageRouter}.
 */
public class AndesBinding implements Serializable {

    private String messageRouterName;
    private String bindingKey;
    private StorageQueue boundQueue;

    /**
     * create an instance of andes binding
     *
     * @param messageRouterName name of exchange binding carries
     * @param boundQueue        name of the queue bound
     * @param bindingKey        routing key of the binding
     */
    public AndesBinding(String messageRouterName, StorageQueue boundQueue, String bindingKey) {
        this.messageRouterName = messageRouterName;
        this.boundQueue = boundQueue;
        this.bindingKey = bindingKey;
    }

    /**
     * Get name of message router binding is for
     *
     * @return name of message router binding
     */
    public String getMessageRouterName() {
        return messageRouterName;
    }

    /**
     * Get binding key of the binding to the router
     *
     * @return binding key
     */
    public String getBindingKey() {
        return bindingKey;
    }

    /**
     * Get queue binding binds
     *
     * @return StorageQueue
     */
    public StorageQueue getBoundQueue() {
        return boundQueue;
    }

    /**
     * create an instance of andes binding
     *
     * @param bindingAsStr binding information as encoded string
     */
    public AndesBinding(String bindingAsStr) throws AndesException {
        String[] propertyToken = bindingAsStr.split(",");
        for (String pt : propertyToken) {
            String[] tokens = pt.split("=");
            switch (tokens[0]) {
                case "boundMessageRouter":
                    this.messageRouterName = tokens[1];
                    break;
                case "boundQueueName":
                    this.boundQueue = AndesContext.getInstance().
                            getStorageQueueRegistry().getStorageQueue(tokens[1]);
                    if (null == boundQueue) {
                        throw new AndesException("Queue to bind is not found " + tokens[1]);
                    }
                    break;
                case "bindingKey":
                    this.bindingKey = tokens[1];
                    break;
            }
        }
    }

    /**
     * Encode object to a string.
     */
    public String encodeAsString() {
        return "boundMessageRouter=" + messageRouterName +
                ",boundQueueName=" + boundQueue.getName() +
                ",bindingKey=" + bindingKey;
    }

    public String toString() {
        return "[Binding]" + "E=" + messageRouterName +
                "/Q=" + boundQueue.getName() +
                "/RK=" + bindingKey +
                "/D=" + boundQueue.isDurable() +
                "/EX=" + boundQueue.isExclusive();
    }

    public boolean equals(Object o) {
        if (o instanceof AndesBinding) {
            AndesBinding c = (AndesBinding) o;
            if (this.messageRouterName.equals(c.messageRouterName) &&
                    this.boundQueue.getName().equals(c.boundQueue.getName()) &&
                    this.bindingKey.equals(c.bindingKey)) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(messageRouterName).
                append(boundQueue.getName()).
                append(bindingKey).
                toHashCode();
    }
}
