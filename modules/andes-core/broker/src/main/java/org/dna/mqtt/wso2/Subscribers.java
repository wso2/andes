/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.dna.mqtt.wso2;

import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per topic, there will be multiple subscriptions for each new subscriber there will be an instance of this
 */
public class Subscribers {

    /**
     * Each topic can have multiple subscribers
     * Key = the client id, value = the subscription
     * We need to define a concurrent hash-map, since subscriptions will be added through one thread, read from another
     */
    private Map<String, Subscription> subscriptions = new ConcurrentHashMap<String, Subscription>();

    /**
     * Adds new subscription to the corresponding topic
     *
     * @param clientID   the id which will uniquely identify the subscriber
     * @param subscriber the subscription which holds the channel information
     */
    public void addNewSubscriber(String clientID, Subscription subscriber) {
        subscriptions.put(clientID, subscriber);
    }

    /**
     * Retrieve the subscription for a relevant client
     *
     * @param clientID the id of the client
     * @return the subscription
     */
    public Subscription getSubscriptionFromClientID(String clientID) {
        return subscriptions.get(clientID);
    }

    /**
     * Removes a subscription of the provided client id
     *
     * @param clientID the id of the client
     */
    public void removeSubscription(String clientID) {
        subscriptions.remove(clientID);
    }
}
