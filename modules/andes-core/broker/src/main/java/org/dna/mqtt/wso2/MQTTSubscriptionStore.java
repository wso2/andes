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
import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.SubscriptionsStore;

import java.util.HashMap;
import java.util.Map;

/**
 * Will handle new subscriptions bound through andes cluster, we extent the subscription store since we need to
 * partially use its functionality.
 * {@link org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.SubscriptionsStore}
 */
public class MQTTSubscriptionStore extends SubscriptionsStore {

    /**
     * Key = the name of the topic
     * Value = the subscription/s represented through the topic
     */
    private Map<String, Subscribers> localSubscriptions = new HashMap<String, Subscribers>();

    /**
     * Would include the subscription to the list so that this could be used when sending the message out
     *
     * @param newSubscription the subscription which holds the channel information
     */
    private void addLocalSubscription(Subscription newSubscription) {
        String topic = newSubscription.getTopic();
        String clientID = newSubscription.getClientId();

        Subscribers subscribers = localSubscriptions.get(topic);

        if (null == subscribers) {
            Subscribers subscriber = new Subscribers();
            subscriber.addNewSubscriber(clientID, newSubscription);
            localSubscriptions.put(topic, subscriber);
        } else {
            subscribers.addNewSubscriber(clientID, newSubscription);
        }

    }

    @Override
    protected void addDirect(Subscription newSubscription) {
        addLocalSubscription(newSubscription);
    }

    @Override
    public void add(Subscription newSubscription) {
        addLocalSubscription(newSubscription);
    }

    @Override
    public void removeSubscription(String topic, String clientID) {
        Subscribers subscribers = localSubscriptions.get(topic);
        subscribers.removeSubscription(clientID);
    }

    @Override
    public Subscription getSubscriptions(String topic, String clientID) {
        Subscribers subscribers = localSubscriptions.get(topic);
        return subscribers.getSubscriptionFromClientID(clientID);
    }

    @Override
    public void clearAllSubscriptions() {
        localSubscriptions.clear();
    }
}
