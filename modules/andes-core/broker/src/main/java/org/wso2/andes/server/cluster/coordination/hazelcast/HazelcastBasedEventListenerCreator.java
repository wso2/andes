/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 *
 */

package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.ITopic;
import org.wso2.andes.kernel.BindingListener;
import org.wso2.andes.kernel.ExchangeListener;
import org.wso2.andes.kernel.QueueListener;
import org.wso2.andes.kernel.SubscriptionListener;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.EventListenerCreator;

/**
 * The HazelcastBasedEventListenerCreator class provides methods to create cluster event listeners based on the
 * respective channel.
 */
public class HazelcastBasedEventListenerCreator implements EventListenerCreator {

    /**
     * The hazelcast topics the listeners will be publishing to.
     */
    private static ITopic<ClusterNotification> queueChannel;
    private static ITopic<ClusterNotification> exchangeChannel;
    private static ITopic<ClusterNotification> bindingChannel;
    private static ITopic<ClusterNotification> subscriptionChannel;

    public static void setQueueChannel(ITopic<ClusterNotification> channel) {
        queueChannel = channel;
    }

    public static void setExchangeChannel(ITopic<ClusterNotification> channel) {
        exchangeChannel = channel;
    }

    public static void setBindingChannel(ITopic<ClusterNotification> channel) {
        bindingChannel = channel;
    }

    public static void setSubscriptionChannel(ITopic<ClusterNotification> channel) {
        subscriptionChannel = channel;
    }

    /**
     * Create a new QueueListener which will be publishing to a previously set queue notification channel.
     * {@inheritDoc}
     */
    @Override
    public QueueListener getQueueListener() {
        return new QueueListener(new HazelcastBasedClusterNotificationPublisher(queueChannel));
    }

    /**
     * Create a new ExchangeListener which will be publishing to a previously set exchange notification channel.
     * {@inheritDoc}
     */
    @Override
    public ExchangeListener getExchangeListener() {
        return new ExchangeListener(new HazelcastBasedClusterNotificationPublisher(exchangeChannel));
    }

    /**
     * Create a new BindingListener which will be publishing to a previously set binding notification channel.
     * {@inheritDoc}
     */
    @Override
    public BindingListener getBindingListener() {
        return new BindingListener(new HazelcastBasedClusterNotificationPublisher(bindingChannel));
    }

    /**
     * Create a new SubscriptionListener which will be publishing to a previously set subscription notification channel.
     * {@inheritDoc}
     */
    @Override
    public SubscriptionListener getSubscriptionListener() {
        return new SubscriptionListener(new HazelcastBasedClusterNotificationPublisher(subscriptionChannel));
    }
}
