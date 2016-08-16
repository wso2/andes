/*
 * Copyright (c)2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.cluster.coordination;

import org.wso2.andes.kernel.BindingListener;
import org.wso2.andes.kernel.ExchangeListener;
import org.wso2.andes.kernel.QueueListener;
import org.wso2.andes.kernel.SubscriptionListener;

/**
 * StandaloneEventListenerCreator create listeners(Queue, Binding, Exchange and Subscription) that will work in the
 * standalone mode.
 */
public class StandaloneEventListenerCreator implements EventListenerCreator {

    @Override
    public QueueListener getQueueListener() {
        return new QueueListener(new StandaloneClusterNotificationPublisher());
    }

    @Override
    public ExchangeListener getExchangeListener() {
        return new ExchangeListener(new StandaloneClusterNotificationPublisher());
    }

    @Override
    public BindingListener getBindingListener() {
        return new BindingListener(new StandaloneClusterNotificationPublisher());
    }

    @Override
    public SubscriptionListener getSubscriptionListener() {
        return new SubscriptionListener(new StandaloneClusterNotificationPublisher());
    }
}
