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

package org.wso2.andes.server.cluster.coordination;

import org.wso2.andes.kernel.BindingListener;
import org.wso2.andes.kernel.ExchangeListener;
import org.wso2.andes.kernel.QueueListener;
import org.wso2.andes.kernel.SubscriptionListener;

/**
 * EventListenerCreator provides methods to create queue/binding/exchange/subscription listener depending on the
 * underlying cluster even sync mode.
 */
public interface EventListenerCreator {

    /**
     * Create a new Queue listener and return it.
     *
     * @return a QueueListener object.
     */
    QueueListener getQueueListener();

    /**
     * Create a new Exchange listener and return it.
     *
     * @return a ExchangeListener object.
     */
    ExchangeListener getExchangeListener();

    /**
     * Create a new binding listener and return it.
     *
     * @return a BindingListener object.
     */
    BindingListener getBindingListener();

    /**
     * Create a new Subscription listener and return it.
     *
     * @return a SubscriptionListener object.
     */
    SubscriptionListener getSubscriptionListener();

}
