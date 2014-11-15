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


public interface AndesSubscription {
    /**
     * @return subscription ID of the subscription or null
     */
	public String getSubscriptionID();

    /**
     * @return routing key of the binding to whom subscription is made
     */
	public String getSubscribedDestination();

    /**
     * @return is queue of this subscription bound to any topic
     */
	public boolean isBoundToTopic();

    /**
     * @return is subscribed to a durable queue/binding
     */
	public boolean isDurable();

    /**
     * @return get the node from which subscription is made or null
     */
	public String getSubscribedNode();

    /**
     * @return is subscribed queue is exclusive
     */
	public boolean isExclusive();

	public void setExclusive(boolean isExclusive);

    /**
     * Encode the object as a string
     * @return  encoded string
     */
	public String encodeAsStr();

    /**
     * @return  subscribed queue name
     */
	public String getTargetQueue();

    /**
     * @return name of the queue in message store messages addressed to
     * this subscription is stored
     */
    public String getStorageQueueName();

    /**
     * @return owner of the subscribed queue
     */
    public String getTargetQueueOwner();

    /**
     * exchange subscribed queue is bound (for each binding we will be adding a subscription entry)
     * @return  exchange name subscribed queue is bound
     */
    public String getTargetQueueBoundExchangeName();

    /**
     *  exchange type subscribed queue is bound (for each binding we will be adding a subscription entry)
     * @return  exchange type subscribed queue is bound
     */
    public String getTargetQueueBoundExchangeType();

    /**
     * whether exchange is auto-deletable where subscribed queue is bound (for each binding we will be adding a subscription entry)
     * @return  whether exchange of the binding is auto-deletable
     */
    public Short ifTargetQueueBoundExchangeAutoDeletable();

    /**
     * @return  whether subscribed queue external subscription
     */
    public boolean hasExternalSubscriptions();
}
