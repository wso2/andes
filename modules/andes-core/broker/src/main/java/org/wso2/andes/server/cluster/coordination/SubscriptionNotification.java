/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesExchange;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.SubscriptionListener.SubscriptionChange;

import java.io.Serializable;

/**
 * When a cluster wide subscription is changed, the information regarding the subscription change is
 * communicated using SubscriptionNotification instances.
 */
public class SubscriptionNotification implements Serializable {
    private AndesExchange andesExchange;
    private AndesBinding andesBinding;
    private AndesQueue andesQueue;
    private SubscriptionChange status;
    private boolean isDurable;
    private String encodedString;

    /**
     * Initialize SubscriptionNotification.
     *
     * @param exchangeName          Name of the exchange
     * @param exchangeType          Type of the exchange
     * @param exchangeAutoDeletable Is the exchange auto deletable
     * @param change                Is it a subscription addition, or deletion or disconnection
     * @param queue                 Belonging queue name
     * @param queueOwner            Owner of the queue
     * @param isExclusive           Is exclusive
     * @param isDurable             Is durable topic or a queue
     * @param destination           destination name
     * @param encodedString         encoded string which contains all information about the subscription.
     */
    public SubscriptionNotification(String exchangeName, String exchangeType,
                                    Short exchangeAutoDeletable, SubscriptionChange change, String queue,
                                    String queueOwner, boolean isExclusive, boolean isDurable,
                                    String destination, String encodedString) {
        this.andesExchange = new AndesExchange(exchangeName, exchangeType, exchangeAutoDeletable);
        this.andesQueue = new AndesQueue(queue, queueOwner, isExclusive, isDurable);
        this.andesBinding = new AndesBinding(exchangeName, this.andesQueue, destination);
        this.isDurable = isDurable;
        this.status = change;
        this.encodedString = encodedString;
    }

    /**
     * Get AndesExchange used by the subscription
     *
     * @return
     */
    public AndesExchange getAndesExchange() {
        return this.andesExchange;
    }

    /**
     * Get AndesQueue used by the subscription
     *
     * @return
     */
    public AndesQueue getAndesQueue() {
        return this.andesQueue;
    }

    /**
     * Get AndesBinding used by the subscription
     *
     * @return
     */
    public AndesBinding getAndesBinding() {
        return this.andesBinding;
    }

    /**
     * Get the type of change happen to the subscription (Added, deleted or disconnected)
     *
     * @return
     */
    public SubscriptionChange getStatus() {
        return this.status;
    }

    /**
     * Is the subscription to a durable topic or to a queue.
     *
     * @return
     */
    public boolean isDurable() {
        return this.isDurable;
    }

    /**
     * Get encoded string of the subscription
     *
     * @return
     */
    public String getEncodedString() {
        return this.encodedString;
    }
}
