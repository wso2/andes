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

public class SubscriptionNotification implements Serializable {
    private AndesExchange andesExchange;
    private AndesBinding andesBinding;
    private AndesQueue andesQueue;
    private  SubscriptionChange status;
    private boolean isDurable;
    private String subscribedDestination;
    private String encodedString;


    public SubscriptionNotification(String exchangeName, String exchangeType,
                                    Short exchangeAutoDeletable, SubscriptionChange change, String queue,
                                    String queueOwner, boolean isExclusive, boolean isDurable,
                                    String destination, String encodedString){
        this.andesExchange = new AndesExchange(exchangeName, exchangeType, exchangeAutoDeletable);
        this.andesQueue = new AndesQueue(queue, queueOwner, isExclusive, isDurable);
        this.andesBinding = new AndesBinding(exchangeName, this.andesQueue, destination);
        this.isDurable = isDurable;
        this.status = change;
        this.subscribedDestination = destination;
        this.encodedString = encodedString;
    }

    public AndesExchange getAndesExchange() {
        return this.andesExchange;
    }

    public AndesQueue getAndesQueue() {
        return  this.andesQueue;
    }

    public AndesBinding getAndesBinding() {
        return this.andesBinding;
    }

    public SubscriptionChange getStatus() {
        return this.status;
    }

    public boolean isDurable() {
        return this.isDurable;
    }

    public String getSubscribedDestination(){
        return this.subscribedDestination;
    }

    public String getEncodedString(){
        return this.encodedString;
    }
}
