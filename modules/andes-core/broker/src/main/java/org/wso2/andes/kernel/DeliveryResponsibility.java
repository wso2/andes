/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
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

package org.wso2.andes.kernel;


import org.wso2.andes.subscription.LocalSubscription;

/**
 * Delivery responsibility represents the constrains that needs to be checked
 * Chain of responsibility pattern is used for handling the delivery responsibilities since there are
 * chain of tasks that need to be done before deliver the message
 */
public abstract class DeliveryResponsibility {

    /**
     * Holds the next delivery responsibility
     */
    protected DeliveryResponsibility nextDeliveryResponsibility;

    /**
     * Set the next delivery responsibility
     * @param deliveryResponsibility
     */
    public void setNextDeliveryFilter(DeliveryResponsibility deliveryResponsibility){
        this.nextDeliveryResponsibility = deliveryResponsibility;
    }

    /**
     * Handover the message to the next responsibility if current responsibility successfully finished
     * @param subscription
     * @param message
     */
    public void handleDeliveryMessage(LocalSubscription subscription, DeliverableAndesMetadata message)
            throws AndesException{

        if(null != nextDeliveryResponsibility && performResponsibility(subscription,message)){
            nextDeliveryResponsibility.handleDeliveryMessage(subscription,message);
        }

    }

    /** //todo discribe parameters and return statement
     * Responsibility specific logic that can decide the message pass to the next responsibility
     * @param subscription
     * @param message
     * @return is Okay to hand over to the next responsibility in the chain
     */
    protected abstract boolean performResponsibility (LocalSubscription subscription,
                                                      DeliverableAndesMetadata message) throws AndesException;


}
