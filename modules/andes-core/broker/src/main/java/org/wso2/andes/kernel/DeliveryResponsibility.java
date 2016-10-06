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


import org.wso2.andes.kernel.subscription.AndesSubscription;

/**
 * Delivery responsibility represents the constrains that needs to be checked.Chain of responsibility pattern is used
 * for handling the delivery responsibilities since there are chain of tasks that need to be done before deliver the
 * message.
 */
public abstract class DeliveryResponsibility {

    /**
     * Holds the next delivery responsibility.
     */
    protected DeliveryResponsibility nextDeliveryResponsibility;

    /**
     * Set the next delivery responsibility.
     *
     * @param deliveryResponsibility responsibility that need to be performed in the delivery path
     */
    public void setNextDeliveryFilter(DeliveryResponsibility deliveryResponsibility){
        this.nextDeliveryResponsibility = deliveryResponsibility;
    }

    /**
     * Handover the message to the next responsibility if current responsibility successfully finished.
     *
     * @param subscription subscription to which message should be delivered
     * @param message message to deliver
     */
    public void handleDeliveryMessage(AndesSubscription subscription, DeliverableAndesMetadata message)
            throws AndesException{

        if(performResponsibility(subscription,message) && (null != nextDeliveryResponsibility)){
            nextDeliveryResponsibility.handleDeliveryMessage(subscription,message);
        }

    }

    /**
     * Responsibility specific logic that can decide the message pass to the next responsibility.
     *
     * @param subscription subscription to which message should be delivered
     * @param message message to deliver
     * @return true if it is okay to hand over to the next responsibility in the chain
     */
    protected abstract boolean performResponsibility (AndesSubscription subscription,
                                                      DeliverableAndesMetadata message) throws AndesException;


}
