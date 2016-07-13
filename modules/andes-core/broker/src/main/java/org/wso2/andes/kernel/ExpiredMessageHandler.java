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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.subscription.LocalSubscription;

/**
 * This class skips the expired messages from delivery and add them into a set for
 * a batch delete
 */
public class ExpiredMessageHandler implements DeliveryResponsibility {

    private static Log log = LogFactory.getLog(ExpiredMessageHandler.class);


    private DeliveryResponsibility nextDeliveryResponsibility;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNextDeliveryFilter(DeliveryResponsibility deliveryFilter) {
        this.nextDeliveryResponsibility = deliveryFilter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleDeliveryMessage(LocalSubscription subscription, DeliverableAndesMetadata message) throws AndesException {

        //Check if destination entry has expired. Any expired message will not be delivered
        if (message.isExpired()) {
            log.warn("Message is expired. Therefore, it will not be sent. : id= " + message.getMessageID());
            //since this message is not going to be delivered, no point in wait for ack
            message.getSlot().decrementPendingMessageCount();
            //add the expired messages to a list for a batch delete
            //TODO: Is it better to have a listener instead of task
            SetBasedExpiryMessageDeletionTask.getExpiredMessageSet().add(message);
        } else {
            nextDeliveryResponsibility.handleDeliveryMessage(subscription, message);
        }
    }


}
