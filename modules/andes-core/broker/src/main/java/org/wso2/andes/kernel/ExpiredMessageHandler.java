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
 * ExpiredMessageHandler skips the expired messages from delivery and add them into a set for
 * a batch delete
 */
public class ExpiredMessageHandler extends DeliveryResponsibility {

    private static Log log = LogFactory.getLog(ExpiredMessageHandler.class);

    // Hold the pre delivery expiry message deletion task
    private PreDeliveryExpiryMessageDeletionTask preDeliveryExpiryMessageDeletionTask;

    /**
     * Set the expiry message deletion task
     * @param task Deletion task
     */
    public void setExpiryMessageDeletionTask(PreDeliveryExpiryMessageDeletionTask task){
        this.preDeliveryExpiryMessageDeletionTask = task;
    }


    /**
     * Check the messages for expiry and add them in a set for a batch delete if expired
     * {@inheritDoc}
     */
    @Override
    protected boolean performResponsibility(LocalSubscription subscription,
                                            DeliverableAndesMetadata message) throws AndesException {
        boolean isOkayToProceed = false;
        //Check if destination entry has expired. Any expired message will not be delivered
        if (message.isExpired()) {
            log.warn("Message is expired. Therefore, it will not be sent. : id= " + message.getMessageID());
            //since this message is not going to be delivered, no point in wait for ack
            message.getSlot().decrementPendingMessageCount();
            //add the expired messages to a list for a batch delete
            preDeliveryExpiryMessageDeletionTask.addMessageIdToExpiredQueue(message.getMessageID());
        } else {
           isOkayToProceed = true;
        }
        return isOkayToProceed;
    }


}
