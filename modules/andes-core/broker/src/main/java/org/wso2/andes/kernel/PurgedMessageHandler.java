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
 * PurgedMessageHandler skips the purged messages from the delivery path and
 * handover the un-purged messages to the next delivery responsibility
 */
public class PurgedMessageHandler extends DeliveryResponsibility {

    private static Log log = LogFactory.getLog(PurgedMessageHandler.class);

    /**
     * Check the purged message and skip those message from delivery
     * {@inheritDoc}
     */
    @Override
    protected boolean performResponsibility(LocalSubscription subscription,
            DeliverableAndesMetadata message) throws AndesException {
        // Get last purged timestamp of the destination queue.
        MessageDeliveryInfo deliveryInfo = MessageFlusher.getInstance().getMessageDeliveryInfo(message.getDestination(),
                subscription.getDestinationType());
        //This check is done as a temporary fix. This method should not at all be called if delivery information is
        // not there.
        if (null != deliveryInfo) {
            long lastPurgedTimestampOfQueue = deliveryInfo.getLastPurgedTimestamp();
            if (message.getArrivalTime() <= lastPurgedTimestampOfQueue) {
                log.warn("Message was sent at " + message.getArrivalTime()
                         + " before last purge event at " + lastPurgedTimestampOfQueue
                         + ". Therefore, it will not be sent. id= "
                         + message.getMessageID());
                if (!message.isPurgedOrDeletedOrExpired()) {
                    message.markAsPurgedMessage();
                }
                return false;
            }
        }
        return true;
    }
}
