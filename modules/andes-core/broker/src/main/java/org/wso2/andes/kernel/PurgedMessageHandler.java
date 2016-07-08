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

public class PurgedMessageHandler implements DeliveryResponsibility {

    private static Log log = LogFactory.getLog(PurgedMessageHandler.class);


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
    public void handleDeliveryMessage(LocalSubscription subscription, DeliverableAndesMetadata message)
            throws AndesException{

        // Get last purged timestamp of the destination queue.
        long lastPurgedTimestampOfQueue =
                MessageFlusher.getInstance().getMessageDeliveryInfo(message.getDestination(),
                        subscription.getProtocolType(), subscription.getDestinationType())
                        .getLastPurgedTimestamp();

        if (message.getArrivalTime() <= lastPurgedTimestampOfQueue) {

            log.warn("Message was sent at " + message.getArrivalTime()
                    + " before last purge event at " + lastPurgedTimestampOfQueue
                    + ". Therefore, it will not be sent. id= "
                    + message.getMessageID());
            if(!message.isPurgedOrDeletedOrExpired()) {
                message.markAsPurgedMessage();
            }
        } else {
            nextDeliveryResponsibility.handleDeliveryMessage(subscription,message);
        }

    }
}
