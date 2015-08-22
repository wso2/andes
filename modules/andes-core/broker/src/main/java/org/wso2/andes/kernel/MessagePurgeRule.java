/*
* Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.queue.QueueEntry;

/**
 * This class represents message purging Delivery Rule
 */
public class MessagePurgeRule implements DeliveryRule {
    private static Log log = LogFactory.getLog(MessagePurgeRule.class);

    /**
     * Used to get message information
     */
    private OnflightMessageTracker onflightMessageTracker;

    public MessagePurgeRule() {
        onflightMessageTracker = OnflightMessageTracker.getInstance();
    }

    /**
     * Evaluating the message purge delivery rule
     *
     * @return isOKToDelivery
     * @throws AndesException
     */
    @Override
    public boolean evaluate(QueueEntry message) throws AndesException {
        long messageID = message.getMessage().getMessageNumber();
        // Get last purged timestamp of the destination queue.
        String messageDestination = message.getAndesMessageReference().getTrackingData().destination;
        long lastPurgedTimestampOfQueue = MessageFlusher.getInstance().
                getMessageDeliveryInfo(messageDestination).getLastPurgedTimestamp();
        long arrivalTime = message.getAndesMessageReference().getTrackingData().arrivalTime;
        if (arrivalTime <= lastPurgedTimestampOfQueue) {
            log.warn("Message was sent at " + arrivalTime + "before last purge event at "
                    + lastPurgedTimestampOfQueue + ". Therefore, it will not be sent. id= "
                    + messageID);
            message.getAndesMessageReference().getTrackingData().addMessageStatus(MessageStatus.PURGED);
            return false;
        } else {
            return true;
        }
    }
}
