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

import java.util.List;

/**
 * Message status to keep track in which state message is
 */
public enum MessageStatus {
    /**
     * Message has been read from store
     */
    READ,
    /**
     * Message has been buffered for delivery
     */
    BUFFERED,
    /**
     * Message has been sent to its routed consumer
     */
    SENT,
    /**
     * In a topic scenario, message has been sent to all subscribers
     */
    SENT_TO_ALL,
    /**
     * The consumer has acknowledged receipt of the message
     */
    ACKED,
    /**
     * In a topic scenario, all subscribed consumers have acknowledged receipt of message
     */
    ACKED_BY_ALL,
    /**
     * Consumer has rejected the message ad it has been buffered again for delivery (possibly to another waiting
     * consumer)
     */
    REJECTED_AND_BUFFERED,
    /**
     * Message has been added to the final async delivery queue (deliverAsynchronously method has been called for
     * the message.)
     */
    SCHEDULED_TO_SEND,
    /**
     * Message has passed all the delivery rules and is eligible to be sent.
     */
    DELIVERY_OK,
    /**
     * Message did not align with one or more delivery rules, and has not been sent.
     */
    DELIVERY_REJECT,
    /**
     * Message has been sent more than once.
     */
    RESENT,
    /**
     * All messages of the slot containing this message have been handled successfully, causing it to be removed
     */
    SLOT_REMOVED,
    /**
     * Message has expired (JMS Expiration duration sent with the message has passed)
     */
    EXPIRED,
    /**
     * Message is moved to the DLC queue
     */
    DLC_MESSAGE,
    /**
     * Message has been cleared from delivery due to a queue purge event.
     */
    PURGED;

    /**
     * Is OK to remove tracking message
     *
     * @return eligibility to remove
     */
    public static boolean isOKToRemove(List<MessageStatus> messageStatus) {
        return (messageStatus.contains(MessageStatus.ACKED_BY_ALL) || messageStatus.contains(MessageStatus.EXPIRED)
                || messageStatus.contains(MessageStatus.DLC_MESSAGE));
    }
}
