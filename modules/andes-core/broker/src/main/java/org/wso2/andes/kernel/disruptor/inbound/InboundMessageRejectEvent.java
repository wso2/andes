/*
 * Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

package org.wso2.andes.kernel.disruptor.inbound;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.UUID;

/**
 * Disruptor event handling message reject
 */
public class InboundMessageRejectEvent implements AndesInboundStateEvent {


    private static Log log = LogFactory.getLog(InboundMessageRejectEvent.class);
    /**
     * Channel event type handle by the event object
     */
    private EventType eventType;

    /**
     * Id of the message rejected by client
     */
    private long messageId;

    /**
     * Id of the channel reject is received
     */
    private UUID channelId;

    /**
     * Whether to re-queue the message back to the subscriber
     */
    private boolean reQueue;

    /**
     * This is to handle the messages beyond the actual rollback event, which are rejected from the client side
     * message buffer. True if the message was rejected after the last rollback event.
     */
    private boolean isMessageBeyondLastRollback;

    /**
     * Supported state events
     */
    public enum EventType {
        /**
         * Specific client channel close event
         */
        MESSAGE_REJECT_EVENT,
    }

    /**
     * Create an instance of message reject event
     *
     * @param messageId Id of the message rejected
     * @param channelId Id of the channel message reject received
     * @param reQueue   whether to re queue message back to subscriber
     */
    public InboundMessageRejectEvent(long messageId, UUID channelId, boolean reQueue) {
        this.messageId = messageId;
        this.channelId = channelId;
        this.reQueue = reQueue;
    }

    /**
     * Prepare to reject message. This is done prior to inset the
     * event to disruptor
     *
     * @param isMessageBeyondLastRollback True if the message was rejected after the last rollback event.
     */
    public void prepareToRejectMessage(boolean isMessageBeyondLastRollback) {
        this.isMessageBeyondLastRollback = isMessageBeyondLastRollback;
        this.eventType = EventType.MESSAGE_REJECT_EVENT;
    }

    @Override
    public void updateState() throws AndesException {
        AndesSubscription subscription = AndesContext.getInstance().
                getAndesSubscriptionManager().getSubscriptionByProtocolChannel(channelId);
        if (subscription != null) {
            DeliverableAndesMetadata rejectedMessage = subscription.onMessageReject(messageId, reQueue);
            rejectedMessage.setIsBeyondLastRollbackedMessage(isMessageBeyondLastRollback);
            //Tracing message activity
            MessageTracer.trace(rejectedMessage, MessageTracer.MESSAGE_REJECTED);
        } else {
            log.warn("Cannot handle reject. Subscription not found for channel "
                    + channelId + "Dropping message id= " + messageId);
        }
    }

    @Override
    public String eventInfo() {
        return eventType.toString();
    }

    @Override
    public boolean isActionableWhenPassive() {
        return false;
    }
}
