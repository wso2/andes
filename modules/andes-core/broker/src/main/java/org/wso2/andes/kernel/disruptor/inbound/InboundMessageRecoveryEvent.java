/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.inbound;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
import org.wso2.andes.kernel.subscription.AndesSubscription;

import java.util.UUID;

/**
 * Events for message recovery that are received by the channel are published as InboundMessageRecoveryEvent.
 */
public class InboundMessageRecoveryEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundMessageRecoveryEvent.class);

    /**
     * Supported state events.
     */
    public enum EventType {
        /**
         * Specific client channel close event.
         */
        CHANNEL_RECOVER_EVENT,
    }

    /**
     * Event type of the recovery event.
     */
    private EventType eventType;

    /**
     * Channel ID from which the recovery event was received.
     */
    private UUID channelID;

    /**
     * Callback to send recover-ok.
     */
    private DisruptorEventCallback recoverOKCallback;

    /**
     * Creates a disruptor event for the recovery event.
     *
     * @param channelID         id of the channel
     * @param recoverOKCallback callback to send recover-ok
     */
    public InboundMessageRecoveryEvent(UUID channelID, DisruptorEventCallback recoverOKCallback) {
        this.recoverOKCallback = recoverOKCallback;
        this.channelID = channelID;
        eventType = EventType.CHANNEL_RECOVER_EVENT;
    }

    @Override
    public void updateState() throws AndesException {

        AndesSubscription subscription = AndesContext.getInstance().
                getAndesSubscriptionManager().getSubscriptionByProtocolChannel(channelID);

        // Subscription can be null if we send a recover call without subscribing. In this case we do not have
        // to recover any messages.
        if (null == subscription) {
            log.warn("Cannot handle recover. No subscriptions found for channel " + channelID);
            return;
        }

        subscription.recoverMessages();
        recoverOKCallback.execute();
    }

    @Override
    public String eventInfo() {
        return eventType.toString();
    }

}
