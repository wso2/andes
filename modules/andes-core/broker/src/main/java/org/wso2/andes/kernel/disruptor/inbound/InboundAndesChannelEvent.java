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
 * Andes channel related events are published to Disruptor as InboundAndesChannelEvent
 */
public class InboundAndesChannelEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundAndesChannelEvent.class);

    /**
     * Supported state events
     */
    public enum EventType {
        /**
         * Specific client channel close event
         */
        CHANNEL_RECOVER_EVENT,
    }

    /**
     * Channel event type handle by the event object 
     */
    private EventType eventType;

    /**
     * Channel ID 
     */
    private UUID channelID;

    /**
     * Callback to send recover-ok
     */
    private DisruptorEventCallback recoverOKCallback;

    /**
     * Create a disruptor event for channel event
     *
     * @param channelID         Id of the channel
     * @param recoverOKCallback Callback to send recover-ok
     */
    public InboundAndesChannelEvent(UUID channelID, DisruptorEventCallback recoverOKCallback) {
        this.recoverOKCallback = recoverOKCallback;
        this.channelID = channelID;
    }
    
    @Override
    public void updateState() throws AndesException {
        switch (eventType) {
            case CHANNEL_RECOVER_EVENT:
                AndesSubscription subscription  = AndesContext.getInstance().
                        getAndesSubscriptionManager().getSubscriptionByProtocolChannel(channelID);

                // Subscription can be null if we send a recover call without subscribing. In this case we do not have
                // to recover any messages. Therefore print a log and return.
                if (null == subscription) {
                    log.warn("Cannot handle recover. No subscriptions found for channel " + channelID);
                    return;
                }
                subscription.recoverMessages(recoverOKCallback);
                break;
            default:
                log.error("Event type not set properly " + eventType);
                break;
        }
    }

    @Override
    public String eventInfo() {
        return eventType.toString();
    }

    /**
     * Update event to a channel open event 
     */
    public void prepareForChannelRecover() {
        eventType = EventType.CHANNEL_RECOVER_EVENT;
    }
}
