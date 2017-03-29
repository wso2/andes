/*
 * Copyright (c)2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;

import java.util.UUID;

/**
 * InboundChannelFlow event is used when a channel suspend/resume request is received. Once received, andes
 * subscription manager is notified of the flow state to be updated.
 */
public class InboundChannelFlowEvent implements AndesInboundStateEvent {

    private static Log log = LogFactory.getLog(InboundChannelFlowEvent.class);

    /**
     * Supported state events.
     */
    public enum EventType {

        /**
         * channel flow event signals the channel to be suspended/resumed.
         */
        CHANNEL_FLOW_EVENT
    }

    /**
     * Channel event type handled by the event.
     */
    private EventType eventType;

    /**
     * Channel ID to which the flow information is related.
     */
    private UUID channelID;

    /**
     * Callback to send flow ok
     */
    private DisruptorEventCallback callback;

    /**
     * Whether the event suspends or resumes message delivery to the channel. If active is set to false, the channel
     * will be suspended.
     */
    private boolean active;

    /**
     * Creates a disruptor event for channel event.
     *
     * @param channelID     Id of the channel
     * @param active        the new state of the channel. If set to false, the channel is suspended.
     * @param eventCallback Callback to send response
     */
    public InboundChannelFlowEvent(UUID channelID, boolean active, DisruptorEventCallback eventCallback) {
        eventType = EventType.CHANNEL_FLOW_EVENT;
        this.channelID = channelID;
        this.active = active;
        callback = eventCallback;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateState() {
        if (log.isDebugEnabled()) {
            log.debug("Inbound channel flow event received. Flow status: " + active);
        }
        AndesContext.getInstance().getAndesSubscriptionManager().notifySubscriptionFlow(channelID, active);
        callback.execute();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String eventInfo() {
        return eventType.toString();
    }

}
