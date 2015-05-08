/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.distruptor.inbound;

import com.lmax.disruptor.EventFactory;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * All inbound events are published to disruptor ring buffer as an InboundEventContainer object
 * This class specifies the event type and any data that is relevant to that event.
 */
public class InboundEventContainer {

    private AndesChannel channel;

    /**
     * Maintains the last generated message ID as the safe zone of this node.
     */
    private long safeZoneLimit;

    /**
     * Specific event type of relevant to this InboundEventContainer
     */
    private Type eventType;

    /**
     * Andes state change related event
     */
    private AndesInboundStateEvent stateEvent;

    /**
     * For topic we may need to duplicate content. Message Pre processor will do that
     */
    public List<AndesMessage> messageList;

    /**
     * Acknowledgments received to disruptor
     */
    public AndesAckData ackData;

    /**
     * Publisher acknowledgements are handled by this ack handler
     */
    public PubAckHandler pubAckHandler;

    public AndesChannel getChannel() {
        return channel;
    }

    public void setChannel(AndesChannel channel) {
        this.channel = channel;
    }

    public void setSafeZoneLimit(long safeZoneLimit) {
        this.safeZoneLimit = safeZoneLimit;
    }

    public long getSafeZoneLimit() {
        return safeZoneLimit;
    }
    /**
     * For storing retained messages for topic
     */
    public AndesMessage retainMessage;

    /**
     * Inbound event type is specified by this enum
     */
    public enum Type {

        /** Message receive event */
        MESSAGE_EVENT,

        /** Message acknowledgement receive event */
        ACKNOWLEDGEMENT_EVENT,

        /** Andes state change related event type */
        STATE_CHANGE_EVENT,

        /** Ignore the event and skip processing */
        IGNORE_EVENT,

        /** slot deletion safe zone update event */
        SAFE_ZONE_DECLARE_EVENT

    }

    public InboundEventContainer() {
        messageList = new ArrayList<AndesMessage>();
        eventType = Type.IGNORE_EVENT;
        safeZoneLimit = Long.MIN_VALUE;

    }

    public final Type getEventType() {
        return eventType;
    }

    public final void setEventType(Type eventType) {
        this.eventType = eventType;
    }

    public void updateState() throws AndesException {
        stateEvent.updateState();
    }

    public void setStateEvent(AndesInboundStateEvent stateEvent) {
        this.stateEvent = stateEvent;
    }

    /**
     * Reset internal references null and sets event type to IGNORE_EVENT
     */
    public void clear() {
        messageList.clear();
        retainMessage = null;
        ackData = null;
        stateEvent = null;
        eventType = Type.IGNORE_EVENT;
        pubAckHandler = null;
    }
    /**
     * Disruptor uses this factory to populate the ring with inboundEvent objects
     */
    public static class InboundEventFactory implements EventFactory<InboundEventContainer> {
        @Override
        public InboundEventContainer newInstance() {
            return new InboundEventContainer() {
            };
        }
    }

    /**
     * Human readable information of the event
     * @return brief description of the event
     */
    public String eventInfo() {
        if(eventType == Type.STATE_CHANGE_EVENT) {
            return stateEvent.eventInfo();
        } else {
            return eventType.toString();
        }
    }

    public static EventFactory<InboundEventContainer> getFactory() {
        return new InboundEventFactory();
    }
}
