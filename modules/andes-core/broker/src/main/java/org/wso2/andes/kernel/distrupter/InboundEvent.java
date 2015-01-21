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

package org.wso2.andes.kernel.distrupter;

import com.google.common.util.concurrent.SettableFuture;
import com.lmax.disruptor.EventFactory;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * All inbound events are published to disruptor ring buffer as an inboundEvent object
 * This class specifies the event type and any data that is relevant to that event.
 */
public class InboundEvent {

    private AndesChannel channel;

    /**
     * This future is used to catch exceptions occurring at the disruptor. When exception is
     * occurred at the disruptor that exception will be set to the future.
     */
    private SettableFuture<String> future;

    public AndesChannel getChannel() {
        return channel;
    }

    public void setChannel(AndesChannel channel) {
        this.channel = channel;
    }

    public SettableFuture<String> getFuture() {
        return future;
    }

    public void setFuture(SettableFuture<String> future) {
        this.future = future;
    }

    /**
     * Inbound event type is specified by this enum
     */
    public enum Type {

        /** Message receive event */
        MESSAGE_EVENT,

        /** Message acknowledgement receive event */
        ACKNOWLEDGEMENT_EVENT,

        /** Specific client channel close event */
        CHANNEL_CLOSE_EVENT,

        /** New client connected and client channel is opened event */
        CHANNEL_OPEN_EVENT,

        /** Stop message delivery in Andes core */
        STOP_MESSAGE_DELIVERY_EVENT,

        /** Start message delivery in Andes core event */
        START_MESSAGE_DELIVERY_EVENT,

        /** Shutdown andes broker messaging engine event*/
        SHUTDOWN_MESSAGING_ENGINE_EVENT,

        /** New local subscription created event */
        OPEN_SUBSCRIPTION_EVENT,

        /** Close a local subscription event */
        CLOSE_SUBSCRIPTION_EVENT,

        /** Start expired message deleting task, notification event */
        START_EXPIRATION_WORKER_EVENT,

        /** Stop expired message deleting task, notification event */
        STOP_EXPIRATION_WORKER_EVENT,

        /** Ignore the event and skip processing */
        IGNORE_EVENT

        }

    /**
     * Specific event type of relevant to this InboundEvent
     */
    private Type eventType;

    /**
     * data holder for the inbound event related information.
     */
    private Object data;

    /**
     * For topic we may need to duplicate content. Message Pre processor will do that
     */
    public List<AndesMessage> messageList;

    /**
     * Acknowledgments received to disruptor
     */
    public AndesAckData ackData;

    public InboundEvent() {
        messageList = new ArrayList<AndesMessage>();
        eventType = Type.IGNORE_EVENT;
    }

    public final Type getEventType() {
        return eventType;
    }

    public final void setEventType(Type eventType) {
        this.eventType = eventType;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Object getData() {
        return data;
    }


    public void clear() {
        messageList.clear();
        ackData = null;
        data = null;
        eventType = Type.IGNORE_EVENT;
    }
    /**
     * Disruptor uses this factory to populate the ring with inboundEvent objects
     */
    public static class InboundEventFactory implements EventFactory<InboundEvent> {
        @Override
        public InboundEvent newInstance() {
            return new InboundEvent() {
            };
        }
    }

    public static EventFactory<InboundEvent> getFactory() {
        return new InboundEventFactory();
    }
}
