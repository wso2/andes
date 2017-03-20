/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import java.util.EnumSet;

/**
 * This enum defines the states a message can have channel wise. A particular message can be
 * delivered to multiple channels (subscriptions). ChannelMessageStatus is used to define state
 * of a message related to a particular channel. This is merged with MessageStatus at some states.
 */
public enum  ChannelMessageStatus {


    /**
     * Message has been dispatched to subscriber
     */
    DISPATCHED,

    /**
     * Message has been failed to send to its routed consumer
     */
    SEND_FAILED,

    /**
     * The consumer has acknowledged receipt of the message
     */
    ACKED,

    /**
     * Consumer has rejected (NACKED) the message ad it has been buffered again for
     * delivery (possibly to another waiting consumer)
     */
    NACKED,

    /**
     * Consumer has rejected (NACKED)  the message repeatedly. Consider message is rejected
     * permanently by client. No need to consider for delivery again.
     */
    CLIENT_REJECTED,

    /**
     * Channel is recovered. Messages will be flushed to some channel again
     */
    RECOVERED,

    /**
     * Consumer is closed
     */
    CLOSED;


    //keep next possible states
    private EnumSet<ChannelMessageStatus> next;

    //keep previous possible states
    private EnumSet<ChannelMessageStatus> previous;

    /**
     * Check if submitted state is an allowed state as per state model
     *
     * @param nextState suggested next state to transit
     * @return if transition is valid
     */
    public boolean isValidNextTransition(ChannelMessageStatus nextState) {
        return next.contains(nextState);
    }

    /**
     * Check if submitted state is an allowed state as per state model
     *
     * @param previousState suggested next state to transit
     * @return if transition is valid
     */
    public boolean isValidPreviousState(ChannelMessageStatus previousState) {
        return previous.contains(previousState);
    }

    static {

        //Channel wise message status begins at DISPATCHED state.
        //If message CLOSED there is no next state for message.

        DISPATCHED.next = EnumSet.of(SEND_FAILED, ACKED, NACKED, RECOVERED, CLOSED);
        DISPATCHED.previous = EnumSet.complementOf(EnumSet.allOf(ChannelMessageStatus.class));

        SEND_FAILED.next = EnumSet.of(DISPATCHED, CLIENT_REJECTED, CLOSED);
        SEND_FAILED.previous = EnumSet.of(DISPATCHED);

        ACKED.next = EnumSet.of(RECOVERED, CLOSED);
        ACKED.previous = EnumSet.of(DISPATCHED);

        NACKED.next = EnumSet.of(DISPATCHED, RECOVERED, CLOSED);
        NACKED.previous = EnumSet.of(DISPATCHED);

        CLIENT_REJECTED.next = EnumSet.complementOf(EnumSet.allOf(ChannelMessageStatus.class));
        CLIENT_REJECTED.previous = EnumSet.of(SEND_FAILED);

        RECOVERED.next = EnumSet.of(DISPATCHED, CLOSED);
        RECOVERED.previous = EnumSet.of(DISPATCHED, ACKED, NACKED);

        //this is because we directly mark close on channel close
        CLOSED.next = EnumSet.of(SEND_FAILED, CLOSED);
        CLOSED.previous = EnumSet.allOf(ChannelMessageStatus.class);
    }

}
