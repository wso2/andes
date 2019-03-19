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

import java.util.EnumSet;
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
     * Message has been added to the final async delivery queue (deliverAsynchronously method has been called for
     * the message.)
     */
    SCHEDULED_TO_SEND,

    /**
     * In a topic scenario, all subscribed consumers have acknowledged receipt of message
     */
    ACKED_BY_ALL,


    /**
     * All messages of the slot containing this message have been handled successfully, causing it to be removed
     */
    SLOT_REMOVED,

    /**
     * Message has expired (JMS Expiration duration sent with the message has passed)
     */
    EXPIRED,

    /**
     * Message is discarded as no valid subscriber to send
     */
    NO_MATCHING_CONSUMER,

    /**
     * Message is moved to the DLC queue
     */
    DLC_MESSAGE,

    /**
     * Message has been cleared from delivery due to a queue purge event.
     */
    PURGED,

    /**
     * Message is prepared to delete.
     */
    PREPARED_TO_DELETE,

    /**
     * Message is deleted from the store
     */
    DELETED,

    /**
     * Slot of the message is returned back to the coordinator, causing message to remove from memory
     */
    SLOT_RETURNED;

    //keep next possible states
    private EnumSet<MessageStatus> next;

    //keep previous possible states
    private EnumSet<MessageStatus> previous;

    /**
     * Check if submitted state is an allowed state as per state model
     *
     * @param nextState suggested next state to transit
     * @return if transition is valid
     */
    public boolean isValidNextTransition(MessageStatus nextState) {
        return next.contains(nextState);
    }

    /**
     * Check if submitted state is an allowed state as per state model
     *
     * @param previousState suggested next state to transit
     * @return if transition is valid
     */
    public boolean isValidPreviousState(MessageStatus previousState) {
        return previous.contains(previousState);
    }

    static {

        //SLOT_RETURNED, PURGE, EXPIRE, SLOT REMOVE can happen at any moment
        //next state of SLOT_RETURNED, PURGE, EXPIRE, SLOT REMOVE can be any state

        READ.next = EnumSet.of(BUFFERED, SLOT_RETURNED);
        READ.previous = EnumSet.complementOf(EnumSet.allOf(MessageStatus.class));

        BUFFERED.next = EnumSet.of(SCHEDULED_TO_SEND, NO_MATCHING_CONSUMER, SLOT_RETURNED);
        BUFFERED.previous = EnumSet.of(READ);

        NO_MATCHING_CONSUMER.next = EnumSet.of(DLC_MESSAGE);
        NO_MATCHING_CONSUMER.previous = EnumSet.of(BUFFERED);

        SCHEDULED_TO_SEND.next = EnumSet.of(EXPIRED, ACKED_BY_ALL, BUFFERED, DLC_MESSAGE, SLOT_RETURNED);
        SCHEDULED_TO_SEND.previous = EnumSet.of(BUFFERED);

        ACKED_BY_ALL.next = EnumSet.of(PREPARED_TO_DELETE, SLOT_RETURNED);
        ACKED_BY_ALL.previous = EnumSet.of(SCHEDULED_TO_SEND);

        EXPIRED.next = EnumSet.of(PREPARED_TO_DELETE, SLOT_RETURNED, DLC_MESSAGE);
        EXPIRED.previous = EnumSet.allOf(MessageStatus.class);

        DLC_MESSAGE.next = EnumSet.of(EXPIRED, BUFFERED, SLOT_REMOVED, SLOT_RETURNED);
        DLC_MESSAGE.previous = EnumSet.of(SCHEDULED_TO_SEND);

        PURGED.next = EnumSet.of(PREPARED_TO_DELETE, SLOT_RETURNED);
        PURGED.previous = EnumSet.allOf(MessageStatus.class);

        PREPARED_TO_DELETE.next = EnumSet.of(DELETED, SLOT_REMOVED);
        PREPARED_TO_DELETE.previous = EnumSet.of(EXPIRED, DLC_MESSAGE, PURGED);

        DELETED.next = EnumSet.of(SLOT_REMOVED, SLOT_RETURNED);
        DELETED.previous = EnumSet.of(PREPARED_TO_DELETE, SLOT_REMOVED);

        //TODO: ideally this should be EnumSet.complementOf(EnumSet.allOf(MessageStatus.class)) but we need to solve
        //TODO: concurrency problem between slot removal task and message deleting task
        SLOT_REMOVED.next = EnumSet.of(DELETED, SLOT_RETURNED);
        SLOT_REMOVED.previous = EnumSet.of(PREPARED_TO_DELETE, DELETED);

        /*
         * next status of slot return status can be any state due to subscription could close at any given moment.
         */
        SLOT_RETURNED.next = EnumSet.allOf(MessageStatus.class);
        SLOT_RETURNED.previous = EnumSet.allOf(MessageStatus.class);

    }

    /**
     * Is OK to remove tracking message
     *
     * @return eligibility to remove
     */
    public static boolean isOKToRemove(List<MessageStatus> messageStatus) {
        return (messageStatus.contains(MessageStatus.EXPIRED)
                || messageStatus.contains(MessageStatus.DLC_MESSAGE)
                || messageStatus.contains(MessageStatus.PURGED)
                || messageStatus.contains(MessageStatus.DELETED))
                || messageStatus.get(messageStatus.size() - 1).equals(MessageStatus.SLOT_REMOVED)
                || messageStatus.get(messageStatus.size() - 1).equals(MessageStatus.SLOT_RETURNED);
    }
}
