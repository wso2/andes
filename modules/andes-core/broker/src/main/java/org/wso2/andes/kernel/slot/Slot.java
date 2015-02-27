/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.slot;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class stores all the data related to a slot
 */
public class Slot implements Serializable, Comparable<Slot> {

    private static Log log = LogFactory.getLog(Slot.class);

    /**
     * Number of messages in the slot
     */
    private long messageCount;

    /**
     * Start message ID of the slot
     */
    private long startMessageId;

    /**
     * End message ID of the slot
     */
    private long endMessageId;

    /**
     * QueueName which the slot belongs to. This is set when the slot is assigned to a subscriber
     */
    private String storageQueueName;

    /**
     * Keep if slot is active, if not it is eligible to be removed
     */
    private boolean isSlotActive;

    /**
     * Indicates whether the slot is a fresh one or an overlapped one
     */
    private boolean isAnOverlappingSlot;

    /**
     * Keep state of the slot
     */
    private List<SlotState> slotStates;

    /**
     * Keep actual destination of messages in slot
     */
    private String destinationOfMessagesInSlot;


    public Slot() {
        isSlotActive = true;
        isAnOverlappingSlot = false;
        this.slotStates = new ArrayList<SlotState>();
        addState(SlotState.CREATED);
    }

    public Slot(long start, long end, String destinationOfMessagesInSlot) {
        this();
        this.startMessageId = start;
        this.endMessageId = end;
        this.destinationOfMessagesInSlot = destinationOfMessagesInSlot;
    }

    public void setStorageQueueName(String storageQueueName) {
        this.storageQueueName = storageQueueName;
    }

    public String getStorageQueueName() {
        return storageQueueName;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }

    public long getEndMessageId() {
        return endMessageId;
    }

    public void setEndMessageId(long endMessageId) {
        this.endMessageId = endMessageId;
    }

    public long getStartMessageId() {
        return startMessageId;
    }

    public void setStartMessageId(long startMessageId) {
        this.startMessageId = startMessageId;
    }

    public void setSlotInActive() {
        isSlotActive = false;
    }

    public boolean isSlotActive() {
        return isSlotActive;
    }

    public boolean isAnOverlappingSlot() {
        return isAnOverlappingSlot;
    }

    public void setAnOverlappingSlot(boolean isAnOverlappingSlot) {
        this.isAnOverlappingSlot = isAnOverlappingSlot;
        if(isAnOverlappingSlot) {
            addState(SlotState.OVERLAPPED);
        }
    }

    public String getDestinationOfMessagesInSlot() {
        return destinationOfMessagesInSlot;
    }

    public void setDestinationOfMessagesInSlot(String destinationOfMessagesInSlot) {
        this.destinationOfMessagesInSlot = destinationOfMessagesInSlot;
    }

    /**
     * Check if state going to be added is valid considering it as the next
     * transition compared to current latest state.
     * @param state state to be transferred
     */
    public boolean addState(SlotState state) {

        boolean isValidTransition = false;

        if(slotStates.isEmpty()) {
            if(SlotState.CREATED.equals(state)) {
                isValidTransition = true;
                slotStates.add(state);
            } else {
                log.warn("Invalid State transition suggested: " + state);
            }
        } else {
            isValidTransition = slotStates.get(slotStates.size() - 1).isValidNextTransition(state);
            if(isValidTransition) {
                slotStates.add(state);
            } else {
                log.warn("Invalid State transition from " + slotStates.get
                        (slotStates.size() - 1) + " suggested: " + state + " Slot ID: " + this
                        .getId());

            }
        }

        return isValidTransition;
    }

    /**
     * Convert Slot state list to a string
     * @return Encoded string
     */
    public String encodeSlotStates() {
        String encodedString;
        StringBuilder builder = new StringBuilder();
        for (SlotState slotState : slotStates) {
            builder.append(slotState.getCode()).append("%");
        }
        encodedString = builder.toString();
        return encodedString;
    }

    /**
     * Decode slot states from a string
     * @param stateInfo encoded string
     */
    public void decodeAndSetSlotStates(String stateInfo) {
        String[] states = StringUtils.split(stateInfo, "%");
        slotStates.clear();
        for (String state : states) {
            int code = Integer.parseInt(state);
            slotStates.add(SlotState.parseSlotState(code));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Slot slot = (Slot) o;

        return endMessageId == slot.endMessageId && startMessageId == slot.startMessageId &&
                storageQueueName.equals(slot.storageQueueName);

    }

    @Override
    public int hashCode() {
        int result = (int) (startMessageId ^ (startMessageId >>> 32));
        result = 31 * result + (int) (endMessageId ^ (endMessageId >>> 32));
        result = 31 * result + storageQueueName.hashCode();
        return result;
    }

    @Override
    public String toString() {

        Gson gson = new GsonBuilder().create();
        return gson.toJson(this);
    }

    /**
     * Return unique id for the slot
     *
     * @return slot message id
     */
    public String getId() {
        return storageQueueName + "|" + startMessageId + "-" + endMessageId;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(Slot other) {
        if ((this.getStartMessageId() == other.getStartMessageId()) && (this.getEndMessageId() == other
                .getEndMessageId()) && this.getStorageQueueName().equals(other.getStorageQueueName())) {
            return 0;
        } else {
            return this.getStartMessageId() > other.getStartMessageId() ? 1 : -1;
        }
    }
}
