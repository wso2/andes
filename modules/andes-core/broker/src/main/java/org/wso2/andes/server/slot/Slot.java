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

package org.wso2.andes.server.slot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

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
     * Keep actual destination of messages in slot
     */
    private String destinationOfMessagesInSlot;

    /**
     * Track message count buffered to be delivered by slot
     * but not acked by all
     */
    private AtomicInteger pendingMessageCountBySlot = new AtomicInteger(0);


    public Slot() {
        isSlotActive = true;
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

    public void incrementPendingMessageCountBySlot() {
        pendingMessageCountBySlot.incrementAndGet();
    }

    public void decrementPendingMessageCountBySlot() throws AndesException {
        pendingMessageCountBySlot.decrementAndGet();
    }

    public void checkForSlotCompletionAndResend() throws AndesException {
        if(pendingMessageCountBySlot.get() == 0) {
            resendMessagesIfExist();
        }
    }


    private void resendMessagesIfExist() throws AndesException {
        SlotDeliveryWorker slotWorker = SlotDeliveryWorkerManager.getInstance().getSlotWorker(getStorageQueueName());
        if (log.isDebugEnabled()) {
            log.debug("Slot has no pending messages. Now re-checking slot for messages");
        }
        slotWorker.checkForSlotCompletionAndResend(this);
    }

    public String getDestinationOfMessagesInSlot() {
        return destinationOfMessagesInSlot;
    }

    public void setDestinationOfMessagesInSlot(String destinationOfMessagesInSlot) {
        this.destinationOfMessagesInSlot = destinationOfMessagesInSlot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Slot slot = (Slot) o;

        if (endMessageId != slot.endMessageId) return false;
        if (startMessageId != slot.startMessageId) return false;
        if (!storageQueueName.equals(slot.storageQueueName)) return false;

        return true;
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
        return "Slot{" +
                "startMessageId=" + startMessageId +
                ", storageQueueName='" + storageQueueName + '\'' +
                ", endMessageId=" + endMessageId +
                '}';
    }

    /**
     * Return uniqueue id for the slot
     *
     * @return slot message id
     */
    public String getId() {
        return storageQueueName + "|" + startMessageId + "-" + endMessageId;
    }

    @Override
    public int compareTo(Slot other) {
        if(this.getStartMessageId() == other.getStartMessageId()) {
            return 0;
        } else {
            return this.getStartMessageId() > other.getStartMessageId() ? 1 : -1;
        }
    }
}
