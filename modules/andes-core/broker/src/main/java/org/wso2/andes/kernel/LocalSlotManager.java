/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

package org.wso2.andes.kernel;

import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
class LocalSlotManager {

    /**
     * TODO: this should be read from configuration
     */
    private static final long SLOT_SIZE = 10;
    private final MessagingEngine messagingEngine;

    ConcurrentHashMap<String, SlotInfo> cachedSlotIds = new ConcurrentHashMap<>();

    LocalSlotManager(MessagingEngine messagingEngine) {
        this.messagingEngine = messagingEngine;
    }

    long getSlotId(String targetQueueName, int numberOfMessages) {
        SlotInfo slot = cachedSlotIds.get(targetQueueName);

        if (null == slot) {
            long slotId = messagingEngine.generateUniqueId();
            slot = new SlotInfo(slotId);
            cachedSlotIds.put(targetQueueName, slot);
        }

        long currentTotal = slot.getMessageCount() + numberOfMessages;

        if (currentTotal > SLOT_SIZE) {
            cachedSlotIds.remove(targetQueueName);
        } else {
            slot.setMessageCount(currentTotal);
        }

        return slot.getSlotId();
    }

    private static class SlotInfo {
        private long slotId;
        private long messageCount;

        private SlotInfo(long slotId) {
            this.slotId = slotId;
            this.messageCount = 0;
        }

        private long getSlotId() {
            return slotId;
        }

        private void setSlotId(long slotId) {
            this.slotId = slotId;
        }

        private long getMessageCount() {
            return messageCount;
        }

        private void setMessageCount(long messageCount) {
            this.messageCount = messageCount;
        }
    }
}
