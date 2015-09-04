/*
 *
 *   Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class will track message delivery by broker
 * on the fly. Message delivery times, message status,
 * is tracked here
 */
public class OnflightMessageTracker {

    private static Log log = LogFactory.getLog(OnflightMessageTracker.class);

    private static OnflightMessageTracker instance;

    static {
        try {
            instance = new OnflightMessageTracker();
        } catch (AndesException e) {
            log.error("Error occurred when reading configurations : ", e);
        }
    }

    public static OnflightMessageTracker getInstance() {
        return instance;
    }

    /**
     * In memory map keeping sent message statistics by message id
     */
    private final ConcurrentHashMap<Long, DeliverableAndesMetadata> msgId2MsgData;

    /**
     * Class to keep tracking data of a message
     */

    private OnflightMessageTracker() throws AndesException {

        // We don't know the size of the map at startup. hence using an arbitrary value of 16, Need to test
        // Load factor set to default value 0.75
        // Concurrency level set to 6. Currently SlotDeliveryWorker, AckHandler AckSubscription, DeliveryEventHandler,
        // MessageFlusher access this. To be on the safe side set to 6.
        msgId2MsgData = new ConcurrentHashMap<>(16, 0.75f, 6);

    }

    /**
     * Track that this message is buffered. Return true if eligible to buffer
     *
     * @param andesMessageMetadata metadata to buffer
     */
    public void addMessageToTracker(DeliverableAndesMetadata andesMessageMetadata) {
        long messageID = andesMessageMetadata.getMessageID();
        if (log.isDebugEnabled()) {
            log.debug("Adding message to tracker, id = " + messageID + " slot = ");
        }

        msgId2MsgData.put(messageID, andesMessageMetadata);
    }

    /**
     * Remove message from tracker.
     *
     * @param messageID
     *         Message ID of the message
     * @return the tracking data associated with the given message I, or null if there was no mapping for key
     */
    public DeliverableAndesMetadata removeMessageFromTracker(Long messageID) {
        return msgId2MsgData.remove(messageID);
    }

    /**
     * Get message tracking object for a message. This contains
     * all delivery information and message status of the message
     *
     * @param messageID id of the message
     * @return tracking object for message
     */
    public DeliverableAndesMetadata getTrackingData(long messageID) {
        return msgId2MsgData.get(messageID);
    }

    /**
     * Return all message tracking data
     *
     * @return Collection of tracking data
     */
    public Collection<DeliverableAndesMetadata> getAllTrackingData() {
        return msgId2MsgData.values();
    }
}
