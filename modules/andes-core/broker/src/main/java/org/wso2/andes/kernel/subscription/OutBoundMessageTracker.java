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

package org.wso2.andes.kernel.subscription;

import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.ProtocolMessage;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents an protocol wrapped subscriber with outbound
 * operations. Common operations related to outbound message operations
 * are implemented here.
 */
public class OutBoundMessageTracker {

    /**
     * Map to track messages being sent <message id, MsgData reference>. This map bares message
     * reference at kernel side
     */
    private final ConcurrentHashMap<Long, DeliverableAndesMetadata> messageSendingTracker;

    /**
     * Max number of un-acknowledged messages to keep
     */
    private int maxNumberOfUnAcknowledgedMessages;

    private static Log log = LogFactory.getLog(OutBoundMessageTracker.class);


    /**
     * Create a message tracker
     */
    public OutBoundMessageTracker(int maxNumberOfMessagesToKeep) {
        this.messageSendingTracker = new ConcurrentHashMap<>();
        this.maxNumberOfUnAcknowledgedMessages = maxNumberOfMessagesToKeep;
    }

    /**
     * Remove message from sending tracker. This is called when a send
     * error happens at the Outbound subscriber protocol level. ACK or
     * REJECT can never be received for that message
     *
     * @param messageID Id of the message
     * @return DeliverableAndesMetadata removed message
     */
    public synchronized DeliverableAndesMetadata removeSentMessageFromTracker(long messageID) {
        return messageSendingTracker.remove(messageID);
    }

    /**
     * Get message metadata reference by message ID. Returns null if the reference
     * is not found
     *
     * @param messageID ID of the message
     * @return message metadata reference
     */
    public DeliverableAndesMetadata getMessageByMessageID(long messageID) {
        DeliverableAndesMetadata metadata = messageSendingTracker.get(messageID);
        if (null == metadata) {
            log.error("Message reference has been already cleared for message id " + messageID
                    + ". Acknowledge or Nak is already received");
        }
        return metadata;
    }


    /**
     * Check if this associated subscription has ability to accept messages
     * If pending ack count is high it does not have ability to accept new messages
     *
     * @return true if able to accept messages
     */
    public boolean hasRoomToAcceptMessages() {
        int notAcknowledgedMsgCount = messageSendingTracker.size();
        if (notAcknowledgedMsgCount < maxNumberOfUnAcknowledgedMessages) {
            return true;
        } else {

            if (log.isDebugEnabled()) {
                log.debug("Not selected. Too much pending acks, subscription = " + this + " pending count =" +
                        (notAcknowledgedMsgCount));
            }

            return false;
        }
    }

    /**
     * Get all sent but not acknowledged messages of tracker
     *
     * @return List of DeliverableAndesMetadata messages
     */
    public List<DeliverableAndesMetadata> getUnackedMessages() {
        return new ArrayList<>(messageSendingTracker.values());
    }

    /**
     * Clear tracked sent but un-acknowledged messages. Return the messages in the same view.
     * While this operation is performed, no new message will be added to the list.
     *
     * @return list of messages tracked when cleaned up
     */
    public synchronized List<DeliverableAndesMetadata> clearAndReturnUnackedMessages() {
        List<DeliverableAndesMetadata> messages = getUnackedMessages();
        messageSendingTracker.clear();
        return messages;
    }

    /**
     * Add message to sending tracker which keeps messages delivered to channel of associated subscriber
     *
     * @param messageData message to add
     */
    public synchronized void addMessageToSendingTracker(ProtocolMessage messageData) {
        messageSendingTracker.put(messageData.getMessageID(), messageData.getMessage());
    }

}
