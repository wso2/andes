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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.stats.PerformanceCounter;

/**
 * Acknowledgement Handler for the Disruptor based inbound event handling.
 * This handler processes acknowledgements received from clients and updates Andes.
 */
public class AckHandler implements BatchEventHandler {

    private static Log log = LogFactory.getLog(AckHandler.class);

    @Override
    public void onEvent(final List<InboundEvent> eventList) throws Exception {
        if (log.isTraceEnabled()) {
            StringBuilder messageIDsString = new StringBuilder();
            for (InboundEvent inboundEvent : eventList) {
                messageIDsString.append(inboundEvent.getAckData().getMessageID()).append(" , ");
            }
            log.trace(eventList.size() + " messages received : " + messageIDsString);
        }
        if(log.isDebugEnabled()){
            log.debug(eventList.size() + " acknowledgements received from disruptor.");
        }

        try {
            ackReceived(eventList);
        } catch (AndesException e) {
            // Log the AndesException since there is no point in passing the exception to Disruptor
            log.error("Error occurred while processing acknowledgements ", e);
        }/* finally {
            for (InboundEvent inboundEvent : eventList) {
                inboundEvent.clear();
            }
        }*/
    }

    /**
     * Updates the state of Andes and deletes relevant messages. (For topics message deletion will happen only when
     * all the clients acknowledges)
     * @param eventList inboundEvent list
     */
    public void ackReceived(final List<InboundEvent> eventList) throws AndesException {
        List<AndesRemovableMetadata> removableMetadata = new ArrayList<AndesRemovableMetadata>();
        Map<String, Slot> slotsOfMessages = new HashMap<String, Slot>(5);
        for (InboundEvent event : eventList) {

            AndesAckData ack = event.getAckData();
            DeliverableAndesMessageMetadata messageRef = ack.getMessageReference();
            if(null == ack) {
                log.info("ack is null");
            }
            if(null == messageRef) {
                log.info("message ref is null");
            }
            messageRef.recordAcknowledge(ack.getChannelID());
            Slot slotOfAckedMessage = ack.getMessageReference().getSlot();
            if(null == slotsOfMessages.get(slotOfAckedMessage.getId())) {
                slotsOfMessages.put(slotOfAckedMessage.getId(), slotOfAckedMessage);
            }
            // For topics message is shared. If all acknowledgements are received only we should remove message
           boolean deleteMessage = ack.getMessageReference().isOKToRemoveMessage();
            if (deleteMessage) {
                if (log.isDebugEnabled()) {
                    log.debug("Ok to delete message id " + ack.getMessageID());
                }
                removableMetadata.add(
                        new AndesRemovableMetadata(ack.getMessageID(), ack.getDestination(),
                                                   ack.getMsgStorageDestination()));
                slotOfAckedMessage.decrementPendingMessageCountBySlot();
            }

            //record ack received
            PerformanceCounter.recordMessageRemovedAfterAck();
        }

        MessagingEngine.getInstance().deleteMessages(removableMetadata, false);

        //try to release slot after actually deleting metadata
        for(Map.Entry<String, Slot> slotEntry : slotsOfMessages.entrySet()) {
            slotEntry.getValue().checkForSlotCompletionAndResend();
        }

        if (log.isTraceEnabled()) {
            StringBuilder messageIDsString = new StringBuilder();
            for (AndesRemovableMetadata metadata : removableMetadata) {
                messageIDsString.append(metadata.getMessageID()).append(" , ");
            }
            log.trace(eventList.size() + " message ok to remove : " + messageIDsString);
        }
    }
}
