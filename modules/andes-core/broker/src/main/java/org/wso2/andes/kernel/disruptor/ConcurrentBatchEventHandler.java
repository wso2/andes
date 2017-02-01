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

package org.wso2.andes.kernel.disruptor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer;
import org.wso2.andes.kernel.disruptor.inbound.MessageWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a turn based concurrent event batching handler. This should be used with a Default Batch processor
 * The processor will batch event until end of current available sequence numbers is visited send the events
 * to this concurrent event batch handler to do custom event batching
 * 
 * NOTE: Writing a custom batch processor is avoided since it related to implementing disruptor internals related
 * logic which might lead to difficulty in upgrading disruptor versions and getting bug fixes on batch processors 
 */
public class ConcurrentBatchEventHandler extends InboundEventHandler {

    private static Log log = LogFactory.getLog(ConcurrentBatchEventHandler.class);

    /**
     * Batch event handler for handling batched events
     */
    private final MessageWriter messageWriter;

    /**
     * Turn is the value of, sequence % groupCount this batch processor process events. Turn must be
     * less than groupCount
     */
    private final long turn;

    /**
     * total number of concurrent batch processors for the event type
     */
    private final int groupCount;

    /**
     * Maximum size of the batch
     */
    private int batchSize;

    /**
     * Type of event to do batching
     */
    private final InboundEventContainer.Type eventType;

    /**
     * Events are batched using this until the event handler is called
     */
    private final List<AndesMessage> messageList;

    /**
     * Temporary storage for retain messages
     */
    private final Map<String, AndesMessage> retainMap;

    /**
     * Creates an event handler that can be used with a batch processor to do custom batching of inbound
     * event using inbound event type
     *
     * @param turn         is the value of, sequence % groupCount this batch processor process events. Turn must be
     *                     less than groupCount
     * @param groupCount   total number of concurrent batch processors for the event type
     * @param batchSize    maximum size of the batch
     * @param eventType    type of event to batch
     * @param messageWriter event handler that does the actual per event, event handling
     */
    public ConcurrentBatchEventHandler(long turn, int groupCount, int batchSize,
                                       InboundEventContainer.Type eventType, MessageWriter messageWriter) {
        
        if (turn >= groupCount) {
            throw new IllegalArgumentException("Turn should be less than groupCount");
        }

        this.turn = turn;
        this.groupCount = groupCount;
        this.batchSize = batchSize;
        this.eventType = eventType;
        this.messageWriter = messageWriter;
        messageList = new ArrayList<>(this.batchSize);
        retainMap = new HashMap<>();

    }

    /**
     * Batches event according to the event type
     * 
     * {@inheritDoc} 
     */
    @Override
    public void onEvent(InboundEventContainer event, long sequence, boolean endOfBatch) throws Exception {
        long currentTurn;

        // Batch only relevant event type. Skip the rest
        if (eventType == event.getEventType()) {

            currentTurn = sequence % groupCount;
            if (turn == currentTurn) {
                messageList.addAll(event.getMessageList());
                if(null != event.retainMessage) {
                    retainMap.put(event.retainMessage.getMetadata().getDestination(), event.retainMessage);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("[ " + sequence + " ] Current turn " + currentTurn + ", turn " + turn
                        + ", groupCount " + groupCount + ", EventType " + eventType);
            }
        }

        // Batch and invoke event handler. Irrespective of event type following should execute.
        // End of batch may come in an irrelevant event type slot.
        if (((messageList.size() >= batchSize) || endOfBatch)) {
            try {
                messageWriter.writeMessages(messageList, retainMap);
                messageList.clear();
                retainMap.clear();
                if (log.isDebugEnabled()) {
                    log.debug("Event handler called with " + messageList.size() + " events. EventType "
                            + eventType);
                }
            } finally {
                // Clear the list irrespective of the output of messageWriter#writeMessages
                // On an error situation of messageWriter#writeMessages we need to clear the events from this batching list.
                // Respective event handler should take care of the actual events passed. If not same events 
                // will be passed multiple times to the handler in an erroneous situation
                messageList.clear();
            }
        }
    }
}
