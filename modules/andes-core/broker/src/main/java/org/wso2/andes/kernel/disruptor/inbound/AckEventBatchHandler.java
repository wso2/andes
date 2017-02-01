/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.disruptor.inbound;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.disruptor.InboundEventHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Disruptor batch handler to process acknowledgment events
 */
public class AckEventBatchHandler extends InboundEventHandler {

    /**
     * Logger
     */
    private static Log log = LogFactory.getLog(AckHandler.class);

    /**
     * Batched acknowledgments event list.
     */
    private final List<AndesAckData> ackDataList;

    /**
     * Number assigned to this handler. Value is less than the groupCount. If the turn value is equal to
     * sequence % groupCount then the events related to that Disruptor sequence will be processed
     */
    private final int turn;

    /**
     * Total number of {@link AckEventBatchHandler}s
     */
    private final int groupCount;

    /**
     * Maximum number of acknowledgments to be batched before processing
     */
    private final int batchSize;

    /**
     * Specific acknowledgment processing entity
     */
    private final AckHandler ackHandler;

    /**
     * Create a {@link AckEventBatchHandler} handler instance. Two handlers should not have the same turn value.
     * @param turn a number less than the group count and a unique number within the {@link AckEventBatchHandler} group
     * @param groupCount Total number of {@link AckEventBatchHandler}s in the group
     * @param batchSize Maximum number of acknowledgments to be batched before processing
     * @param ackHandler Specific acknowledgment processing entity
     */
    public AckEventBatchHandler(int turn, int groupCount, int batchSize, AckHandler ackHandler) {
        ackDataList = new ArrayList<>(batchSize);
        this.turn = turn;
        this.groupCount = groupCount;
        this.batchSize = batchSize;
        this.ackHandler = ackHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onEvent(InboundEventContainer event, long sequence, boolean endOfBatch) throws Exception {

        if (InboundEventContainer.Type.ACKNOWLEDGEMENT_EVENT == event.getEventType()) {
            long currentTurn = sequence % groupCount;
            if (turn == currentTurn) {
                ackDataList.add(event.ackData);
            }
            if (log.isDebugEnabled()) {
                log.debug("[ " + sequence + " ] Current turn " + currentTurn + ", turn " + turn
                                  + ", groupCount " + groupCount + ", EventType "
                                  + InboundEventContainer.Type.ACKNOWLEDGEMENT_EVENT);
            }
        }

        if (((ackDataList.size() >= batchSize) || endOfBatch) && !ackDataList.isEmpty()) {
            ackHandler.processAcknowledgements(ackDataList);
            ackDataList.clear();
        }
    }
}
