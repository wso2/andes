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
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.disruptor.InboundEventHandler;

/**
 * Distributed transaction messages and acknowledgment handler class. Invokes database calls related to the
 * distributed transaction messages and acknowledge events
 */
public class DtxDbWriter extends InboundEventHandler {

    /**
     * logger class
     */
    private static Log log = LogFactory.getLog(StateEventHandler.class);

    /**
     * Reference to messaging engine. This is used to store/acknowledge messages
     */
    private final MessagingEngine messagingEngine;

    /**
     * total number of {@link DtxDbWriter} handlers
     */
    private final int handlerCount;

    /**
     * Turn is the value of, sequence % groupCount this event handler process events. Turn must be
     * less than groupCount
     */
    private final int turn;

    DtxDbWriter(MessagingEngine engine, int turn, int handlerCount) {
        this.messagingEngine = engine;
        this.turn = turn;
        this.handlerCount = handlerCount;
    }

    @Override
    public void onEvent(InboundEventContainer event, long sequence, boolean endOfBatch) throws Exception {

        long currentTurn = sequence % handlerCount;

        if (turn == currentTurn) {
            if (log.isDebugEnabled()) {
                log.debug("Sequence [ " + sequence + " ] Event " + event.getEventType());

            }
            if (InboundEventContainer.Type.DTX_COMMIT_EVENT == event.getEventType()) {
                event.getDtxBranch().writeToDbOnCommit();
            } else if (InboundEventContainer.Type.DTX_ROLLBACK_EVENT == event.getEventType()) {
                event.getDtxBranch().writeToDbOnRollback();
            } else if (InboundEventContainer.Type.DTX_PREPARE_EVENT == event.getEventType()) {
                event.getDtxBranch().persistRecords();
            }
        }
    }
}
