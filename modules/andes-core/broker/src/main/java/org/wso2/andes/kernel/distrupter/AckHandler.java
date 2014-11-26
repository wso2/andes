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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import com.lmax.disruptor.EventHandler;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.stats.PerformanceCounter;

public class AckHandler implements EventHandler<InboundEvent> {

    private static Log log = LogFactory.getLog(AckHandler.class);
    private int maxAckCount = 100;
    private final List<AndesAckData> ackList;
    private int writerCount;
    private long turn;

    public AckHandler(long turn, int writerCount) {
        this.turn = turn;
        this.writerCount = writerCount;
        ackList = new ArrayList<AndesAckData>(maxAckCount);
    }

    @Override
    public void onEvent(final InboundEvent event, final long sequence, final boolean endOfBatch) throws Exception {

        if (InboundEvent.Type.ACKNOWLEDGEMENT_EVENT == event.getEventType()) {
            // if the turn to process an ack add it to process batch
            long calculatedTurn = sequence % writerCount;
            if (calculatedTurn == turn) {
                AndesAckData ackData = (AndesAckData) event.getData();
                ackList.add(ackData);
                if (log.isDebugEnabled()) {
                    log.debug("[ sequence " + sequence + " ] Ack for message id " + ackData.getMessageID() + " added " +
                            "to ack processing batch.");
                }
            }
        }

        // Irrespective of the event (ACKNOWLEDGEMENT_EVENT) this should execute. endOfBatch might come in any event.
        if (endOfBatch || ackList.size() > maxAckCount) {
            ackReceived(ackList);
            if (log.isDebugEnabled() && (ackList.size() > 0)) {
                log.debug(ackList.size() + " Acknowledgments processed.");
            }
            ackList.clear();
        }
    }

    public static void ackReceived(List<AndesAckData> ackList) throws AndesException {
        List<AndesRemovableMetadata> removableMetadata = new ArrayList<AndesRemovableMetadata>();
        for (AndesAckData ack : ackList) {
            // For topics message is shared. If all acknowledgements are received only we should remove message
            boolean isOkToDeleteMessage = OnflightMessageTracker.getInstance()
                    .handleAckReceived(ack.getChannelID(), ack.getMessageID());
            if (isOkToDeleteMessage) {
                if (log.isDebugEnabled()) {
                    log.debug("Ok to delete message id= " + ack.getMessageID());
                }
                removableMetadata.add(new AndesRemovableMetadata(ack.getMessageID(), ack.getDestination(),
                        ack.getMsgStorageDestination()));
            }

            OnflightMessageTracker.getInstance().decrementNonAckedMessageCount(ack.getChannelID());
            //record ack received

            PerformanceCounter.recordMessageRemovedAfterAck();
        }

        // Depending on the async or direct deletion strategy used in MessagingEngine messages will be deleted
        MessagingEngine.getInstance().deleteMessages(removableMetadata, false);
    }


}
