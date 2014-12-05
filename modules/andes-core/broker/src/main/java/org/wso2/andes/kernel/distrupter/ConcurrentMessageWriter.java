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

import com.lmax.disruptor.EventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.MessagingEngine;

import java.util.ArrayList;
import java.util.List;

public class ConcurrentMessageWriter implements EventHandler<InboundEvent> {

    private static Log log = LogFactory.getLog(ConcurrentMessageWriter.class);
    private static final int MAX_ITEM_COUNT = 50; // Max Item count allowed in a single write

    private int totalPendingItems; // To segment batches using record count instead of byte length
    private int concurrentWritersCount;// Total concurrent writers in ring buffer
    private long turn; // turn
    private final List<AndesMessage> messageList;


    public ConcurrentMessageWriter(int writerCount, long turn) {
        totalPendingItems = 0;
        concurrentWritersCount = writerCount;
        this.turn = turn;
        messageList = new ArrayList<AndesMessage>(MAX_ITEM_COUNT);
    }

    @Override
    public void onEvent(InboundEvent inboundEvent, long sequence, boolean endOfBatch) throws Exception {

        if (InboundEvent.Type.MESSAGE_EVENT == inboundEvent.getEventType()) {

            long calculatedTurn = sequence % concurrentWritersCount;

            if (calculatedTurn == turn) {
                // Message parts we write on the fly. It is trade off of memory vs. batching
                // May be we need better handling .. batch that data as well
                messageList.addAll(inboundEvent.messageList);
                totalPendingItems = 1;
                if (log.isDebugEnabled()) {
                    log.debug("[ sequence " + sequence + " ] " + inboundEvent.messageList.size() + " Message parts " +
                            "received from ring buffer.");
                }
            }
        }

        if ((totalPendingItems > MAX_ITEM_COUNT) || (endOfBatch)) {
            // Write message part list to database
            if (messageList.size() > 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Number of message content sent to message store: " + messageList.size());
                }
                MessagingEngine.getInstance().messagesReceived(messageList);

                if(log.isDebugEnabled()) {
                    String msgs = "";
                    for (AndesMessage message: messageList ) {
                        msgs = msgs + message.getMetadata().getMessageID() + " , ";
                    }
                    log.debug("Messages WRITTEN: " + msgs);
                }
                messageList.clear();
            }

            totalPendingItems = 0;
        }

    }
}
