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

package org.wso2.andes.kernel.distrupter;

import com.lmax.disruptor.EventHandler;

import org.wso2.andes.kernel.AndesSubscription;

import java.util.ArrayList;
import java.util.List;

/**
 * We do this to make Listener take turns while running. So we can run many copies of these and control number
 * of IO threads through that.
 */
public class SubscriptionDataSender implements EventHandler<SubscriptionDataEvent> {
    private int writerCount;
    private int turn;
    private AndesSubscription delivery;
    // TODO - Disruptor make this configurable
    private int MAX_BUFFER_LIMIT = 100;

    private List<SubscriptionDataEvent> messageList = new ArrayList<SubscriptionDataEvent>();

    public SubscriptionDataSender(int writerCount, int turn, AndesSubscription delivery) {
        this.writerCount = writerCount;
        this.turn = turn;
        this.delivery = delivery;
    }

    public void onEvent(final SubscriptionDataEvent event, final long sequence, final boolean endOfBatch) throws Exception {
//        // We write in sequence per subscription
//        long calculatedTurn = Math.abs(event.subscription.getSubscriptionID() % writerCount);
//
//        if (calculatedTurn == turn) {
//            messageList.add(event);
//        }
//
//        if (messageList.size() > MAX_BUFFER_LIMIT || (endOfBatch)) {
//            // Send messages
//            if (messageList.size() > 0) {
//                delivery.sendAsynchronouslyToQueueEndPoint(messageList);
//                messageList.clear();
//            }
//        }
    }
}
