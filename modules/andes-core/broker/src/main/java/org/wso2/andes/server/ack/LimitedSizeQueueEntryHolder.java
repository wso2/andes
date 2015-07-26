/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.ack;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.QueueEntry;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class provides a modified LinkedHashMap behaviour. It automatically removes
 * the eldest entry as the new entries are put in if the conditions are met.
 */
public class LimitedSizeQueueEntryHolder extends LinkedHashMap<Long, QueueEntry> {

    protected static final Logger _logger = Logger.getLogger(LimitedSizeQueueEntryHolder.class);

    //after the map size reach this limit eldest elements are considered to be removed
    private int growLimit;

    private AMQChannel amqChannel;

    /**
     * Create a LimitedSizeQueueEntryHolder. This has all the functionality of LinkedHashMap.
     * @param initialCapacity initial capacity of the internal LinkedHashMap
     * @param growLimit allowed size of the map to grow. After this size is breached eldest entry is considered
     *                  to be removed
     * @param amqChannel AMQ channel Queue entry holder is registered.
     */
    public LimitedSizeQueueEntryHolder(int initialCapacity, int growLimit, AMQChannel amqChannel) {
        super(initialCapacity);
        this.growLimit = growLimit;
        this.amqChannel = amqChannel;
    }

    /**
     * Here we consider to remove the eldest entry of the map judging by inserted order.
     * @param eldest entry to remove
     * @return if removed
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<Long, QueueEntry> eldest) {
        if(size() > growLimit && eldest.getValue().isTimelyDisposable()) {
            QueueEntry eldestQueueEntry = eldest.getValue();
            eldestQueueEntry.getMessage().getArrivalTime();
            _logger.warn("Simulating Acknowledgement and removing queue entry id= " + eldestQueueEntry.getMessage()
                    .getMessageNumber() + " as it is "
                    + "growing");
            simulateAcknowledgement(eldestQueueEntry);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Simulate an acknowledgement for the queue entry
     * @param queueEntry queue entry to simulate the acknowledgement
     */
    private void simulateAcknowledgement(QueueEntry queueEntry) {
        try {
            boolean isTopic = ((AMQMessage) queueEntry.getMessage()).getMessagePublishInfo()
                    .getExchange()
                    .equals(AMQPUtils
                            .TOPIC_EXCHANGE_NAME);
            QpidAndesBridge.ackReceived(amqChannel.getId(), queueEntry.getMessage().getMessageNumber(),
                    queueEntry.getMessage().getRoutingKey(),
                    isTopic);
        } catch (AMQException e) {
            _logger.error("Error while simulating acknowledgement for message id= " + queueEntry.getMessage()
                    .getMessageNumber(), e);
        }
    }
}
