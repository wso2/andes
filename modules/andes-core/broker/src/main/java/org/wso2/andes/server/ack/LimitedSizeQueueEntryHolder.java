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
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.QueueEntry;

import java.util.LinkedHashMap;
import java.util.Map;

public class LimitedSizeQueueEntryHolder extends LinkedHashMap<Long, QueueEntry> {

    protected static final Logger _logger = Logger.getLogger(LimitedSizeQueueEntryHolder.class);

    private final int limit = AndesConfigurationManager.readValue
            (AndesConfiguration.PERFORMANCE_TUNING_ACK_HANDLING_MAX_UNACKED_MESSAGES);

    private AMQChannel amqChannel;

    public LimitedSizeQueueEntryHolder(int initialCapacity, AMQChannel amqChannel) {
        super(initialCapacity);
        this.amqChannel = amqChannel;
    }
    @Override
    protected boolean removeEldestEntry(Map.Entry<Long, QueueEntry> eldest) {
        if(size() > limit) {
            QueueEntry eldestQueueEntry = eldest.getValue();
            simulateAcknowledgement(eldestQueueEntry);
            _logger.warn("Removing queue entry id= " + eldestQueueEntry.getMessage().getMessageNumber() + " as it is " +
                    "growing");
            return true;
        } else {
            return false;
        }
    }

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
                    .getMessageNumber());
        }
    }
}
