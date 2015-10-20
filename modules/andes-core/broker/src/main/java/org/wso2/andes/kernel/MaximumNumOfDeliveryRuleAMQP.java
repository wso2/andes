/*
* Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.QueueEntry;

import java.util.UUID;

/**
 * This class represents Counting Delivery Rule
 * This class has info and methods to evaluate counting delivery rule
 */
public class MaximumNumOfDeliveryRuleAMQP implements AMQPDeliveryRule {
    private static Log log = LogFactory.getLog(MaximumNumOfDeliveryRuleAMQP.class);
    private UUID amqChannelID;
    /**
     * Maximum number of times a message is tried to deliver
     */
    private int maximumRedeliveryTimes = (Integer) AndesConfigurationManager
            .readValue(AndesConfiguration.TRANSPORTS_AMQP_MAXIMUM_REDELIVERY_ATTEMPTS);

    public MaximumNumOfDeliveryRuleAMQP(AMQChannel channel) {
        this.amqChannelID = channel.getId();
    }

    /**
     * Evaluating the maximum number of delivery rule
     *
     * @return isOKToDelivery
     * @throws AndesException
     */
    @Override
    public boolean evaluate(QueueEntry message) throws AndesException {
        long messageID = message.getMessage().getMessageNumber();
        //Check if number of redelivery tries has breached.
        //we should allow a number of delivery attempts that is equal to the maximumRedeliveryTries + 1
        //since we set the limit on maxRedeliveryTries rather than maxDeliveryTries
        ProtocolMessage protocolMessage = ((AMQMessage)message.getMessage()).getAndesMetadataReference();
        int numOfDeliveriesOfCurrentMsg = protocolMessage.getNumberOfDeliveriesForProtocolChannel();

        if (numOfDeliveriesOfCurrentMsg > maximumRedeliveryTimes + 1) {
            log.warn("Number of Maximum Redelivery Tries Has Breached. Message id = " + messageID);
            return false;
        } else {
            return true;
        }
    }
}
