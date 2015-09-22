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
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;

/**
 * This class represents no local Delivery Rule
 * This class has info and methods to evaluate no local option.
 * <p/>
 * No Local option is clearly described in jms spec,
 * It allows us to control sending messages to subscribers whose using the same connection that the publisher used.
 */
public class NoLocalRuleAMQP implements AMQPDeliveryRule {
    private static Log log = LogFactory.getLog(NoLocalRuleAMQP.class);
    private Subscription amqpSubscription;
    private AMQChannel amqChannel;

    public NoLocalRuleAMQP(Subscription amqpSubscription, AMQChannel channel) {
        this.amqpSubscription = amqpSubscription;
        this.amqChannel = channel;
    }

    /**
     * Evaluating the no local delivery rule
     *
     * @return isOkToDelivery
     */
    @Override
    public boolean evaluate(QueueEntry message) {
        AMQMessage amqMessage = (AMQMessage) message.getMessage();
        boolean isOKToDelivery;
        if (amqpSubscription.isNoLocal()) {
        /*
        When subscription's no local option is enabled we should block sending messages to the same session
        that mean we don't send messages for local subscribers
        */
            if (amqMessage.getMessageMetaData().getPublisherSessionID() !=
                amqChannel.getProtocolSession().getSessionID()) {
                isOKToDelivery = true;
            } else {
                isOKToDelivery = false;
                log.warn("No Local violation id: " + amqMessage.getMessageId());
            }
        } else {
            isOKToDelivery = true;
        }
        return isOKToDelivery;
    }
}
