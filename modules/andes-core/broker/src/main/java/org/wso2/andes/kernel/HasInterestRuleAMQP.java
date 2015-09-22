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
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;

/**
 * This class represents has Interest Delivery Rule
 * This class has info and methods to evaluate whether there is some subscriber's interests like jms Selectors
 */
public class HasInterestRuleAMQP implements AMQPDeliveryRule {
    private static Log log = LogFactory.getLog(HasInterestRuleAMQP.class);
    /**
     * This Subscription used to check whether subscription is interest in the message
     */
    private Subscription amqpSubscription;

    public HasInterestRuleAMQP(Subscription amqpSubscription) {
        this.amqpSubscription = amqpSubscription;
    }

    /**
     * Evaluating the hasInterest delivery rule
     *
     * @return isOKToDelivery
     */
    @Override
    public boolean evaluate(QueueEntry message) {
        /*All we have to do is to find any subscription interest is to call this method.
        This hasInterest method was implemented by QPid developers to identify subscriber's interest from the protocol layer.
        It was implemented according to AMQP spec to support filters like jms selectors */

        if (amqpSubscription.hasInterest(message)) {
            return true;
        } else {
        	if (log.isDebugEnabled()){
                log.debug("Subscriber doesn't interest on this message : id= " + message.getMessage().getMessageNumber());
        	}
            return false;
        }
    }
}
