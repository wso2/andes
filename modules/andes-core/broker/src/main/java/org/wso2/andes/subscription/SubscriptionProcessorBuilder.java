/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.subscription;

import org.wso2.andes.amqp.AMQPSubscriptionHandler;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ProtocolType;

/**
 * Builder class for {@link SubscriptionProcessor}.
 */
public class SubscriptionProcessorBuilder {

    /**
     * Build a subscription processor with {@link TopicSubscriptionBitMapHandler} as
     * subscription processor for all subscription types.
     *
     * @return The {@link SubscriptionProcessor} initialized with {@link org.wso2
     * .andes.subscription.ClusterSubscriptionBitMapHandler}
     * @throws AndesException
     */
    public static SubscriptionProcessor getBitMapClusterSubscriptionProcessor() throws AndesException {
        SubscriptionProcessor bitMapSubscriptionProcessor = new SubscriptionProcessor();
        bitMapSubscriptionProcessor.addProtocolType(ProtocolType.AMQP, new TopicSubscriptionBitMapHandler
                (ProtocolType.AMQP));
        bitMapSubscriptionProcessor.addProtocolType(ProtocolType.MQTT, new TopicSubscriptionBitMapHandler
                (ProtocolType.MQTT));
        return bitMapSubscriptionProcessor;
    }

    public static SubscriptionProcessor getProtocolSpecificProcessor() throws AndesException {
        SubscriptionProcessor subscriptionProcessor = new SubscriptionProcessor();

        subscriptionProcessor.addProtocolType(ProtocolType.AMQP, new AMQPSubscriptionHandler());

        subscriptionProcessor.addProtocolType(ProtocolType.MQTT,
                new TopicSubscriptionBitMapHandler(ProtocolType.MQTT));

        return subscriptionProcessor;
    }
}
