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

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription.SubscriptionType;

/**
 * Builder class for {@link org.wso2.andes.subscription.ClusterSubscriptionProcessor}.
 */
public class ClusterSubscriptionProcessorBuilder {

    /**
     * Build a subscription processor with {@link org.wso2.andes.subscription.ClusterSubscriptionBitMapHandler} as
     * subscription processor for all subscription types.
     *
     * @return The {@link org.wso2.andes.subscription.ClusterSubscriptionProcessor} initialized with {@link org.wso2
     * .andes.subscription.ClusterSubscriptionBitMapHandler}
     * @throws AndesException
     */
    public static ClusterSubscriptionProcessor getBitMapClusterSubscriptionProcessor() throws AndesException {
        ClusterSubscriptionProcessor bitMapSubscriptionProcessor = new ClusterSubscriptionProcessor();
        bitMapSubscriptionProcessor.addSubscriptionType(SubscriptionType.AMQP, new ClusterSubscriptionBitMapHandler
                (SubscriptionType.AMQP));
        bitMapSubscriptionProcessor.addSubscriptionType(SubscriptionType.MQTT, new ClusterSubscriptionBitMapHandler
                (SubscriptionType.MQTT));
        return bitMapSubscriptionProcessor;
    }
}
