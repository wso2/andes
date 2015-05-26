/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.mqtt;

import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesSubscription;

/**
 * This is a sub class of AndesMessage which contains MQTT protocol specific implementations
 */
public class MQTTMessage extends AndesMessage {

    public MQTTMessage(AndesMessageMetadata metadata) {
        super(metadata);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDelivarable(AndesSubscription subscription) {

        // Avoid adding QOS 0 MQTT messages to clean session = false subscribers if disconnected
        if (subscription.isDurable()
                && !subscription.hasExternalSubscriptions()
                && 0 == this.getMetadata().getQosLevel()) {
            return false;
        }

        return true;
    }

}
