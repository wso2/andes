/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.andes.configuration.models.performance;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for message expiration related configs
 */
@Configuration(description = "Message expiration can be set for each messages which are published to Wso2 MB.\n"
        + "After the expiration time, the messages will not be delivered to the consumers. Eventually\n"
        + "they got deleted inside the MB.")
public class MessageExpirationConfiguration {

    @Element(description = "When messages delivered, in the delivery path messages were checked whether they are\n"
            + " already expired. If expired at that time add that message to a queue for a future batch delete.\n"
            + " This interval decides on the time gap between the batch deletes. Time interval specified in seconds.")
    private int preDeliveryExpiryDeletionInterval = 10;

    @Element(description = "Periodically check the database for new expired messages which were not assigned to\n"
            + "any slot delivery worker so far and delete them. This interval decides on the time gap between\n"
            + "the periodic message deletion. Time interval specified in seconds.")
    private int periodicMessageDeletionInterval = 900;

    @Element(description = "When checking the database for expired messages, the messages which were handled by the "
            + "slot delivery worker should no be touched since that mess up the slot delivery worker functionality.\n"
            + "Those messages anyways get caught at the message delivery path. So there is a need to have a safe\n"
            + "buffer of slots which can be allocated to a slot delivery worker in the near future. The specified\n"
            + "number of slots from the last assigned should not be touched by the periodic deletion task.")
    private int safetySlotCount = 3;

    public int getPreDeliveryExpiryDeletionInterval() {
        return preDeliveryExpiryDeletionInterval;
    }

    public int getPeriodicMessageDeletionInterval() {
        return periodicMessageDeletionInterval;
    }

    public int getSafetySlotCount() {
        return safetySlotCount;
    }

}
