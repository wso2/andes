/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.mqtt;

import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.DeliverableAndesMetadata;

/**
 * Holds information relevant to a message which is on the fly
 * The life cycle of this information will be stated when a message is sent to the particular subscription
 * This ends when an acknowledgment is received
 */
public class MQTTSubscriptionInformation {
    /**
     * Metadata which holds information that will be used by the cluster representation of the message
     * This data is required, if a message is not acked by a given time frame the rejection handle will be called
     * {@link org.wso2.andes.kernel.Andes#messageRejected(org.wso2.andes.kernel.AndesMessageMetadata)}
     */
    private DeliverableAndesMetadata metadata;

    /**
     * The reference to the local message id generated to represent the cluster message
     */
    private Integer localMessageID;

    public DeliverableAndesMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(DeliverableAndesMetadata metadata) {
        this.metadata = metadata;
    }

    public Integer getLocalMessageID() {
        return localMessageID;
    }

    public void setLocalMessageID(Integer localMessageID) {
        this.localMessageID = localMessageID;
    }
}
