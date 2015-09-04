/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import java.util.UUID;

/**
 * Wrapper class of message acknowledgment data publish to disruptor
 */
public class AndesAckData {

    private DeliverableAndesMetadata acknowledgedMessage;
    private UUID channelID;
    /**
     * Indicate if this is a removable ack data.
     */
    private boolean isRemovable;

    public AndesAckData(UUID channelID, DeliverableAndesMetadata acknowledgedMessage) {
        this.acknowledgedMessage = acknowledgedMessage;
        this.channelID = channelID;
        this.isRemovable = false;
        
    }


    public DeliverableAndesMetadata getAcknowledgedMessage() {
        return acknowledgedMessage;
    }

    public UUID getChannelID() {
        return channelID;
    }


    /**
     * Mark ack data as removable
     */
    public void makeRemovable() {
        isRemovable = true;
    }

    /**
     * Check if the ack data can be removed
     *
     * @return True if ack data can be removed
     */
    public boolean isRemovable() {
        return isRemovable;
    }
}
