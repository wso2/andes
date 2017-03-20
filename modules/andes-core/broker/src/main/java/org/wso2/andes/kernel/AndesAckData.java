/*
 * Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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

package org.wso2.andes.kernel;


import java.util.UUID;

/**
 * Container class to hold acknowledge information from protocols
 */
public class AndesAckData {

    /**
     * Id of acknowledged message
     */
    private long messageId;

    /**
     * Id of the channel acknowledge is received
     */
    private UUID channelId;

    /**
     * Generate AndesAckData object.
     *
     * @param channelId ID of the channel ack is received
     * @param messageId Id of the message being acknowledged
     */
    public AndesAckData(UUID channelId, long messageId) {
        this.channelId = channelId;
        this.messageId = messageId;
    }

    /**
     * Get the id of the message being acknowledged
     *
     * @return Metadata of the acknowledged message
     */
    public long getMessageId() {
        return messageId;
    }

    /**
     * Get Id of the channel acknowledgement is received
     *
     * @return channel ID
     */
    public UUID getChannelId() {
        return channelId;
    }
}
