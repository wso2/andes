/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.lmax.disruptor.EventFactory;
import org.wso2.andes.server.AMQChannel;

import java.util.UUID;

/**
 * Wrapper class of message acknowledgment data publish to disruptor
 *
 */
public class AndesAckData {

    private long messageID;
    private String destination;
    private String msgStorageDestination;
    private boolean isTopic;
    private UUID channelID;

    public AndesAckData(){}

    public AndesAckData(UUID channelID, long messageID, String destination, String msgStorageDestination, boolean isTopic) {
        super();
        this.channelID = channelID;
        this.messageID = messageID;
        this.destination = destination;
        this.msgStorageDestination = msgStorageDestination;
        this.isTopic = isTopic;
    }

    public AndesRemovableMetadata convertToRemovableMetaData() {
        return new AndesRemovableMetadata(this.messageID, this.msgStorageDestination, this.destination);
    }
    
    public static class AndesAckDataEventFactory implements EventFactory<AndesAckData> {
        @Override
        public AndesAckData newInstance() {
            return new AndesAckData();
        }
    }

    public static EventFactory<AndesAckData> getFactory() {
        return new AndesAckDataEventFactory();
    }

    public long getMessageID() {
        return messageID;
    }

    public void setMessageID(long messageID) {
        this.messageID = messageID;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getMsgStorageDestination() {
        return msgStorageDestination;
    }

    public void setMsgStorageDestination(String msgStorageDestination) {
        this.msgStorageDestination = msgStorageDestination;
    }

    public boolean isTopic() {
        return isTopic;
    }

    public void setTopic(boolean isTopic) {
        this.isTopic = isTopic;
    }

    public UUID getChannelID() {
        return channelID;
    }

    public void setChannelID(UUID channelID) {
        this.channelID = channelID;
    }

}
