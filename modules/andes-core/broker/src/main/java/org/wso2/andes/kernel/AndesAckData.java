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

import java.util.UUID;

public class AndesAckData {
    
    public AndesAckData(){}
    
    public AndesAckData(UUID channelID, long messageID, String destination, String msgStorageDestination, boolean isTopic) {
        super();
        this.channelID = channelID;
        this.messageID = messageID;
        this.destination = destination;
        this.msgStorageDestination = msgStorageDestination;
        this.isTopic = isTopic;
    }
    public long messageID; 
    public String destination;
    public String msgStorageDestination;
    public boolean isTopic;
    public UUID channelID;

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

}
