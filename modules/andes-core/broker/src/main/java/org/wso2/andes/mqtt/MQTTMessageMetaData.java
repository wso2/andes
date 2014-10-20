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

package org.wso2.andes.mqtt;

import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


public class MQTTMessageMetaData implements StorableMessageMetaData {
    public static final MessageMetaDataType.Factory<MQTTMessageMetaData> FACTORY = new MetaDataFactory();

    /*Will Gather All the Information*/
    private long messageID;
    private boolean isTopic;
    private String destination;
    private boolean isPersistance;
    private int messageLength;

    public MQTTMessageMetaData(long mid, boolean topic, String destination, boolean persistance, int messageLength) {
        this.messageID = mid;
        this.isTopic = topic;
        this.destination = destination;
        this.isPersistance = persistance;
        this.messageLength = messageLength;
    }

    @Override
    public MessageMetaDataType getType() {
        return null;
    }

    @Override
    public int getStorableSize() {
        return 0;
    }

    @Override
    public int writeToBuffer(int offsetInMetaData, ByteBuffer dest) {
        return 0;
    }

    @Override
    public int getContentSize() {
        return messageLength;
    }

    @Override
    public boolean isPersistent() {
        return isPersistance;
    }

    public long getMessageID() {
        return messageID;
    }

    public void setMessageID(long messageID) {
        this.messageID = messageID;
    }

    public boolean isTopic() {
        return isTopic;
    }

    public void setTopic(boolean topic) {
        isTopic = topic;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    private static class MetaDataFactory implements MessageMetaDataType.Factory<MQTTMessageMetaData> {

        private Map<String, String> decodedValues = new HashMap<String, String>();

        /*Will Decode the String and re-gain access to meta information*/
        private void decodeMetaData(ByteBuffer buffer) {
            String information = new String(buffer.array());
            //Will split the Meta Body information
            String[] message_parts = information.split(":");
            for (String keyValue : message_parts[1].split(",")) {
                String[] pairs = keyValue.split("=", 2);
                decodedValues.put(pairs[0], pairs.length == 1 ? "" : pairs[1]);
            }

        }

        @Override
        public MQTTMessageMetaData createMetaData(ByteBuffer buf) {
            //Will call the decode meta-data option
            //todo need to add a constant class to decode the values
            decodeMetaData(buf);
            return new MQTTMessageMetaData(Long.parseLong(decodedValues.get("MessageID")), Boolean.parseBoolean(decodedValues.get("Topic")), decodedValues.get("Destination"),
                    Boolean.parseBoolean(decodedValues.get("Persistant")), Integer.parseInt(decodedValues.get("MessageContentLength")));

        }
    }
}
