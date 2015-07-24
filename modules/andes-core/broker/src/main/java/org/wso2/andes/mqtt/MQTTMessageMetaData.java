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

/**
 * Represents the meta data object which should represent the MQTT header information which is exchanged
 */
public class MQTTMessageMetaData implements StorableMessageMetaData {
    public static final MessageMetaDataType.Factory<MQTTMessageMetaData> FACTORY = new MetaDataFactory();

    //Will store the following information as meta data of the message
    private long messageID;
    //By default this will be true in the case of MQTT, since all the transactions are made through topics
    private boolean isTopic;
    //The destination of the message, this will be the name of the topic
    private String destination;
    //Should this be persisted or kept in memory
    private boolean isPersistance;
    //The content length of the message
    private int messageLength;
    //The level of qos, this can either be 0,1 or 2
    private int qosLevel;


    /**
     * Will create a metadat object through this method
     *
     * @param mid           meta data identification
     * @param topic         the name of the topic the message has being published
     * @param destination   the detination the message should be sent to
     * @param persistance   does it require the message to be persisted even after the delivery of the message
     * @param messageLength the length of the message which was recived
     */
    public MQTTMessageMetaData(long mid, boolean topic, String destination, boolean persistance, int messageLength,int qos) {
        this.messageID = mid;
        this.isTopic = topic;
        this.destination = destination;
        this.isPersistance = persistance;
        this.messageLength = messageLength;
        this.qosLevel = qos;
    }

    @Override
    public MessageMetaDataType getType() {
        return MessageMetaDataType.META_DATA_MQTT;
    }

    @Override
    public int getStorableSize() {
        return messageLength;
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

    public int getQosLevel() {
        return qosLevel;
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

        /**
         * Will decode the message meata data from the given buffer
         * @param buffer the message information which will be provided
         */
        private void decodeMetaData(ByteBuffer buffer) {
            String information = new String(buffer.array());
            //Will split the Meta Body information
            String[] message_parts = information.split("\\?");
            //Check whether the message parts is split into 2 properly
            if (message_parts.length > 1) {
                for (String keyValue : message_parts[1].split(",")) {
                    String[] pairs = keyValue.split("=", 2);
                    decodedValues.put(pairs[0], pairs.length == 1 ? "" : pairs[1]);
                }
            }

        }

        /**
         * The values will be extracted from the bytestream and will be decoded         
         * @param buf the bytes stream the data contains
         * @return meta data object
         */
        @Override
        public MQTTMessageMetaData createMetaData(ByteBuffer buf) {
            decodeMetaData(buf);
            Long messageID = Long.parseLong(decodedValues.get("MessageID"));
            //TODO intoduce a static class for this
            boolean isTopic = Boolean.parseBoolean(decodedValues.get("Topic"));
            boolean isPersistant = Boolean.parseBoolean(decodedValues.get("Persistant"));
            int messageContentLength = Integer.parseInt(decodedValues.get("MessageContentLength"));
            int qos = Integer.parseInt(decodedValues.get("QOSLevel"));
            return new MQTTMessageMetaData(messageID,
                    isTopic,
                    decodedValues.get("Destination"),
                    isPersistant,
                    messageContentLength,qos
                    );
        }
    }
}
