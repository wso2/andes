/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
    //Time message has arrived (published) to broker
    private long messageArrivalTime;
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
    //Value to indicate, if the message is a compressed one or not
    private boolean isCompressed = false;


    /**
     * Will create a metadat object through this method
     *
     * @param mid                meta data identification
     * @param messageArrivalTime time message has arrived to broker
     * @param topic              the name of the topic the message has being published
     * @param destination        the detination the message should be sent to
     * @param persistance        does it require the message to be persisted even after the delivery of the message
     * @param messageLength      the length of the message which was recived
     * @param qos                The level of qos
     * @param isCompressed       Value to indicate, if the message is a compressed one or not
     */
    public MQTTMessageMetaData(long mid, long messageArrivalTime, boolean topic, String destination, boolean
            persistance, int messageLength, int qos, boolean isCompressed) {
        this.messageID = mid;
        this.messageArrivalTime = messageArrivalTime;
        this.isTopic = topic;
        this.destination = destination;
        this.isPersistance = persistance;
        this.messageLength = messageLength;
        this.qosLevel = qos;
        this.isCompressed = isCompressed;
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

    /**
     * Get time message has arrived to the broker
     *
     * @return time in milli seconds
     */
    public long getArrivalTime() {
        return messageArrivalTime;
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

    public boolean isCompressed() {
        return isCompressed;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    private static class MetaDataFactory implements MessageMetaDataType.Factory<MQTTMessageMetaData> {
        /**
         * Will decode the message meata data from the given buffer
         *
         * @param buffer the message information which will be provided
         */
        private Map<String, String> decodeMetaData(ByteBuffer buffer) {
            Map<String, String> decodedValues = new HashMap<>();

            String information = new String(buffer.array());
            //Will split the Meta Body information
            String[] messageParts = information.split("\\?");
            //Check whether the message parts is split into 2 properly
            if (messageParts.length > 1) {
                for (String keyValue : messageParts[1].split(",")) {
                    String[] pairs = keyValue.split("=", 2);
                    decodedValues.put(pairs[0], pairs.length == 1 ? "" : pairs[1]);
                }
            }

            return decodedValues;
        }

        /**
         * The values will be extracted from the bytestream and will be decoded
         *
         * @param buf the bytes stream the data contains
         * @return meta data object
         */
        @Override
        public MQTTMessageMetaData createMetaData(ByteBuffer buf) {
            Map<String, String> decodedValues = decodeMetaData(buf);
            Long messageID = Long.parseLong(decodedValues.get("MessageID"));
            //TODO intoduce a static class for this
            boolean isTopic = Boolean.parseBoolean(decodedValues.get("Topic"));
            boolean isPersistant = Boolean.parseBoolean(decodedValues.get("Persistant"));
            int messageContentLength = Integer.parseInt(decodedValues.get("MessageContentLength"));
            int qos = Integer.parseInt(decodedValues.get("QOSLevel"));
            long messageArrivalTime = Long.parseLong(decodedValues.get(MQTTUtils.ARRIVAL_TIME));

            // This is an optional property, to indicate; if the message is compressed or not
            boolean isCompressed = false;
            if (decodedValues.containsKey(MQTTUtils.IS_COMPRESSED)) {
                isCompressed = Boolean.parseBoolean(decodedValues.get(MQTTUtils.IS_COMPRESSED));
            }

            return new MQTTMessageMetaData(messageID,
                    messageArrivalTime,
                    isTopic,
                    decodedValues.get("Destination"),
                    isPersistant,
                    messageContentLength, qos,
                    isCompressed
            );
        }
    }
}
