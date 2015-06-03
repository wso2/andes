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

import io.netty.channel.Channel;
import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;

import java.nio.ByteBuffer;

import static org.dna.mqtt.wso2.AndesMQTTBridge.QOSLevel;

/**
 * Builder pattern to contain parameter values defined through the protocol engine
 */
public class MQTTMessageContext {
    /**
     * The name of the topic a given message is published
     */
    private String topic;
    /**
     * The level of QoS this will be either 0,1 or 2
     * @see org.dna.mqtt.wso2.AndesMQTTBridge.QOSLevel
     */
    private QOSLevel qosLevel;
    /**
     * The message content wrapped as a ByteBuffer
     */
    private ByteBuffer message;
    /**
     * Should this message retained/persisted
     */
    private boolean retain;
    /**
     * MQTT local message ID
     */
    private int mqttLocalMessageID;
    /**
     * Holds the id of the publisher
     */
    private String publisherID;
    /**
     * The ack handler that will ack for QoS 1 and 2 messages
     */
    private PubAckHandler pubAckHandler;
    /**
     * The socket used to communicate with the publisher
     */
    private Channel socket;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public QOSLevel getQosLevel() {
        return qosLevel;
    }

    public void setQosLevel(QOSLevel qosLevel) {
        this.qosLevel = qosLevel;
    }

    public ByteBuffer getMessage() {
        return message;
    }

    public void setMessage(ByteBuffer message) {
        this.message = message;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public int getMqttLocalMessageID() {
        return mqttLocalMessageID;
    }

    public void setMqttLocalMessageID(int mqttLocalMessageID) {
        this.mqttLocalMessageID = mqttLocalMessageID;
    }

    public String getPublisherID() {
        return publisherID;
    }

    public void setPublisherID(String publisherID) {
        this.publisherID = publisherID;
    }

    public PubAckHandler getPubAckHandler() {
        return pubAckHandler;
    }

    public void setPubAckHandler(PubAckHandler pubAckHandler) {
        this.pubAckHandler = pubAckHandler;
    }

    public Channel getSocket() {
        return socket;
    }

    public void setSocket(Channel socket) {
        this.socket = socket;
    }

}
