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

import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;


/**
 * Will be used to clone meta information of MQTT related topic messages received
 */
public class MQTTMetaDataHandler {

    /**
     * Update message metadata for MQTT; after an update of the routing key and the exchange of a message, for
     * durable topic subscriptions.
     *
     * @param routingKey       routing key of the message
     * @param buf              buffer of the original metadata
     * @param originalMetadata source metadata that needs to be copied
     * @param exchange         exchange of the message
     * @return copy of the metadata as a byte array
     */
    public static byte[] constructMetadata(String routingKey, ByteBuffer buf, StorableMessageMetaData originalMetadata,
                                           String exchange) {

        //For MQTT we just need to take a copy
        MQTTMessageMetaData metaInformation = (MQTTMessageMetaData) originalMetadata;
        //Will re-encode the bytes
        return MQTTUtils.encodeMetaInfo(MQTTUtils.MQTT_META_INFO, metaInformation.getMessageID(), metaInformation
                        .getArrivalTime(), false, metaInformation.getQosLevel(), routingKey,
                metaInformation.isPersistent(), metaInformation.getContentSize(), metaInformation.isCompressed());

    }

    /**
     * Update message metadata for MQTT; to indicate the message is a compressed one.
     *
     * @param buf                       buffer of the original metadata
     * @param originalMetadata          source metadata that needs to be copied
     * @param newCompressedMessageValue Value to indicate if the message is compressed or not
     * @return copy of the metadata as a byte array
     */
    public static byte[] constructMetadata(ByteBuffer buf, StorableMessageMetaData originalMetadata,
                                           boolean newCompressedMessageValue) {

        //For MQTT we just need to take a copy
        MQTTMessageMetaData metaInformation = (MQTTMessageMetaData) originalMetadata;
        //Will re-encode the bytes
        return MQTTUtils.encodeMetaInfo(MQTTUtils.MQTT_META_INFO, metaInformation.getMessageID(), metaInformation
                        .getArrivalTime(), false, metaInformation.getQosLevel(), metaInformation
                        .getDestination(),
                metaInformation.isPersistent(), metaInformation.getContentSize(), newCompressedMessageValue);

    }
}
