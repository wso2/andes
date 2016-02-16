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


import org.wso2.andes.mqtt.subscriptions.SubscriptionsStore;

public class MQTTUtils {

    public static final String MULTI_LEVEL_WILDCARD = "#";
    public static final String SINGLE_LEVEL_WILDCARD = "+";
    public static final String PROTOCOL_TYPE = "MQTT";

    public static final String MESSAGE_ID = "MessageID";
    private static final String TOPIC = "Topic";
    public static final String ARRIVAL_TIME = "ArrivalTime";
    private static final String DESTINATION = "Destination";
    private static final String PERSISTENCE = "Persistent";
    private static final String MESSAGE_CONTENT_LENGTH = "MessageContentLength";
    public static final String QOSLEVEL = "QOSLevel";
    public static final String IS_COMPRESSED = "IsCompressed";

    public static final String MQTT_META_INFO = "\u0002MQTT Protocol v3.1";


    /**
     * Check if a subscribed queue bound destination routing key matches with a given message routing key using MQTT
     * wildcards.
     *
     * @param queueBoundRoutingKey The subscribed destination with/without wildcards
     * @param messageRoutingKey    The message destination routing key without wildcards
     * @return Is queue bound routing key match the message routing key
     */
    public static boolean isTargetQueueBoundByMatchingToRoutingKey(String queueBoundRoutingKey,
                                                                   String messageRoutingKey) {
        return SubscriptionsStore.matchTopics(messageRoutingKey, queueBoundRoutingKey);
    }

    /**
     * The data about the message (meta information) will be constructed at this phase Andes requires the meta data as a
     * byte stream, The method basically collects all the relevant information necessary to construct the bytes stream
     * and will convert the message into a bytes object
     *
     * @param metaData      the information about the published message
     * @param messageID     the identity of the message
     * @param arrivalTime   the arrival time of the message
     * @param topic         the value to indicate if this is a topic or not
     * @param qos           the level of qos the message was published at
     * @param destination   the definition where the message should be sent to
     * @param persistence   should this message be persisted
     * @param contentLength the length of the message content
     * @param isCompressed  the value to indicate, if the message is compressed or not
     * @return the collective information as a bytes object
     */
    public static byte[] encodeMetaInfo(String metaData, long messageID, long arrivalTime, boolean topic, int qos,
                                        String destination, boolean persistence, int contentLength, boolean
            isCompressed) {
        byte[] metaInformation;
        String information = metaData + "?" + MESSAGE_ID + "=" + messageID + "," + ARRIVAL_TIME + "=" + arrivalTime
                             + "," + TOPIC + "=" + topic + "," + DESTINATION + "=" + destination + "," + PERSISTENCE
                             + "=" + persistence + "," + MESSAGE_CONTENT_LENGTH + "=" + contentLength + "," +
                             QOSLEVEL + "=" + qos
                             + "," + IS_COMPRESSED + "=" + isCompressed;
        metaInformation = information.getBytes();
        return metaInformation;
    }
}
