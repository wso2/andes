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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.server.store.StorableMessageMetaData;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class AndesMessageMetadata implements Comparable<AndesMessageMetadata> {

    private static Log log = LogFactory.getLog(AndesMessageMetadata.class);

    /**
     * Unique identifier of the message
     */
    long messageID;
    /**
     * AMQ metadata of the message
     */
    byte[] metadata;
    /**
     * The timestamp at which the message is set to expire.
     */
    long expirationTime;
    /**
     * true if the message is addressed to a topic exchange.
     */
    boolean isTopic;

    /**
     * Name of the exchange message is addressed to
     */
    String messageRouterName;

    /**
     * The timestamp at which the message arrived at the first gates of the broker.
     */
    long arrivalTime;

    /**
     * Destination (routing key) of message
     */
    private String destination;

    /**
     * Queue name in store in which message
     * should be saved
     */
    private String storageQueueName;

    /**
     * True if the message is sent with JMS persistent mode.
     */
    private boolean isPersistent;

    /**
     * Added for MQTT usage
     */
    private int messageContentLength;
    private int qosLevel;

    /**
     * The meta data type which specify which protocol this meta data belongs to``
     */
    private MessageMetaDataType metaDataType;

    private boolean isCompressed;

    /**
     * Properties that are not directly relevant to Andes but to protocols can be stored
     * in this map. But non of the data is persisted
     */
    private Map<String, Object> propertyMap;


    public AndesMessageMetadata() {
        propertyMap = new HashMap<>();
    }

    public AndesMessageMetadata(long messageID, byte[] metadata, boolean parse) {
        super();
        propertyMap = new HashMap<>();
        this.messageID = messageID;
        this.metadata = metadata;
        if (parse) {
            parseMetaData();
        }

    }

    public long getMessageID() {
        return messageID;
    }


    public void setMessageID(long messageID) {
        this.messageID = messageID;
    }

    /**
     * Will retive the level of QOS of the message. This will be applicable only if the message is MQTT
     *
     * @return the level of qos it can be either 0,1 or 2
     */
    public int getQosLevel() {
        return qosLevel;
    }

    /**
     * The level of qos the message is published at
     *
     * @param qosLevel the qos level can be of value 0,1 or 2
     */
    public void setQosLevel(int qosLevel) {
        this.qosLevel = qosLevel;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    public void setMetadata(byte[] metadata) {
        this.metadata = metadata;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public boolean isTopic() {
        return isTopic;
    }

    public String getMessageRouterName() {
        return messageRouterName;
    }

    public void setMessageRouterName(String messageRouterName) {
        this.messageRouterName = messageRouterName;
    }

    public void setTopic(boolean isTopic) {
        this.isTopic = isTopic;
    }

    /**
     * Get the routing key of the message. This is set
     * when parsing metadata.
     *
     * @return routing key of the message
     */
    public String getDestination() {
        return destination;
    }

    /**
     * Set the routing key of the message. If you
     * need to override the value set by parsing metadata
     * this method should be used.
     *
     * @param destination routing key to set
     */
    public void setDestination(String destination) {
        this.destination = destination;
    }

    /**
     * Get the name of storage queue message is saved to. When messages
     * are read from persistent storage and deliver this is used.
     *
     * @return name of storage queue message is persisted to
     */
    public String getStorageQueueName() {
        return storageQueueName;
    }

    /**
     * Set name of the storage queue this message is saved to
     *
     * @param storageQueueName name of storage queue
     */
    public void setStorageQueueName(String storageQueueName) {
        this.storageQueueName = storageQueueName;
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public void setPersistent(boolean persistent) {
        isPersistent = persistent;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    /**
     * Create a clone, with new message ID
     *
     * @param messageId message id
     * @return returns AndesMessageMetadata
     */
    public AndesMessageMetadata shallowCopy(long messageId) {
        AndesMessageMetadata clone = new AndesMessageMetadata();
        clone.messageID = messageId;
        clone.metadata = metadata;
        clone.expirationTime = expirationTime;
        clone.isTopic = isTopic;
        clone.messageRouterName = messageRouterName;
        clone.destination = destination;
        clone.storageQueueName = storageQueueName;
        clone.isPersistent = isPersistent;
        clone.arrivalTime = arrivalTime;
        clone.metaDataType = metaDataType;
        clone.propertyMap = propertyMap;
        clone.messageContentLength = messageContentLength;
        clone.isCompressed = isCompressed;
        return clone;
    }


    /**
     * Update metadata of message, after a change of the routing key and the exchange of a message. This will change
     * AMQP bytes representing metadata. Routing key and exchange name will be set to the given values.
     *
     * @param newDestination  new routing key to set
     * @param newExchangeName new exchange name to set
     */
    public void updateMetadata(String newDestination, String newExchangeName) {
        this.metadata = createNewMetadata(this.metadata, newDestination, newExchangeName);
        this.destination = newDestination;
        if (log.isDebugEnabled()) {
            log.debug("updated andes message metadata id= " + messageID + " new destination = " + newDestination);
        }
    }

    /**
     * Update metadata of message, after a change of the compression state of the message. This will change AMQP bytes
     * representing metadata. IsCompressed will be set to the given value.
     *
     * @param isCompressedMessage new value to indicate if the message is compressed or not
     */
    public void updateMetadata(boolean isCompressedMessage) {
        this.metadata = createNewMetadata(this.metadata, isCompressedMessage);
        this.isCompressed = isCompressedMessage;
        if (log.isDebugEnabled()) {
            log.debug("updated andes message metadata id = " + messageID + ", compression state of the message is " +
                    isCompressedMessage);
        }
    }

    public boolean isExpired() {
        if (expirationTime != 0L) {
            long now = System.currentTimeMillis();
            return (now > expirationTime);
        }
        return false;
    }

    private void parseMetaData() {
        ByteBuffer buf = ByteBuffer.wrap(metadata);
        buf.position(1);
        buf = buf.slice();
        MessageMetaDataType type = MessageMetaDataType.values()[metadata[0]];
        metaDataType = type;
        StorableMessageMetaData mdt = type.getFactory()
                .createMetaData(buf);
        //todo need to discuss on making the flow more generic
        if (type.equals(MessageMetaDataType.META_DATA_0_10) || type.equals(MessageMetaDataType.META_DATA_0_8)) {
            this.isPersistent = ((MessageMetaData) mdt).isPersistent();
            this.expirationTime = ((MessageMetaData) mdt).getMessageHeader().getExpiration();
            this.arrivalTime = ((MessageMetaData) mdt).getArrivalTime();
            this.destination = ((MessageMetaData) mdt).getMessagePublishInfo().getRoutingKey().toString();
            this.messageContentLength = ((MessageMetaData) mdt).getContentSize();
            this.isTopic = ((MessageMetaData) mdt).getMessagePublishInfo().getExchange().equals(AMQPUtils
                    .TOPIC_EXCHANGE_NAME);
            this.messageRouterName = ((MessageMetaData) mdt).getMessagePublishInfo().getExchange().toString();
            this.isCompressed = ((MessageMetaData) mdt).isCompressed();
        }

    }

    /**
     * Create a copy of updated metadata, for durable topic subscriptions
     *
     * @param originalMetadata source metadata that needs to be copied
     * @param routingKey       routing key of the message
     * @param exchangeName     exchange of the message
     * @return copy of the metadata as a byte array
     */
    private byte[] createNewMetadata(byte[] originalMetadata, String routingKey, String exchangeName) {
        ByteBuffer buf = ByteBuffer.wrap(originalMetadata);
        buf.position(1);
        buf = buf.slice();
        MessageMetaDataType type = MessageMetaDataType.values()[originalMetadata[0]];
        metaDataType = type;
        StorableMessageMetaData originalMessageMetadata = type.getFactory().createMetaData(buf);

        byte[] underlying;

        underlying = AMQPMetaDataHandler.constructMetadata(routingKey, buf, originalMessageMetadata, exchangeName);

        return underlying;
    }

    /**
     * Create a copy of metadata
     *
     * @param originalMetadata          source metadata that needs to be copied
     * @param isCompressed Value to indicate if the message is compressed or not
     * @return copy of the metadata as a byte array
     */
    private byte[] createNewMetadata(byte[] originalMetadata, boolean isCompressed) {
        ByteBuffer buf = ByteBuffer.wrap(originalMetadata);
        buf.position(1);
        buf = buf.slice();
        MessageMetaDataType type = MessageMetaDataType.values()[originalMetadata[0]];
        metaDataType = type;
        StorableMessageMetaData originalMessageMetadata = type.getFactory().createMetaData(buf);

        byte[] underlying;

        underlying = AMQPMetaDataHandler.constructMetadata(buf, originalMessageMetadata, isCompressed);


        return underlying;
    }

    public int getMessageContentLength() {
        return messageContentLength;
    }

    public void setMessageContentLength(int messageContentLength){
        this.messageContentLength = messageContentLength;
    }

    public boolean isCompressed() {
        return isCompressed;
    }

    public void setCompressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
    }

    @Override
    public int compareTo(AndesMessageMetadata other) {
        if (this.getMessageID() == other.getMessageID()) {
            return 0;
        } else {
            return this.getMessageID() > other.getMessageID() ? 1 : -1;
        }
    }

    public MessageMetaDataType getMetaDataType() {
        return metaDataType;
    }

    public void setMetaDataType(MessageMetaDataType metaDataType) {
        this.metaDataType = metaDataType;
    }

    /**
     * Add a property that is not directly relevant to Andes. The properties are not persistent. Lost when the object is
     * deleted
     * @param key String Key
     * @param value Object. Value of the property
     */
    public void addProperty(String key, Object value) {
        propertyMap.put(key, value);
    }

    /**
     * Returns the property for the given
     * @param key String
     * @return value of the property. Null if not found
     */
    public Object getProperty(String key) {
        return propertyMap.get(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AndesMessageMetadata)){
            return false;
        }
        AndesMessageMetadata andesMessageMetadata = (AndesMessageMetadata) o;
        return getMessageID() == andesMessageMetadata.getMessageID();
    }

    @Override
    public int hashCode() {
        return (int) (getMessageID() ^ (getMessageID() >>> 32));
    }

    /**
     * True if the expiration time is defined for the message.
     */
    public boolean isExpirationDefined() {
        return 0L < expirationTime;
    }

}
