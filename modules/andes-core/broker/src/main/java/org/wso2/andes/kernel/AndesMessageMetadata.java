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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * Andes core representation of message metadata. This contains the metadata fields of a message required for
 * routing.
 * </p>
 * <p>
 * <p>
 * In addition protocol specific metadata can be stored as well (encoded in a byte stream).
 * {@link AndesEncodingUtil} can be used to encode metadata to a byte stream
 *
 * @see #setProtocolMetadata(byte[])
 * @see #getProtocolMetadata()
 * </p>
 */
public class AndesMessageMetadata implements Comparable<AndesMessageMetadata> {

    private static Log log = LogFactory.getLog(AndesMessageMetadata.class);

    /**
     * Unique identifier of the message
     */
    private long messageID;

    /**
     * Content length of the message in bytes
     */
    private int messageContentLength;

    /**
     * The timestamp at which the message is set to expire.
     */
    private long expirationTime;

    /**
     * The timestamp at which the message arrived at the first gates of the broker.
     */
    private long arrivalTime;

    /**
     * Denotes if the content is compressed or not
     */
    private boolean isCompressed;

    /**
     * {@link ProtocolType} of the metadata
     */
    private ProtocolType protocolType;

    /**
     * Destination (routing key) of message
     */
    private String destination;

    /**
     * This destination name is used when persisting the messages
     */
    private String storageDestination;

    /**
     * Protocol specific metadata. This part is not decoded by the core
     */
    private byte[] protocolMetadata;

    /**
     * Whether the message should be persisted or not
     */
    private boolean isPersistent;

    private String messageRouterName;

    private boolean isTopic;

    /**
     * Properties that are not directly relevant to Andes but to protocols can be stored
     * in this map. But non of the data is persisted
     */
    private Map<String, Object> temporaryPropertiesMap;

    /**
     * <p>
     * Create {@link AndesMessageMetadata} object
     * </p>
     *
     * @param messageId    message id that is used by Andes core
     * @param destination  destination of the message
     * @param protocolType protocol type of the metadata
     */
    public AndesMessageMetadata(long messageId, String destination, ProtocolType protocolType) {
        setMessageID(messageId);
        setProtocolType(protocolType);
        setDestination(destination);
        setStorageDestination(destination);
        this.isCompressed = false;
        temporaryPropertiesMap = new ConcurrentHashMap<>();
    }

    /**
     * <p>
     * Create {@link AndesMessageMetadata} object
     * </p>
     *
     * @param destination  destination of the message
     * @param protocolType {@link ProtocolType} of the message
     */
    public AndesMessageMetadata(String destination, ProtocolType protocolType) {
        this(0L, destination, protocolType);
    }

    /**
     * Create {@link AndesMessageMetadata} object from an encoded metadata byte array
     *
     * @param metadata encoded byte array
     */
    public AndesMessageMetadata(byte[] metadata) {
        decode(metadata);
        temporaryPropertiesMap = new ConcurrentHashMap<>();
    }

    /**
     * <p>
     * Return the encoded byte representation of metadata
     * </p>
     *
     * @return byte array
     */
    public byte[] getBytes() {
        return encode().array();
    }

    /**
     * Get the byte length of the encoded metadata
     *
     * @return length in bytes
     */
    public int getStorableSize() {

        return AndesEncodingUtil.getEncodedLongLength() + // message id
                AndesEncodingUtil.getEncodedIntLength() + //  message content length
                AndesEncodingUtil.getEncodedLongLength() + // message expiration time
                AndesEncodingUtil.getEncodedLongLength() + // arrival time
                AndesEncodingUtil.getEncodedBooleanLength() + // is topic
                AndesEncodingUtil.getEncodedBooleanLength() + // is compressed
                AndesEncodingUtil.getEncodedBooleanLength() + // is persistent
                AndesEncodingUtil.getEncodedStringLength(protocolType.toString()) +
                AndesEncodingUtil.getEncodedStringLength(destination) +
                AndesEncodingUtil.getEncodedStringLength(storageDestination) +
                AndesEncodingUtil.getEncodedStringLength(messageRouterName) +
                getProtocolMetadata().length; // protocol related metadata

    }

    /**
     * Encode {@link AndesMessageMetadata} variables into a byte array
     *
     * @return {@link ByteBuffer} that wraps the byte array
     */
    private ByteBuffer encode() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(getStorableSize());
        AndesEncodingUtil.putLong(byteBuffer, getMessageID());
        AndesEncodingUtil.putInt(byteBuffer, getMessageContentLength());
        AndesEncodingUtil.putLong(byteBuffer, getExpirationTime());
        AndesEncodingUtil.putLong(byteBuffer, getArrivalTime());

        // TODO: need to optimise boolean encoding (store in single byte using bit masks)
        AndesEncodingUtil.putBoolean(byteBuffer, isTopic());
        AndesEncodingUtil.putBoolean(byteBuffer, isCompressed());
        AndesEncodingUtil.putBoolean(byteBuffer, isPersistent());

        AndesEncodingUtil.putString(byteBuffer, protocolType.toString());
        AndesEncodingUtil.putString(byteBuffer, getDestination());
        AndesEncodingUtil.putString(byteBuffer, getStorageDestination());
        AndesEncodingUtil.putString(byteBuffer, getMessageRouterName());
        byteBuffer.put(getProtocolMetadata());
        if (log.isDebugEnabled()) {
            log.debug("Message encoded " + this);
        }
        return byteBuffer;
    }

    /**
     * Decode the given byte array and populate the variable in {@link AndesMessageMetadata}
     *
     * @param src encoded byte array
     */
    private void decode(byte[] src) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(src);
        setMessageID(AndesEncodingUtil.getEncodedLong(byteBuffer));
        setMessageContentLength(AndesEncodingUtil.getEncodedInt(byteBuffer));
        setExpirationTime(AndesEncodingUtil.getEncodedLong(byteBuffer));
        setArrivalTime(AndesEncodingUtil.getEncodedLong(byteBuffer));
        setTopic(AndesEncodingUtil.getBoolean(byteBuffer));
        setCompressed(AndesEncodingUtil.getBoolean(byteBuffer));
        setPersistent(AndesEncodingUtil.getBoolean(byteBuffer));
        setProtocolType(ProtocolType.valueOf(AndesEncodingUtil.getString(byteBuffer)));
        setDestination(AndesEncodingUtil.getString(byteBuffer));
        setStorageDestination(AndesEncodingUtil.getString(byteBuffer));
        setMessageRouterName(AndesEncodingUtil.getString(byteBuffer));
        byte[] metadata = new byte[byteBuffer.remaining()];
        byteBuffer.get(metadata);
        setProtocolMetadata(metadata);
        if (log.isDebugEnabled()) {
            log.debug("Message decoded " + this);
        }
    }

    /**
     * <p>
     * Unique message id generated by Andes core. Not relevant to any protocol
     * </p>
     *
     * @return unque message id
     */
    public long getMessageID() {
        return messageID;
    }

    public boolean isTopic() {
        return isTopic;
    }

    public void setTopic(boolean isTopic) {
        this.isTopic = isTopic;
    }

    /**
     * <p>
     * Set a unique message id for the core. Message id's are generated and set to messages from
     * {@link org.wso2.andes.kernel.disruptor.inbound.MessagePreProcessor}
     * </p>
     *
     * @param messageID unique message id
     */
    public void setMessageID(long messageID) {
        this.messageID = messageID;
    }

    /**
     * <p>
     * Get expiration time
     * </p>
     *
     * @return expiration time in milliseconds
     */
    public long getExpirationTime() {
        return expirationTime;
    }

    /**
     * <p>
     * The timestamp at which the message is set to expire. Message won't be delivered to subscribers after the
     * expiration time
     * </p>
     *
     * @param expirationTime time in milliseconds
     */
    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public String getMessageRouterName() {
        return messageRouterName;
    }

    public void setMessageRouterName(String messageRouterName) {
        this.messageRouterName = messageRouterName;
    }

    /**
     * <p>
     * Destination (routing key) of message
     * </p>
     *
     * @return destination
     */
    public String getDestination() {
        return destination;
    }

    /**
     * <p>
     * set the destination (routing key) of the message
     * </p>
     *
     * @param destination destination of the message
     */
    public void setDestination(String destination) {
        this.destination = destination;
    }

    /**
     * <p>
     * This destination name is used when persisting the messages
     * </p>
     *
     * @return storage destination
     */
    public String getStorageDestination() {
        return storageDestination;
    }

    /**
     * <p>
     * The destination name used when persisting the messages
     * </p>
     *
     * @param storageDestination storage destination of the message
     */
    public void setStorageDestination(String storageDestination) {
        this.storageDestination = storageDestination;
    }

    /**
     * <p>
     * The timestamp at which the message arrived at Andes
     * </p>
     *
     * @return arrival time in milliseconds
     */
    public long getArrivalTime() {
        return arrivalTime;
    }

    /**
     * <p>
     * Set the timestamp at which the message arrived at Andes.
     * </p>
     *
     * @param arrivalTime message arrival time (to broker) in milliseconds
     */
    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    /**
     * <p>
     * Create a clone, with new message ID
     * </p>
     *
     * @param messageId message id
     * @return returns AndesMessageMetadata
     */
    public AndesMessageMetadata shallowCopy(long messageId) {
        AndesMessageMetadata clone = new AndesMessageMetadata(messageId, destination, protocolType);
        clone.expirationTime = expirationTime;
        clone.storageDestination = storageDestination;
        clone.arrivalTime = arrivalTime;
        clone.messageContentLength = messageContentLength;
        clone.isCompressed = isCompressed;
        clone.isPersistent = isPersistent;
        clone.protocolMetadata = protocolMetadata;
        clone.temporaryPropertiesMap = temporaryPropertiesMap;
        clone.messageRouterName = messageRouterName;
        clone.isTopic = isTopic;
        return clone;
    }

    /**
     * <p>
     * Add a property that is not directly relevant to Andes. The properties are not persistent. Lost when the
     * object is gc'ed. This is just a place holder for transient data. Properties are not encoded into the
     * metadata byte array. For instance data can be kept until a message is put to inbound disruptor and pub ack
     * is received from it.
     * </p>
     * <p>
     * <p>
     * The properties can be accessed concurrently
     * </p>
     *
     * @param key   String Key
     * @param value Object. Value of the property
     */
    public void addTemporaryProperty(String key, Object value) {
        temporaryPropertiesMap.put(key, value);
    }

    /**
     * Returns the property for the given key
     * <p>
     * Properties can be accessed concurrently
     *
     * @param key String
     * @return value of the property. Null if not found
     */
    public Object getTemporaryProperty(String key) {
        return temporaryPropertiesMap.get(key);
    }

    /**
     * <p>
     * Whether the message is persisted or not
     * </p>
     *
     * @return message persisted if true
     */
    public boolean isPersistent() {
        return isPersistent;
    }

    /**
     * <p>
     * Set message persistence flag
     * </p>
     *
     * @param persistent true if need to be persisted
     */
    public void setPersistent(boolean persistent) {
        isPersistent = persistent;
    }

    /**
     * <p>
     * Check whether the message is expired.
     * </p>
     *
     * @return true if expired and vice versa
     * @see #setExpirationTime(long)
     */
    public boolean isExpired() {
        if (expirationTime != 0L) {
            long now = System.currentTimeMillis();
            return (now > expirationTime);
        }
        return false;
    }

    /**
     * <p>
     * Retrieve content length of the message
     * </p>
     *
     * @return content length in bytes
     */
    public int getMessageContentLength() {
        return messageContentLength;
    }

    /**
     * <p>
     * Set message content length
     * </p>
     *
     * @param messageContentLength content length in bytes
     */
    public void setMessageContentLength(int messageContentLength) {
        this.messageContentLength = messageContentLength;
    }

    /**
     * <p>
     * Whether the content is compressed or not by Andes core
     * </p>
     *
     * @return true if compressed and vice versa
     */
    public boolean isCompressed() {
        return isCompressed;
    }

    /**
     * <p>
     * If set to true message content will be compressed before persisting when publishing a message to Andes core
     * </p>
     *
     * @param isCompressed message is compressed if true
     */
    public void setCompressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(AndesMessageMetadata other) {
        if (this.getMessageID() == other.getMessageID()) {
            return 0;
        } else {
            return this.getMessageID() > other.getMessageID() ? 1 : -1;
        }
    }

    /**
     * <p>
     * {@link ProtocolType} of the metadata
     * </p>
     *
     * @return {@link ProtocolType}
     */
    public ProtocolType getProtocolType() {
        return protocolType;
    }

    /**
     * <p>
     * protocol type of the message (eg: AMQP, MQTT)
     * </p>
     *
     * @param protocolType {@link ProtocolType}
     */
    public void setProtocolType(ProtocolType protocolType) {
        this.protocolType = protocolType;
    }

    /**
     * <p>
     * Protocol related metadata
     * </p>
     *
     * @return protocol metadata returned as a byte array
     */
    public byte[] getProtocolMetadata() {
        return protocolMetadata;
    }

    /**
     * <p>
     * Protocol related metadata can be encoded into a byte stream and set to {@link AndesMessageMetadata} using
     * this method.
     * </p>
     * <p>
     * Andes core won't be processing this metadata field. But will be persisted if the andes metadata is
     * persisted. {@link AndesEncodingUtil} can be used to encode/decode the values
     * </p>
     *
     * @param protocolMetadata encoded protocol specific metadata
     */
    public void setProtocolMetadata(byte[] protocolMetadata) {
        this.protocolMetadata = protocolMetadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "AndesMessageMetadata{" +
                "messageID=" + messageID +
                ", messageContentLength=" + messageContentLength +
                ", expirationTime=" + expirationTime +
                ", arrivalTime=" + arrivalTime +
                ", isCompressed=" + isCompressed +
                ", protocolType=" + protocolType +
                ", destination='" + destination + '\'' +
                ", storageDestination='" + storageDestination + '\'' +
                ", isPersistent=" + isPersistent +
                '}';
    }
}
