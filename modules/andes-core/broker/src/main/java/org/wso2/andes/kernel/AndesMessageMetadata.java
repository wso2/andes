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
import org.wso2.andes.mqtt.MQTTMessageMetaData;
import org.wso2.andes.mqtt.MQTTMetaDataHandler;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.server.store.StorableMessageMetaData;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor.PendingJob;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

public class AndesMessageMetadata implements Comparable<AndesMessageMetadata>{


    long messageID;
    byte[] metadata;
    long expirationTime;
    boolean isTopic;
    long arrivalTime;

    /**
     *through which connection this message came into broker
     * or rejected to the broker
     */
    UUID channelId;

    /**
     * Destination (routing key) of message
     */
    private String destination;

    /**
     * Queue name in store in which message
     * should be saved
     */
    private String storageQueueName;

    private boolean isPersistent;
    private boolean reDelivered;
    Map<UUID, PendingJob> pendingJobsTracker;
    public QueueAddress queueAddress;
    private static Log log = LogFactory.getLog(AndesMessageMetadata.class);

    /**
     *Added for MQTT usage
     */
    private int messageContentLength;
    private int qosLevel;

    /**
     *slotID which this metadata belongs
     */
    private Slot slot;

    public AndesMessageMetadata(){}

    public AndesMessageMetadata(long messageID, byte[] metadata, boolean parse) {
		super();
		this.messageID = messageID;
		this.metadata = metadata;
		if(parse){
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
     * @return the level of qos it can be either 0,1 or 2
     */
    public int getQosLevel() {
        return qosLevel;
    }

    /**
     * The level of qos the message is published at
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

    public Map<UUID, PendingJob> getPendingJobsTracker() {
        return pendingJobsTracker;
    }

    public void setPendingJobsTracker(Map<UUID, PendingJob> pendingJobsTracker) {
        this.pendingJobsTracker = pendingJobsTracker;
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

    public void setTopic(boolean isTopic) {
        this.isTopic = isTopic;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getStorageQueueName() {
        return storageQueueName;
    }

    public void setStorageQueueName(String storageQueueName) {
        this.storageQueueName = storageQueueName;
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public void setPersistent(boolean persistent) {
        isPersistent = persistent;
    }

    public UUID getChannelId() {
        return channelId;
    }

    public void setChannelId(UUID channelId) {
        this.channelId = channelId;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    /**
     * Create a clone, with new message ID
     * @param messageId message id
     * @return returns AndesMessageMetadata
     */
    public  AndesMessageMetadata deepClone(long messageId){
    	AndesMessageMetadata clone = new AndesMessageMetadata();
    	clone.messageID = messageId;
    	clone.metadata = metadata;
        clone.channelId = channelId;
    	clone.expirationTime = expirationTime;
        clone.isTopic = isTopic;
        clone.destination = destination;
        clone.storageQueueName = storageQueueName;
        clone.isPersistent = isPersistent;
        clone.pendingJobsTracker = pendingJobsTracker; 
        clone.queueAddress = queueAddress;
        clone.slot = slot;
        clone.arrivalTime = arrivalTime;
        return clone;
    }


    /**
     * Update metadata of message. This will change AMQP bytes representing
     * metadata. Routing key and exchange name will be set to the
     * given values.
     * @param newDestination  new routing key to set
     * @param newExchangeName new exchange name to set
     */
    public void updateMetadata(String newDestination, String newExchangeName){
    	this.metadata = createNewMetadata(this.metadata, newDestination, newExchangeName);
        this.destination = newDestination;
        log.debug("updated andes message metadata id= " + messageID + " new destination = " + newDestination);
    }

    public void setRedelivered() {
        this.reDelivered = true;
    }

    public boolean getRedelivered() {
        return reDelivered;
    }

    public boolean isExpired() {
            if (expirationTime != 0L)
            {
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
        StorableMessageMetaData mdt = type.getFactory()
                .createMetaData(buf);
        //todo need to discuss on making the flow more generic
        if (type.equals(MessageMetaDataType.META_DATA_0_10) || type.equals(MessageMetaDataType.META_DATA_0_8)) {
            isPersistent = ((MessageMetaData) mdt).isPersistent();
            expirationTime = ((MessageMetaData) mdt).getMessageHeader().getExpiration();
            arrivalTime = ((MessageMetaData) mdt).getArrivalTime();
            destination = ((MessageMetaData) mdt).getMessagePublishInfo().getRoutingKey().toString();
            isTopic = ((MessageMetaData) mdt).getMessagePublishInfo().getExchange().equals(AMQPUtils.TOPIC_EXCHANGE_NAME);
            queueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE, AndesUtils.getGlobalQueueNameForDestinationQueue(destination));
        }
        //For MQTT Specific Types
        if (type.equals(MessageMetaDataType.META_DATA_MQTT)) {
            this.isTopic = ((MQTTMessageMetaData) mdt).isTopic();
            this.destination = ((MQTTMessageMetaData) mdt).getDestination();
            this.isPersistent = ((MQTTMessageMetaData) mdt).isPersistent();
            this.messageContentLength = ((MQTTMessageMetaData) mdt).getContentSize();
            this.qosLevel = ((MQTTMessageMetaData) mdt).getQosLevel();
        }

    }

    public Object getMessageHeader(String header) {
        ByteBuffer buf = ByteBuffer.wrap(metadata);
        buf.position(1);
        buf = buf.slice();
        MessageMetaDataType type = MessageMetaDataType.values()[metadata[0]];
        StorableMessageMetaData mdt = type.getFactory()
                .createMetaData(buf);
        return((MessageMetaData)mdt).getMessageHeader().getHeader(header);
    }

    private byte[] createNewMetadata(byte[] originalMetadata, String routingKey, String exchangeName){
		ByteBuffer buf = ByteBuffer.wrap(originalMetadata);
		buf.position(1);
		buf = buf.slice();
		MessageMetaDataType type = MessageMetaDataType.values()[originalMetadata[0]];
		StorableMessageMetaData original_mdt = type.getFactory()
				.createMetaData(buf);

        byte[] underlying;
        MetaDataHandler handler;
        //TODO need to impliment factory pattern here
        if(type.equals(MessageMetaDataType.META_DATA_MQTT)){
            handler = new MQTTMetaDataHandler();
        }else{
            handler = new AMQPMetaDataHandler();
        }

        underlying = handler.constructMetadata(routingKey, buf, original_mdt, exchangeName);
        //TODO uncomment this once the logic is confirmed - Hasitha to review the change
/*		ContentHeaderBody contentHeaderBody = ((MessageMetaData) original_mdt)
                .getContentHeaderBody();
		int contentChunkCount = ((MessageMetaData) original_mdt)
				.getContentChunkCount();
		long arrivalTime = ((MessageMetaData) original_mdt).getArrivalTime();

		// modify routing key to the binding name
		MessagePublishInfo messagePublishInfo = new CustomMessagePublishInfo(
				original_mdt);
		messagePublishInfo.setRoutingKey(new AMQShortString(routingKey));
        messagePublishInfo.setExchange(new AMQShortString(exchangeName));
		MessageMetaData modifiedMetaData = new MessageMetaData(
				messagePublishInfo, contentHeaderBody, contentChunkCount,
				arrivalTime);

		final int bodySize = 1 + modifiedMetaData.getStorableSize();
		byte[] underlying = new byte[bodySize];
		underlying[0] = (byte) modifiedMetaData.getType().ordinal();
		buf = java.nio.ByteBuffer.wrap(underlying);
		buf.position(1);
		buf = buf.slice();
		modifiedMetaData.writeToBuffer(0, buf);*/
		
		return underlying;
    }

    public int getMessageContentLength() {
        return messageContentLength;
    }

    public void setMessageContentLength(int messageContentLength) {
        this.messageContentLength = messageContentLength;
    }

    public Slot getSlot() {
        return slot;
    }

    public void setSlot(Slot slot) {
        this.slot = slot;
    }

    @Override
    public int compareTo(AndesMessageMetadata other) {
        if(this.getMessageID() == other.getMessageID()) {
            return 0;
        } else {
            return this.getMessageID() > other.getMessageID() ? 1 : -1;
        }
    }
}
