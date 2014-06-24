/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.wso2.andes.messageStore.CQLBasedMessageStoreImpl;
import org.wso2.andes.messageStore.InMemoryMessageStore;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.server.cassandra.TopicDeliveryWorker;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.coordination.MessageIdGenerator;
import org.wso2.andes.server.cluster.coordination.TimeStampBasedMessageIdGenerator;
import org.wso2.andes.server.configuration.ClusterConfiguration;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.ClusterwideSubscriptionChangeNotifier;
import org.wso2.andes.subscription.OrphanedMessagesDueToUnsubscriptionHandler;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor;


/**
 * This class will handle all message related functions of WSO2 Message Broker
 */
public class MessagingEngine {
    private static final Logger log = Logger.getLogger(MessagingEngine.class);
    private static MessagingEngine messagingEngine = null;
    private MessageStore cassandraBasedMessageStore;
    private InMemoryMessageStore inMemoryMessageStore;
    private DisruptorBasedExecutor disruptorBasedExecutor ;
    private SubscriptionStore subscriptionStore;
    private MessageIdGenerator messageIdGenerator;
    private ClusterConfiguration config;
    private final String ID_GENENRATOR = "idGenerator";


    public static synchronized MessagingEngine getInstance() {
        if(messagingEngine == null){
            messagingEngine = new MessagingEngine();
        }
        return messagingEngine;
    }

    private MessagingEngine() {
        cassandraBasedMessageStore = new CQLBasedMessageStoreImpl();
        inMemoryMessageStore = new InMemoryMessageStore();
        disruptorBasedExecutor = new DisruptorBasedExecutor(cassandraBasedMessageStore, null);
        config = ClusterResourceHolder.getInstance().getClusterConfiguration();
        configureMessageIDGenerator();
    }


    public MessageStore getCassandraBasedMessageStore() {
		return cassandraBasedMessageStore;
	}

    public InMemoryMessageStore getInMemoryMessageStore() {
        return inMemoryMessageStore;
    }

    public void initializeMessageStore(DurableStoreConnection connection) throws AndesException {
        cassandraBasedMessageStore.initializeMessageStore(connection);
        subscriptionStore = new SubscriptionStore(connection);
        //adding subscription listeners
        subscriptionStore.addSubscriptionListener(new OrphanedMessagesDueToUnsubscriptionHandler());
        subscriptionStore.addSubscriptionListener(new ClusterwideSubscriptionChangeNotifier());
    }

    public ClusterConfiguration getConfig() {
        return config;
    }

	public void setCassandraBasedMessageStore(
			MessageStore cassandraBasedMessageStore) {
		this.cassandraBasedMessageStore = cassandraBasedMessageStore;
	}

	public DisruptorBasedExecutor getDisruptorBasedExecutor() {
		return disruptorBasedExecutor;
	}


	public void messageContentReceived(AndesMessagePart part) {
        disruptorBasedExecutor.messagePartReceived(part);
    }

    public void messageContentChunkReceived(long _messageId, int offsetInMessage, ByteBuffer src) {
        //write to disruptor
        AndesMessagePart part = new AndesMessagePart();
        src = src.slice();
        final byte[] chunkData = new byte[src.limit()];

        src.duplicate().get(chunkData);

        part.setData(chunkData);
        part.setMessageID(_messageId);
        part.setOffSet(offsetInMessage);
        part.setDataLength(chunkData.length);

        messageContentReceived(part);
    }

    public void messageReceived(AndesMessageMetadata message, long channelID) throws AndesException {
        try {
            if (message.isTopic) {
                List<Subscrption> subscriptionList = subscriptionStore.getClusterSubscribersForTopic(message.getDestination());
                if (subscriptionList.size() == 0) {
                    log.info("Message routing key: " + message.getDestination() + " No routes in cluster.");
                    List<Long> messageIdList =  new ArrayList<Long>();
                    messageIdList.add(message.getMessageID());
                    cassandraBasedMessageStore.deleteMessageParts(messageIdList);
                }

                Set<String> targetAndesNodeSet = new HashSet<String>();
                boolean originalMessageUsed = false;
                boolean hasNonDurableSubscriptions = false; 
                for (Subscrption subscriberQueue : subscriptionList) {
                    if (subscriberQueue.isDurable()) {
                    	/**
                    	 * If message is durable, we clone the message (if there is more than one subscription) and send them to a queue created for with the name of the
                    	 * Subscription ID by writing it to the global queue.
                    	 */
                    	message.setDestination(subscriberQueue.getTargetQueue());
                    	AndesMessageMetadata clone = null; 
                    	if(!originalMessageUsed){
                    		originalMessageUsed = true;
                    		clone = message; 
                    	}else{
                    		clone = cloneAndesMessageMetadataAndContent(message);
                    	}
                		//We must update the routing key in metadata as well
                		clone.updateMetadata(subscriberQueue.getTargetQueue());

                		String globalQueueName = AndesUtils.getGlobalQueueNameForDestinationQueue(clone.getDestination());
                        QueueAddress globalQueueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE, globalQueueName);
                    	sendMessageToMessageStore(globalQueueAddress, clone, channelID);
                    } else {
                    	hasNonDurableSubscriptions = true; 
                    }
                }
                
                if(hasNonDurableSubscriptions){
                	/**
                	 * If there are non durable subscriptions, we write it to each node where there is a subscription. Here we collect all nodes to which we need to write to.
                	 */
                    targetAndesNodeSet  = subscriptionStore.getNodeQueuesHavingSubscriptionsForTopic(message.getDestination());
                    for (String hostId : targetAndesNodeSet) {
                    	AndesMessageMetadata clone = null; 
                    	if(!originalMessageUsed){
                    		originalMessageUsed = true;
                    		clone = message; 
                    	}else{
                    		clone = message.deepClone(messageIdGenerator.getNextId());
                    	}
                        QueueAddress globalQueueAddress = new QueueAddress(QueueAddress.QueueType.TOPIC_NODE_QUEUE, hostId);
                    	sendMessageToMessageStore(globalQueueAddress, clone, channelID);
                    }
                }
            } else {
            	//If Queue, we write the message to Global Queue
                String globalQueueName = AndesUtils.getGlobalQueueNameForDestinationQueue(message.getDestination());
                QueueAddress globalQueueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE, globalQueueName);
            	sendMessageToMessageStore(globalQueueAddress, message, channelID);
            }
        } catch (Exception e) {
            throw new AndesException("Error in storing the message to the store", e);
        }
    }

    public void ackReceived(AndesAckData ack) {
    	disruptorBasedExecutor.ackReceived(ack);
    }

    public void messageReturned(List<AndesAckData> ackList) {

    }

    /**
     * remove messages of the queue matching to some destination queue
     * @param queueAddress  queue address
     * @param destinationQueueNameToMatch  destination queue name to match
     * @return  number of messages removed
     * @throws AndesException
     */
    public int removeMessagesOfQueue(QueueAddress queueAddress, String destinationQueueNameToMatch) throws AndesException {
        long lastProcessedMessageID = 0;
        int messageCount = 0;
        List<AndesMessageMetadata> messageList = cassandraBasedMessageStore.getNextNMessageMetadataFromQueue(queueAddress, lastProcessedMessageID, 500);
        List<AndesRemovableMetadata> messageMetaDataList = new ArrayList<AndesRemovableMetadata>();
        List<Long> messageIdList = new ArrayList<Long>();
        while (messageList.size() != 0) {
            Iterator<AndesMessageMetadata> metadataIterator = messageList.iterator();
            while (metadataIterator.hasNext()) {
                AndesMessageMetadata metadata = metadataIterator.next();
                String destinationQueue = metadata.getDestination();
                if (destinationQueueNameToMatch != null) {
                    if (destinationQueue.equals(destinationQueueNameToMatch)) {
                        messageMetaDataList.add(new AndesRemovableMetadata(metadata.getMessageID(), metadata.getDestination()));
                        messageIdList.add(metadata.getMessageID());
                        messageCount++;
                    } else {
                        metadataIterator.remove();
                    }
                } else {
                    messageCount++;
                }

                lastProcessedMessageID = metadata.getMessageID();

            }
            //remove metadata
            cassandraBasedMessageStore.deleteMessageMetadataFromQueue(queueAddress, messageMetaDataList);
            //remove content
            cassandraBasedMessageStore.deleteMessageParts(messageIdList);
            messageMetaDataList.clear();
            messageList = cassandraBasedMessageStore.getNextNMessageMetadataFromQueue(queueAddress, lastProcessedMessageID, 500);
        }
        return messageCount;
    }

    /**
     * remove in-memory messages tracked for this queue
     *
     * @param destinationQueueName name of queue messages should be removed
     * @throws AndesException
     */
    public void removeInMemoryMessagesAccumulated(String destinationQueueName) throws AndesException {
        //remove in-memory messages accumulated due to sudden subscription closing
        QueueDeliveryWorker queueDeliveryWorker = ClusterResourceHolder.getInstance().getQueueDeliveryWorker();
        if (queueDeliveryWorker != null) {
            queueDeliveryWorker.clearMessagesAccumilatedDueToInactiveSubscriptionsForQueue(destinationQueueName);
        }
        //remove sent but not acked messages
        OnflightMessageTracker.getInstance().getSentButNotAckedMessagesOfQueue(destinationQueueName);
    }

    public SubscriptionStore getSubscriptionStore() {
        return subscriptionStore;
    }

    public void setSubscriptionStore(SubscriptionStore subscriptionStore) {
        this.subscriptionStore = subscriptionStore;
    }

    public long generateNewMessageId() {
        return messageIdGenerator.getNextId();
    }
    
    private void sendMessageToMessageStore(QueueAddress queueAdr, AndesMessageMetadata message, long channelID)throws AndesException{
    	if(message.isPersistent()){
            disruptorBasedExecutor.messageCompleted(queueAdr, message, channelID);
    	}else{
    		//TODO stop creating this list
    		List<AndesMessageMetadata> list = new ArrayList<AndesMessageMetadata>(1);
    		list.add(message);
    		inMemoryMessageStore.addMessageMetaData(queueAdr ,list);
    	}
    }

    private AndesMessageMetadata cloneAndesMessageMetadataAndContent(AndesMessageMetadata message) throws AndesException {
        long newMessageId = messageIdGenerator.getNextId();
    	AndesMessageMetadata clone = message.deepClone(newMessageId);

        //duplicate message content
        ((CQLBasedMessageStoreImpl) cassandraBasedMessageStore).duplicateMessageContent(message.getMessageID(),newMessageId);

        return clone;

    }
    
    public static String getMyNodeQueueName(){
        ClusterManager clusterManager = ClusterResourceHolder.getInstance().getClusterManager();
        String nodeQueueName = AndesConstants.NODE_QUEUE_NAME_PREFIX + clusterManager.getMyNodeID() ;
        return nodeQueueName;
    }

    private void configureMessageIDGenerator() {
        // configure message ID generator
        String idGeneratorImpl = config.getMessageIdGeneratorClass();
        if (idGeneratorImpl != null && !"".equals(idGeneratorImpl)) {
            try {
                Class clz = Class.forName(idGeneratorImpl);

                Object o = clz.newInstance();
                messageIdGenerator = (MessageIdGenerator) o;
            } catch (Exception e) {
                log.error("Error while loading Message id generator implementation : " + idGeneratorImpl +
                        " adding TimeStamp based implementation as the default", e);
                messageIdGenerator = new TimeStampBasedMessageIdGenerator();
            }
        } else {
            messageIdGenerator = new TimeStampBasedMessageIdGenerator();
        }
    }

    /**
     * start message delivery. Start threads. If not created create.
     * @throws Exception
     */
    public void startMessageDelivey() throws Exception{

        log.info("Starting queue message publisher");
        QueueDeliveryWorker qdw =
                ClusterResourceHolder.getInstance().getQueueDeliveryWorker();
        if(qdw == null) {
            boolean isInMemoryMode = ClusterResourceHolder.getInstance().getClusterConfiguration().isInMemoryMode();
            int queueWorkerInterval = ClusterResourceHolder.getInstance().getClusterConfiguration().getQueueWorkerInterval();
            QueueDeliveryWorker queueDeliveryWorker = new QueueDeliveryWorker(queueWorkerInterval, isInMemoryMode);
            ClusterResourceHolder.getInstance().setQueueDeliveryWorker(queueDeliveryWorker);
        }  else {
            if (!qdw.isWorking()) {
                qdw.setWorking();
            }
        }

        log.info("Starting topic message publisher");
        TopicDeliveryWorker tdw =
                ClusterResourceHolder.getInstance().getTopicDeliveryWorker();
        if(tdw == null) {
            TopicDeliveryWorker topicDeliveryWorker = new TopicDeliveryWorker();
            ClusterResourceHolder.getInstance().setTopicDeliveryWorker(topicDeliveryWorker);
        }  else {
            if (!tdw.isWorking()) {
                tdw.setWorking();
            }
        }

        log.info("Starting Disruptor writing messages to store");
    }

    /**
     * Stop message delivery threads
     */
    public void stopMessageDelivery() {

        log.info("Stopping queue message publisher");
        QueueDeliveryWorker qdw = ClusterResourceHolder.getInstance().getQueueDeliveryWorker();
        if(qdw != null && qdw.isWorking()) {
            qdw.stopFlusher();
        }

        log.info("Stopping topic message publisher");
        TopicDeliveryWorker tdw =
                ClusterResourceHolder.getInstance().getTopicDeliveryWorker();
        if (tdw != null && tdw.isWorking()) {
            tdw.stopWorking();
        }

        log.info("Stopping Disruptor writing messages to store");

    }

    public void close() {

        stopMessageDelivery();

    }
    
}
