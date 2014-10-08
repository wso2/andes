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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.storemanager.MessageStoreManagerFactory;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.slot.thrift.MBThriftClient;
import org.wso2.andes.store.cassandra.CQLBasedMessageStoreImpl;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.MessageExpirationWorker;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.server.cassandra.TopicDeliveryWorker;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.coordination.MessageIdGenerator;
import org.wso2.andes.server.cluster.coordination.TimeStampBasedMessageIdGenerator;
import org.wso2.andes.server.configuration.ClusterConfiguration;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.store.jdbc.h2.H2MemMessageStoreImpl;
import org.wso2.andes.subscription.SubscriptionStore;


/**
 * This class will handle all message related functions of WSO2 Message Broker
 */
public class MessagingEngine {

    /**
     * Logger for MessagingEngine
     */
    private static final Logger log;

    /**
     * Static instance of MessagingEngine
     */
    private static MessagingEngine messagingEngine;

    /**
     * Cluster wide unique message id generator
     */
    private MessageIdGenerator messageIdGenerator;

    // todo: remove referring to this from MessagingEngine. Use within MessageStoreManager
    // implmentations
    private MessageStore durableMessageStore;

    // todo: remove referring to this from MessagingEngine. Use within MessageStoreManager
    // implmentations
    private MessageStore inMemoryMessageStore;

    /**
     * reference to subscription store
     */
    private SubscriptionStore subscriptionStore;

    /**
     * Cluster related configurations
     */
    private ClusterConfiguration config;

    /**
     * Manages how the message content is persisted. Eg in async mode or stored in memory etc
     * Underlying implementation of this interface handles the logic
     */
    private MessageStoreManager messageStoreManager;

    // private constructor for singleton pattern
    private MessagingEngine() {
    }

    static {
        log = Logger.getLogger(MessagingEngine.class);
        messagingEngine = new MessagingEngine();
    }

    /**
     * MessagingEngine is used for executing operations related to messages. (Storing,
     * retrieving etc) This works as an API for different transports implemented by MB
     * @return MessagingEngine
     */
    public static MessagingEngine getInstance() {
        return messagingEngine;
    }

    /**
     * Initialises the MessagingEngine with a given durableMessageStore. Message retrieval and
     * storing strategy will be set accoridng to the configurations by calling this.
     * @param messageStore MessageStore
     * @throws AndesException
     */
    public void initialise(MessageStore messageStore) throws AndesException {

        config = ClusterResourceHolder.getInstance().getClusterConfiguration();
        configureMessageIDGenerator();

        try {
            messageStoreManager = MessageStoreManagerFactory.create(messageStore);

            // todo: These message store references need to be removed. Message stores need to be
            // accessed via MessageStoreManager
            durableMessageStore = messageStore;
            subscriptionStore = AndesContext.getInstance().getSubscriptionStore();

            // in memory message store
            inMemoryMessageStore = new H2MemMessageStoreImpl();
            inMemoryMessageStore.initializeMessageStore(null);

        } catch (Exception e) {
            throw new AndesException("Cannot initialize message store", e);
        }
    }

    public ClusterConfiguration getConfig() {
        return config;
    }

    /**
     * Message content is stored in database as chunks. Call this method for all the message
     * content received for a given message before calling messageReceived method with metadata
     * @param part AndesMessagePart
     */
    public void messageContentReceived(AndesMessagePart part) {
        try {
            messageStoreManager.storeMessagePart(part);
        } catch (AndesException e) {
            log.error("Error occurred while storing message content. message id: " +
                      part.getMessageID(), e);
        }
    }

    public AndesMessagePart getMessageContentChunk(long messageID, int offsetInMessage) throws AndesException{
        return durableMessageStore.getContent(messageID, offsetInMessage);
    }

    /**
     * Once all the message content is stored through messageContentReceived method call this
     * method with metadata
     * @param message AndesMessageMetadata
     * @throws AndesException
     */
    public void messageReceived(AndesMessageMetadata message)
            throws AndesException {
        try {

            if (message.getExpirationTime() > 0l) {
                //store message in MESSAGES_FOR_EXPIRY_COLUMN_FAMILY Queue
                // todo: MessageStoreManager needs to replace the deprecated method
                durableMessageStore.addMessageToExpiryQueue(message.getMessageID(),
                                                            message.getExpirationTime(),
                                                            message.isTopic(),
                                                            message.getDestination());
            }

            if (message.isTopic) {
                List<AndesSubscription> subscriptionList = subscriptionStore
                        .getActiveClusterSubscribersForDestination(message.getDestination(), true);
                if (subscriptionList.size() == 0) {
                    log.info("Message routing key: " + message
                            .getDestination() + " No routes in cluster. Ignoring Message");
                    List<Long> messageIdList = new ArrayList<Long>();
                    messageIdList.add(message.getMessageID());
                    durableMessageStore.deleteMessageParts(messageIdList);
                }

                Set<String> targetAndesNodeSet;
                boolean originalMessageUsed = false;
                boolean hasNonDurableSubscriptions = false;
                for (AndesSubscription subscriberQueue : subscriptionList) {
                    if (subscriberQueue.isDurable()) {
                        // If message is durable, we clone the message (if there is more than one
                        // subscription) and send them to a queue created for with the name of the
                        // Subscription ID by writing it to the global queue.
                        message.setDestination(subscriberQueue.getTargetQueue());
                        AndesMessageMetadata clone;
                        if (!originalMessageUsed) {
                            originalMessageUsed = true;
                            clone = message;
                        } else {
                            clone = cloneAndesMessageMetadataAndContent(message);
                        }
                        //We must update the routing key in metadata as well
                        clone.updateMetadata(subscriberQueue.getTargetQueue());

                        messageStoreManager.storeMetadata(message);
                    } else {
                        hasNonDurableSubscriptions = true;
                    }
                }

                if (hasNonDurableSubscriptions) {
                    // If there are non durable subscriptions, we write it to each node where
                    // there is a subscription. Here we collect all nodes to which we need to
                    // write to.
                    targetAndesNodeSet = subscriptionStore
                            .getNodeQueuesHavingSubscriptionsForTopic(message.getDestination());
                    for (String hostId : targetAndesNodeSet) {
                        AndesMessageMetadata clone;
                        if (!originalMessageUsed) {
                            originalMessageUsed = true;
                            clone = message;
                        } else {
                            clone = message.deepClone(messageIdGenerator.getNextId());
                        }
                        messageStoreManager.storeMetadata(clone);
                    }
                }
            } else {
                messageStoreManager.storeMetadata(message);
            }
        } catch (Exception e) {
            throw new AndesException("Error in storing the message to the store", e);
        }
    }

    public AndesMessageMetadata getMessageMetaData(long messageID) throws AndesException {
        return durableMessageStore.getMetaData(messageID);
    }

    public void ackReceived(AndesAckData ackData) throws AndesException {
        messageStoreManager.ackReceived(ackData);
    }

    //todo: hasitha - to implement
    public void messageRejected() throws AndesException{

    }

    /**
     * Move the messages meta data in the given message to the Dead Letter Channel and
     * remove those meta data from the original queue.
     *
     * @param messageId            The message Id to be removed
     * @param destinationQueueName The original destination queue of the message
     * @throws AndesException
     */
    public void moveMessageToDeadLetterChannel(long messageId, String destinationQueueName)
            throws AndesException {
        String deadLetterQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString
                (destinationQueueName, AndesConstants.DEAD_LETTER_QUEUE_NAME);

        messageStoreManager.moveMetaDataToQueue(messageId, destinationQueueName, deadLetterQueueName);
    }

    /**
     * remove messages of the queue matching to some destination queue
     *
     * @param destinationQueue destination queue name to match
     * @return number of messages removed
     * @throws AndesException
     */
    //TODO: hasitha - reimplement 1. get all message ids  2.remove all in one go 3. shedule to remove content 4. make counter delete
    public int removeAllMessagesOfQueue(String destinationQueue) throws AndesException {
        long lastProcessedMessageID = 0;
        int messageCount = 0;
        List<AndesMessageMetadata> messageList = durableMessageStore
                .getNextNMessageMetadataFromQueue(destinationQueue, lastProcessedMessageID, 500);
        List<AndesRemovableMetadata> messageMetaDataList = new ArrayList<AndesRemovableMetadata>();
        List<Long> messageIdList = new ArrayList<Long>();
        while (messageList.size() != 0) {

            // update metadata lists.
            for (AndesMessageMetadata metadata : messageList) {
                messageMetaDataList.add(new AndesRemovableMetadata(metadata.getMessageID(),
                                                                   metadata.getDestination()));
                messageIdList.add(metadata.getMessageID());
            }

            messageCount += messageIdList.size();
            lastProcessedMessageID = messageIdList.get(messageIdList.size() - 1);

            //remove metadata
            durableMessageStore
                    .deleteMessageMetadataFromQueue(destinationQueue, messageMetaDataList);
            //remove content
            durableMessageStore.deleteMessageParts(messageIdList);

            messageMetaDataList.clear();
            messageIdList.clear();
            messageList = durableMessageStore
                    .getNextNMessageMetadataFromQueue(destinationQueue, lastProcessedMessageID,
                                                      500);
        }
        return messageCount;
    }

    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel) throws AndesException {
        messageStoreManager.deleteMessages(messagesToRemove,moveToDeadLetterChannel);
    }

    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException{
        return messageStoreManager.getMessagePart(messageId, offsetValue);
    }

    public int getMessageCountOfQueue(String queueName) throws AndesException {
        return (int) AndesContext.getInstance().getAndesContextStore().getMessageCountForQueue(
                queueName);
    }

    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId, long lastMsgID) throws AndesException{
        return messageStoreManager.getMetaDataList(queueName, firstMsgId, lastMsgID);
    }

    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId, int count) throws AndesException {
        return messageStoreManager.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
    }

    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        return messageStoreManager.getExpiredMessages(limit);
    }

    /**
     * Store a message in a different Queue without altering the meta data.
     *
     * @param messageId        The message Id to move
     * @param currentQueueName The current destination of the message
     * @param targetQueueName  The target destination Queue name
     * @throws AndesException
     */
    public void moveMetaDataToQueue(long messageId, String currentQueueName,
                                    String targetQueueName) throws AndesException{
        messageStoreManager.moveMetaDataToQueue(messageId, currentQueueName, targetQueueName);
    }

    /**
     * Update the meta data for the given message with the given information in the AndesMetaData. Update destination
     * and meta data bytes.
     *
     * @param currentQueueName The queue the Meta Data currently in
     * @param metadataList     The updated meta data list.
     * @throws AndesException
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException{
        messageStoreManager.updateMetaDataInformation(currentQueueName, metadataList);
    }

    /**
     * remove in-memory messages tracked for this queue
     *
     * @param destinationQueueName name of queue messages should be removed
     * @throws AndesException
     */
    public void removeInMemoryMessagesAccumulated(String destinationQueueName)
            throws AndesException {
        //remove in-memory messages accumulated due to sudden subscription closing
        QueueDeliveryWorker queueDeliveryWorker = ClusterResourceHolder.getInstance()
                                                                       .getQueueDeliveryWorker();
        if (queueDeliveryWorker != null) {
            queueDeliveryWorker.clearMessagesAccumilatedDueToInactiveSubscriptionsForQueue(
                    destinationQueueName);
        }
        //remove sent but not acked messages
        OnflightMessageTracker.getInstance()
                              .getSentButNotAckedMessagesOfQueue(destinationQueueName);
    }

    public long generateNewMessageId() {
        return messageIdGenerator.getNextId();
    }

    private AndesMessageMetadata cloneAndesMessageMetadataAndContent(AndesMessageMetadata message
    ) throws AndesException {
        long newMessageId = messageIdGenerator.getNextId();
        AndesMessageMetadata clone = message.deepClone(newMessageId);

        //duplicate message content
        ((CQLBasedMessageStoreImpl) durableMessageStore)
                .duplicateMessageContent(message.getMessageID(), newMessageId);

        return clone;

    }

    public static String getMyNodeQueueName() {
        ClusterManager clusterManager = ClusterResourceHolder.getInstance().getClusterManager();
        return AndesConstants.NODE_QUEUE_NAME_PREFIX + clusterManager.getMyNodeID();

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
                log.error(
                        "Error while loading Message id generator implementation : " +
                        idGeneratorImpl +
                        " adding TimeStamp based implementation as the default", e);
                messageIdGenerator = new TimeStampBasedMessageIdGenerator();
            }
        } else {
            messageIdGenerator = new TimeStampBasedMessageIdGenerator();
        }
    }

    /**
     * start message delivery. Start threads. If not created create.
     *
     * @throws Exception
     */
    public void startMessageDelivery() throws Exception {


//        log.info("Starting queue message publisher");
//        QueueDeliveryWorker qdw =
//                ClusterResourceHolder.getInstance().getQueueDeliveryWorker();
//        if(qdw == null) {
//            boolean isInMemoryMode = ClusterResourceHolder.getInstance()
// .getClusterConfiguration().isInMemoryMode();
//            int queueWorkerInterval = ClusterResourceHolder.getInstance()
// .getClusterConfiguration().getQueueWorkerInterval();
////            QueueDeliveryWorker queueDeliveryWorker = new QueueDeliveryWorker
// (queueWorkerInterval, isInMemoryMode);
////            ClusterResourceHolder.getInstance().setQueueDeliveryWorker(queueDeliveryWorker);
//        }  else {
//            if (!qdw.isWorking()) {
//                qdw.setWorking();
//            }
//        }
//
//        log.info("Starting topic message publisher");
//        TopicDeliveryWorker tdw =
//                ClusterResourceHolder.getInstance().getTopicDeliveryWorker();
//        if(tdw == null) {
//            TopicDeliveryWorker topicDeliveryWorker = new TopicDeliveryWorker();
//            ClusterResourceHolder.getInstance().setTopicDeliveryWorker(topicDeliveryWorker);
//        }  else {
//            if (!tdw.isWorking()) {
//                tdw.setWorking();
//            }
//        }
//
//        log.info("Starting Disruptor writing messages to store");
    }

    /**
     * Stop message delivery threads
     */
    public void stopMessageDelivery() {

        log.info("Stopping queue message publisher");
        QueueDeliveryWorker qdw = ClusterResourceHolder.getInstance().getQueueDeliveryWorker();
        if (qdw != null && qdw.isWorking()) {
            qdw.stopFlusher();
        }

        log.info("Stopping topic message publisher");
        TopicDeliveryWorker tdw =
                ClusterResourceHolder.getInstance().getTopicDeliveryWorker();
        if (tdw != null && tdw.isWorking()) {
            tdw.stopWorking();
        }


        //stop all slotDeliveryWorkers
        SlotDeliveryWorkerManager.getInstance().stopSlotDeliveryWorkers();
        //stop thrift reconnecting thread if started
        if(MBThriftClient.isReconnectingStarted()){
            MBThriftClient.setReconnectingFlag(false);
        }
        log.info("Stopping Disruptor writing messages to store");

    }

    public void close() {

        stopMessageDelivery();
        //todo: hasitha - we need to wait all jobs are finished, all executors have no future tasks
        stopMessageExpirationWorker();
        durableMessageStore.close();
        inMemoryMessageStore.close();

    }

    /**
     * Start Checking for Expired Messages (JMS Expiration)
     */
    public void startMessageExpirationWorker() {
        log.info("Starting Message Expiration Checker");

        MessageExpirationWorker mew =
                ClusterResourceHolder.getInstance().getMessageExpirationWorker();

        if (mew == null) {

            MessageExpirationWorker messageExpirationWorker = new MessageExpirationWorker();
            ClusterResourceHolder.getInstance().setMessageExpirationWorker(messageExpirationWorker);

        } else {
            if (!mew.isWorking()) {
                mew.setWorking();
            }
        }

        log.info("Message Expiration Checker has started.");
    }

    /**
     * Stop Checking for Expired Messages (JMS Expiration)
     */
    public void stopMessageExpirationWorker() {

        log.info("Stopping Message Expiration Checker");

        MessageExpirationWorker mew = ClusterResourceHolder.getInstance()
                                                           .getMessageExpirationWorker();
        if (mew != null && mew.isWorking()) {
            mew.stopWorking();
        }
    }

}
