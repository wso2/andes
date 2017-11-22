/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
import org.wso2.andes.kernel.disruptor.inbound.InboundMessageRecoveryEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundBindingEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundChannelFlowEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundDeleteDLCMessagesEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundDeleteMessagesEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.disruptor.inbound.InboundExchangeEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundKernelOpsEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundMessageRejectEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundQueueEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundTransactionEvent;
import org.wso2.andes.kernel.disruptor.inbound.PubAckHandler;
import org.wso2.andes.kernel.dtx.DistributedTransaction;
import org.wso2.andes.kernel.dtx.DtxRegistry;
import org.wso2.andes.kernel.slot.SlotMessageCounter;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.tools.utils.MessageTracer;
import org.wso2.carbon.metrics.manager.Counter;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.Meter;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.transaction.xa.Xid;

import static org.wso2.andes.configuration.enums.AndesConfiguration.PERFORMANCE_TUNING_PURGED_COUNT_TIMEOUT;

/**
 * API for all the tasks done by Andes.
 */
public class Andes {

    private static Log log = LogFactory.getLog(Andes.class);

    private static Andes instance = new Andes();

    /**
     * Max purge timeout to return the value of purge message count.
     */
    private final int PURGE_TIMEOUT_SECONDS;

    /**
     * Use to manage channel according to flow control rules.
     */
    private final FlowControlManager flowControlManager;

    /**
     * Event manager handling incoming events.
     * Eg: open channel, publish message, process acknowledgments
     */
    private InboundEventManager inboundEventManager;

    /**
     * Andes context related information manager. Exchanges, Queues and Bindings.
     */
    private AndesContextInformationManager contextInformationManager;
    /**
     * handle all message related functions.
     */
    private MessagingEngine messagingEngine;

    /**
     * Manages all subscription related events.
     */
    private AndesSubscriptionManager subscriptionManager;

    /**
     * Scheduler for periodically trigger Slot Deletion Safe Zone
     * update events.
     */
    private final ScheduledExecutorService safeZoneUpdateScheduler = Executors.newScheduledThreadPool(1);

    /**
     * Interval in milliseconds above update call should trigger
     */
    private static final int safeZoneUpdateTriggerInterval = AndesConfigurationManager.readValue(AndesConfiguration
            .PERFORMANCE_TUNING_MAX_SLOT_SUBMIT_DELAY);

    /**
     * Maximum batch size for a transaction. Limit is set for content size of the batch.
     * Exceeding this limit will lead to a failure in the subsequent commit request.
     */
    private final int maxTxBatchSize;

    /**
     * Transaction events such as commit, rollback and close are blocking calls waiting on
     * {@link com.google.common.util.concurrent.SettableFuture} objects. This is the maximum
     * wait time for the completion of those events
     */
    private final long TX_EVENT_TIMEOUT;

    /**
     * Registry object used to store distributed transaction related information
     */
    private DtxRegistry dtxRegistry;

    /**
     * Keep the list of dtx enabled channels
     */
    private List<UUID> dtxChannelList;

    /**
     * Maximum number of parallel dtx enabled channel count
     */
    private final int maxParallelDtxConnections;

    /**
     * Instance of AndesAPI returned.
     *
     * @return AndesAPI
     */
    public static Andes getInstance() {
        return instance;
    }

    /**
     * Singleton class. Hence private constructor.
     */
    private Andes() {
        PURGE_TIMEOUT_SECONDS = AndesConfigurationManager.readValue(PERFORMANCE_TUNING_PURGED_COUNT_TIMEOUT);
        this.flowControlManager = new FlowControlManager();
        maxTxBatchSize = (Integer) AndesConfigurationManager.
                readValue(AndesConfiguration.MAX_TRANSACTION_BATCH_SIZE) * 1024;
        TX_EVENT_TIMEOUT = AndesConfigurationManager.readValue(AndesConfiguration.MAX_TRANSACTION_WAIT_TIMEOUT);
        maxParallelDtxConnections = AndesConfigurationManager.readValue(AndesConfiguration.MAX_PARALLEL_DISTRIBUTED_TRANSACTION_COUNT);

        dtxChannelList = new ArrayList<>();
    }


    /**
     * Recover messages for the subscriber. Re-schedule sent but un-ackenowledged
     * messages back
     *
     * @param channelID         ID of the channel recover request is received
     * @param recoverOKCallback Callback to send recover-ok
     * @throws AndesException in case of publishing event to the disruptor.
     */
    public void recoverMessagesOfChannel(UUID channelID, DisruptorEventCallback recoverOKCallback)
            throws AndesException {

        InboundMessageRecoveryEvent recoveryEvent = new InboundMessageRecoveryEvent(channelID, recoverOKCallback);
        inboundEventManager.publishMessageRecoveryEvent(recoveryEvent);
    }

    /**
     * Initialise is package specific. We don't need outsiders initialising the API
     */
    void initialise(MessagingEngine messagingEngine,
                    InboundEventManager inboundEventManager,
                    AndesContextInformationManager contextInformationManager,
                    AndesSubscriptionManager subscriptionManager,
                    DtxRegistry dtxRegistry ) {

        this.contextInformationManager = contextInformationManager;
        this.messagingEngine = messagingEngine;
        this.subscriptionManager = subscriptionManager;
        this.inboundEventManager = inboundEventManager;
        this.dtxRegistry = dtxRegistry;

        log.info("Andes API initialised.");
    }

    /**
     * Start the safe zone calculation worker. The safe zone is used to decide if a slot can be safely deleted,
     * assuming all messages in the slot range has been delivered.
     */
    public void startSafeZoneUpdateWorkers() {
        SafeZoneUpdateEventTriggeringTask safeZoneUpdateTask = new SafeZoneUpdateEventTriggeringTask(
                inboundEventManager);

        log.info("Starting Safe Zone Calculator for slots.");
        safeZoneUpdateScheduler
                .scheduleAtFixedRate(safeZoneUpdateTask, 5, safeZoneUpdateTriggerInterval, TimeUnit.MILLISECONDS);

        SlotMessageCounter.getInstance().scheduleSubmitSlotToCoordinatorTimer();

    }

    /**
     * When a message is received from a transport it should be converted to an AndesMessage and handed over to Andes
     * for delivery through this method.
     *
     * @param message       AndesMessage
     * @param andesChannel  AndesChannel
     * @param pubAckHandler PubAckHandler
     */
    public void messageReceived(AndesMessage message, AndesChannel andesChannel, PubAckHandler pubAckHandler) {

        //Tracing message
        MessageTracer.trace(message, MessageTracer.REACHED_ANDES_CORE);

        inboundEventManager.messageReceived(message, andesChannel, pubAckHandler);

        //Adding metrics meter for message rate
        Meter messageMeter = MetricManager.meter(MetricsConstants.MSG_RECEIVE_RATE, Level.INFO);
        messageMeter.mark();

        //Adding metrics counter for enqueue messages
        Counter counter = MetricManager.counter(MetricsConstants.ENQUEUE_MESSAGES, Level.INFO);
        counter.inc();
    }

    /**
     * Acknowledgement received from clients for sent messages should be notified to Andes using this method.
     *
     * @param ackData Acknowledgement information by protocol
     * @throws AndesException on an issue publishing ack event into disruptor
     */
    public void ackReceived(AndesAckData ackData) throws AndesException {
       //Adding metrics meter for ack rate
        Meter ackMeter = MetricManager.meter(MetricsConstants.ACK_RECEIVE_RATE, Level.INFO);
        ackMeter.mark();

        //Adding metrics counter for ack messages
        Counter counter = MetricManager.counter(MetricsConstants.ACK_MESSAGES, Level.INFO);
        counter.inc();

        inboundEventManager.ackReceived(ackData);
    }

    /**
     * Close the local subscription with reference to the input subscription event.
     *
     * @param subscriptionEvent disruptor event containing the subscription to close.
     * @throws AndesException
     */
    public void closeLocalSubscription(InboundSubscriptionEvent subscriptionEvent) throws AndesException {
        subscriptionEvent.prepareForCloseSubscription(subscriptionManager);
        inboundEventManager.publishStateEvent(subscriptionEvent);
        try {
            subscriptionEvent.waitForCompletion();
        } catch (SubscriptionAlreadyExistsException e) {
            log.error("Error occurred while closing subscription ", e);
        }
    }

    /**
     * When a local subscription is created notify Andes through this method. This need to be called first to receive
     * any messages from this local subscription
     *
     * @param subscriptionEvent InboundSubscriptionEvent
     * @throws SubscriptionAlreadyExistsException
     */
    public void openLocalSubscription(InboundSubscriptionEvent subscriptionEvent)
            throws AndesException {
        subscriptionEvent.prepareForNewSubscription(subscriptionManager);
        inboundEventManager.publishStateEvent(subscriptionEvent);
        subscriptionEvent.waitForCompletion();
    }

    /**
     * Un-subscribe subscription. This is usually performed on durable topic
     * subscriptions. Event is handled in tow steps
     * 1. Close the subscription
     * 2. Delete storage queue subscription is bound to
     *
     * @param subscriptionEvent un-subscribe event
     * @throws AndesException
     */
    public void unsubscribeLocalSubscription(InboundSubscriptionEvent subscriptionEvent) throws AndesException {
        closeLocalSubscription(subscriptionEvent);

        StorageQueue queue = AndesContext.getInstance().
                getStorageQueueRegistry().getStorageQueue(subscriptionEvent.getBoundStorageQueueName());

        InboundQueueEvent deleteQueueEvent = new InboundQueueEvent(queue.getName(),
                queue.isDurable(), queue.isShared(), queue.getQueueOwner(), queue.isExclusive());
        deleteQueue(deleteQueueEvent);
    }

    /**
     * Notify client connection is closed from protocol level.
     * State related to connection will be updated within Andes.
     */
    public void startMessageDelivery() {
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.prepareForStartMessageDelivery(messagingEngine);
        inboundEventManager.publishStateEvent(kernelOpsEvent);
    }

    /**
     * Stop message delivery.
     */
    public void stopMessageDelivery() {
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.prepareForStopMessageDelivery(messagingEngine);
        inboundEventManager.publishStateEvent(kernelOpsEvent);
    }

    /**
     * Shut down Andes.
     * NOTE: This is package specific. We don't need access outside from kernel for this task
     */
    public void shutDown() throws AndesException {
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.gracefulShutdown(messagingEngine, inboundEventManager, flowControlManager, dtxRegistry);
        kernelOpsEvent.waitForTaskCompletion();
    }

    /**
     * Start message expiration task. This will periodically delete any expired messages
     * NOTE: This is package specific. We don't need access outside from kernel for this task
     */
    void startMessageExpirationWorker() {
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.prepareForStartMessageExpirationWorker(messagingEngine);
        inboundEventManager.publishStateEvent(kernelOpsEvent);
    }

    /**
     * Stop message expiration task.
     * NOTE: This is package specific. We don't need outside kernel access for this task
     */
    void stopMessageExpirationWorker() {
        InboundKernelOpsEvent kernelOpsEvent = new InboundKernelOpsEvent();
        kernelOpsEvent.prepareForStopMessageExpirationWorker(messagingEngine);
        inboundEventManager.publishStateEvent(kernelOpsEvent);
    }

    /**
     * This is the andes-specific purge method and can be called from AMQPBridge,
     * MQTTBridge or UI MBeans (QueueManagementInformationMBean)
     * Remove messages of the queue matching to given destination queue ( h2 / mysql etc. ).
     *
     * @param queueEvent queue event related to purge
     *                   since we cannot guarantee that we caught all messages in delivery threads.)
     * @throws AndesException
     */
    public int purgeQueue(InboundQueueEvent queueEvent) throws AndesException {
        queueEvent.purgeQueue(contextInformationManager);
        inboundEventManager.publishStateEvent(queueEvent);
        try {
            return queueEvent.getPurgedCount(PURGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("Purge event timed out. Purge may have failed or may take longer than " + PURGE_TIMEOUT_SECONDS
                    + " seconds", e);
        }
        return -1;
    }

    /**
     * Schedule to delete messages from store. Optionally move to dead letter channel.
     *
     * @param messagesToRemove        List of messages to remove
     * @param moveToDeadLetterChannel if to move to DLC
     * @throws AndesException
     */
    public void deleteMessages(List<DeliverableAndesMetadata> messagesToRemove, boolean moveToDeadLetterChannel)
            throws AndesException {
        InboundDeleteMessagesEvent deleteMessagesEvent = new InboundDeleteMessagesEvent(messagesToRemove,
                moveToDeadLetterChannel);
        deleteMessagesEvent.prepareForDelete(messagingEngine);
        inboundEventManager.publishStateEvent(deleteMessagesEvent);
    }

    /**
     * Schedule to delete messages from store. Optionally move to dead letter channel. Here if the message
     * is still tracked in message delivery path, message states will be updated accordingly.
     *
     * @param messagesToRemove        Collection of messages to remove
     * @param moveToDeadLetterChannel if to move to DLc
     */
    public void deleteMessages(Collection<AndesMessageMetadata> messagesToRemove, boolean moveToDeadLetterChannel)
            throws AndesException {
        InboundDeleteMessagesEvent deleteMessagesEvent = new InboundDeleteMessagesEvent(messagesToRemove,
                moveToDeadLetterChannel);
        deleteMessagesEvent.prepareForDelete(messagingEngine);
        inboundEventManager.publishStateEvent(deleteMessagesEvent);
    }

    /**
     * Method to delete message from the dead letter channel.
     *
     * @param messagesToRemove List of messages to remove
     */
    public void deleteMessagesFromDLC(List<AndesMessageMetadata> messagesToRemove) throws AndesException {
        InboundDeleteDLCMessagesEvent deleteDLCMessagesEvent = new InboundDeleteDLCMessagesEvent(messagesToRemove);
        deleteDLCMessagesEvent.prepareForDelete(messagingEngine);
        inboundEventManager.publishStateEvent(deleteDLCMessagesEvent);
    }

    /**
     * Create queue in Andes kernel.
     *
     * @param queueEvent queue event to create
     * @throws AndesException
     */
    public void createQueue(InboundQueueEvent queueEvent) throws AndesException {
        queueEvent.prepareForCreateQueue(contextInformationManager);
        inboundEventManager.publishStateEvent(queueEvent);
        queueEvent.waitForCompletion();
    }

    /**
     * Delete the queue from broker. This will purge the queue and
     * delete cluster-wide
     *
     * @param queueEvent queue event for deleting queue
     * @throws AndesException
     */
    public void deleteQueue(InboundQueueEvent queueEvent) throws AndesException {
        queueEvent.prepareForDeleteQueue(contextInformationManager);
        inboundEventManager.publishStateEvent(queueEvent);
    }

    /**
     * Return the requested chunk of a message's content.
     *
     * @param messageID       Unique ID of the Message
     * @param offsetInMessage The offset of the required chunk in the Message content.
     * @return AndesMessagePart
     * @throws AndesException
     */
    public AndesMessagePart getMessageContentChunk(long messageID, int offsetInMessage) throws AndesException {
        return MessagingEngine.getInstance().getMessageContentChunk(messageID, offsetInMessage);
    }

    /**
     * Get a single metadata object.
     *
     * @param messageID id of the message
     * @return AndesMessageMetadata
     * @throws AndesException
     */
    public AndesMessageMetadata getMessageMetaData(long messageID) throws AndesException {
        return MessagingEngine.getInstance().getMessageMetaData(messageID);
    }

    /**
     * Handle message reject.
     *
     * @param messageId  Id of message that is rejected.
     * @param channelID Id of the connection channel reject is received
     * @param reQueue true if message should be re-queued to subscriber
     * @param isMessageBeyondLastRollback  true if the message was rejected after the last rollback event.
     * @throws AndesException on a message re-schedule issue
     */
    public void messageRejected(long messageId, UUID channelID, boolean reQueue,
                                boolean isMessageBeyondLastRollback) throws AndesException {
        InboundMessageRejectEvent messageRejectEvent = new InboundMessageRejectEvent(messageId, channelID, reQueue);
        messageRejectEvent.prepareToRejectMessage(isMessageBeyondLastRollback);
        inboundEventManager.publishStateEvent(messageRejectEvent);
    }

    /**
     * Update the meta data for the given message with the given information in the AndesMetaData. Update destination
     * and meta data bytes.
     *
     * @param currentQueueName The queue the Meta Data currently in
     * @param metadataList     The updated meta data list.
     * @throws AndesException
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {
        MessagingEngine.getInstance().updateMetaDataInformation(currentQueueName, metadataList);
    }

    /**
     * Move the messages meta data in the given message to the Dead Letter Channel.
     *
     * @param message              The message to be removed. This should have all tracking information
     * @param destinationQueueName The original destination queue of the message
     * @throws AndesException
     */
    public void moveMessageToDeadLetterChannel(DeliverableAndesMetadata message, String destinationQueueName)
            throws AndesException {
        MessagingEngine.getInstance().moveMessageToDeadLetterChannel(message, destinationQueueName);
    }

    /**
     * Get content chunk from store.
     *
     * @param messageId   id of the message
     * @param offsetValue chunk id
     * @return message content
     * @throws AndesException
     */
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        return MessagingEngine.getInstance().getContent(messageId, offsetValue);
    }

    /**
     * Get content chunks for a list of message ids from store.
     *
     * @param messageIdList list of messageIds
     * @return map of message id:content chunk list
     * @throws AndesException
     */
    public LongObjectHashMap<List<AndesMessagePart>> getContent(LongArrayList messageIdList) throws AndesException {
        return MessagingEngine.getInstance().getContent(messageIdList);
    }

    /**
     * Get a map of queue names and the message count in the database for each queue in the database.
     *
     * @param queueNames list of queue names of which the message count should be retrieved
     * @return Map of queue names and the message count for each queue
     */
    public Map<String, Integer> getMessageCountForAllQueues(List<String> queueNames) throws AndesException {
        return MessagingEngine.getInstance().getMessageCountForAllQueues(queueNames);
    }

    /**
     * Get message count for queue.
     *
     * @param queueName name of the queue
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCountOfQueue(String queueName) throws AndesException {
        return MessagingEngine.getInstance().getMessageCountOfQueue(queueName);
    }

    /**
     * Get message count in a dead letter channel queue.
     *
     * @param dlcQueueName name of the dlc queue
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCountInDLC(String dlcQueueName) throws AndesException {
        return MessagingEngine.getInstance().getMessageCountInDLC(dlcQueueName);
    }

    /**
     * Get message count in DLC for a specific queue.
     *
     * @param queueName    name of the queue
     * @param dlcQueueName name of the dlc queue
     * @return message count of the queue
     * @throws AndesException
     */
    public long getMessageCountInDLCForQueue(String queueName, String dlcQueueName) throws AndesException {
        return MessagingEngine.getInstance().getMessageCountInDLCForQueue(queueName, dlcQueueName);
    }

    /**
     * Get message metadata from queue starting from given id up a given
     * message count.
     *
     * @param queueName  name of the queue
     * @param firstMsgId id of the starting id
     * @param count      maximum num of messages to return
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId,
            int count) throws AndesException {
        return MessagingEngine.getInstance().getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
    }

    /**
     * Get message metadata in dlc for a queue for a given number of messages starting from a specified id.
     *
     * @param queueName    name of the queue
     * @param dlcQueueName name of the dead letter channel queue
     * @param firstMsgId   starting message id
     * @param count        maximum num of messages to return
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataInDLCForQueue(final String queueName,
            final String dlcQueueName, long firstMsgId, int count) throws AndesException {
        return MessagingEngine.getInstance()
                .getNextNMessageMetadataInDLCForQueue(queueName, dlcQueueName, firstMsgId, count);
    }

    /**
     * Get message metadata in dlc for a given number of messages starting from a specified id.
     *
     * @param dlcQueueName name of the dead letter channel queue
     * @param firstMsgId   id of the starting id
     * @param count        maximum num of messages to return
     * @return List of message metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataFromDLC(final String dlcQueueName, long firstMsgId,
            int count) throws AndesException {
        return MessagingEngine.getInstance().getNextNMessageMetadataFromDLC(dlcQueueName, firstMsgId, count);
    }

    /**
     * Get expired but not yet deleted messages from message store.
     *
     * @param lowerBoundMessageID lower bound message Id of the safe zone for delete
     * @param queueName queue name in which the expired messages needs to be checked
     * @return list of expired message Ids
     * @throws AndesException
     */
    public List<Long> getExpiredMessages(long lowerBoundMessageID, String queueName) throws AndesException {
        return MessagingEngine.getInstance().getExpiredMessages(lowerBoundMessageID, queueName);
    }

    /**
     * Return last assigned message id of slot for given queue.
     *
     * @param queueName name of destination queue
     * @return last assign message id
     */
    public long getLastAssignedSlotMessageId(String queueName) throws AndesException {
        return MessagingEngine.getInstance().getLastAssignedSlotMessageId(queueName);
    }

    /**
     * Generate a new message ID. The return id will be always unique
     * even for different message broker nodes
     *
     * @return id generated
     */
    public long generateUniqueId() {
        return MessagingEngine.getInstance().generateUniqueId();
    }

    /**
     * Create a new Andes channel for a new local channel.
     *
     * @param listener Local flow control listener
     * @return AndesChannel
     */
    public AndesChannel createChannel(FlowControlListener listener) {
        return flowControlManager.createChannel(listener);
    }

    /**
     * Create a new Andes channel for a new local channel.
     *
     * @param listener  Local flow control listener
     * @param channelId the channel id
     * @return AndesChannel
     */
    public AndesChannel createChannel(String channelId, FlowControlListener listener) throws AndesException {
        return flowControlManager.createChannel(channelId, listener);
    }

    /**
     * Remove Andes channel from tracking.
     *
     * @param channel Andes channel
     */
    public void deleteChannel(AndesChannel channel) {
        flowControlManager.deleteChannel(channel);
    }

    /**
     * Create andes binding in Andes kernel.
     *
     * @param bindingsEvent InboundBindingEvent binding to be created
     * @throws AndesException
     */
    public void addBinding(InboundBindingEvent bindingsEvent) throws AndesException {
        bindingsEvent.prepareForAddBindingEvent(contextInformationManager);
        inboundEventManager.publishStateEvent(bindingsEvent);
        bindingsEvent.waitForCompletion();
    }

    /**
     * Remove andes binding from andes kernel.
     *
     * @param bindingEvent binding to be removed
     * @throws AndesException
     */
    public void removeBinding(InboundBindingEvent bindingEvent) throws AndesException {
        bindingEvent.prepareForRemoveBinding(contextInformationManager);
        inboundEventManager.publishStateEvent(bindingEvent);
    }

    /**
     * Create an exchange in Andes kernel.
     *
     * @param exchangeEvent InboundExchangeEvent for AMQP exchange
     * @throws AndesException
     */
    public void createExchange(InboundExchangeEvent exchangeEvent) throws AndesException {
        exchangeEvent.prepareForCreateExchange(contextInformationManager);
        inboundEventManager.publishStateEvent(exchangeEvent);
    }

    /**
     * Delete exchange from andes kernel.
     *
     * @param exchangeEvent exchange to delete
     * @throws AndesException
     */
    public void deleteExchange(InboundExchangeEvent exchangeEvent) throws AndesException {
        exchangeEvent.prepareForDeleteExchange(contextInformationManager);
        inboundEventManager.publishStateEvent(exchangeEvent);
    }

    public boolean checkIfQueueDeletable(InboundQueueEvent queueEvent) throws AndesException {
        queueEvent.prepareForCheckIfQueueDeletable(contextInformationManager);
        inboundEventManager.publishStateEvent(queueEvent);

        return queueEvent.IsQueueDeletable();
    }

    /**
     * Get a new transaction object. This object handles the lifecycle of transactional message publishing.
     * Once the transactional session is closed this object needs to be closed as well.
     *
     * @return InboundTransactionEvent
     * @throws AndesException
     */
    public InboundTransactionEvent newTransaction(AndesChannel channel) throws AndesException {
        return new InboundTransactionEvent(messagingEngine, inboundEventManager, maxTxBatchSize, TX_EVENT_TIMEOUT,
                                           channel);
    }

    /**
     * Get deliverable metadata if exist for the given topic.
     *
     * @param topicName topic name
     * @return List of retain deliverable metadata
     * @throws AndesException
     */
    public List<DeliverableAndesMetadata> getRetainedMetadataByTopic(String topicName) throws AndesException {
        return MessagingEngine.getInstance().getRetainedMessageByTopic(topicName);
    }

    /**
     * Get andes content for given message metadata.
     *
     * @param metadata message metadata
     * @return Andes content of given metadata
     * @throws AndesException
     */
    public AndesContent getRetainedMessageContent(AndesMessageMetadata metadata) throws AndesException {
        return MessagingEngine.getInstance().getRetainedMessageContent(metadata);
    }

    /**
     * On a member left event trigger recovery event. This will trigger a mock submit slot event to coordinator for all
     * the queues and topics. This is to avoid any lost submit slot events from left member node
     */
    public void triggerRecoveryEvent() {
        inboundEventManager.publishRecoveryEvent();
    }

    /**
     * Allocate a distributed transaction object for dtx calls
     *
     * @param channel   channel object of the session
     * @param sessionID session id
     * @return created DistributeTransaction object
     * @throws AndesException if maximum number of parallel transactions limit reached
     */
    public synchronized DistributedTransaction createDistributedTransaction(AndesChannel channel, UUID sessionID)
            throws AndesException {
        if (dtxChannelList.size() <= maxParallelDtxConnections) {
            DistributedTransaction distributedTransaction = new DistributedTransaction(dtxRegistry,
                                                                                       inboundEventManager,
                                                                                       channel);
            dtxChannelList.add(sessionID);
            return distributedTransaction;
        } else {
            throw new AndesException("Maximum number of parallel transactions limit " + maxParallelDtxConnections
                                             + " reached. ");
        }
    }

    /**
     * Consider the given distributed transaction as stale
     *
     * @param sessionId session id of the transaction channel
     */
    public synchronized void releaseDistributedTransaction(UUID sessionId) {
        dtxChannelList.remove(sessionId);
    }

    /**
     * Get list of prepared transactions from Dtx registry
     *
     * @return list of XIDs belonging to branches in prepared state
     */
    public ArrayList<Xid> getPreparedDtxTransactions() {
        return dtxRegistry.getPreparedTransactions();
    }

    /**
     * Get message IDs in dlc for a queue for a given number of messages starting from the specified id.
     *
     * @param sourceQueue    name of the queue
     * @param dlcQueueName   name of the dead letter channel queue
     * @param startMessageId starting message id
     * @param messageLimit   maximum num of messages to return in one invocation.
     * @return List<Long> of message IDs
     * @throws AndesException if an error occurs while reading message IDs from database.
     */
    public List<Long> getNextNMessageIdsInDLCForQueue(final String sourceQueue, final String dlcQueueName,
                                                      long startMessageId, int messageLimit) throws AndesException {
        return MessagingEngine.getInstance().getNextNMessageIdsInDLCForQueue(sourceQueue, dlcQueueName, startMessageId, messageLimit);
    }

    /***
     * Get message IDs in DLC starting from given startMessageId up to the given message count.
     *
     * @param dlcQueueName Queue name of the Dead Letter Channel
     * @param startMessageId last message ID returned from invoking this method.
     * @return List<Long> of message IDs moved to the DLC from the sourceQueue.
     * @throws AndesException if an error occurs while reading messages from the database.
     */
    public List<Long> getNextNMessageIdsInDLC(final String dlcQueueName, long startMessageId, int messageLimit)
            throws AndesException {
        return MessagingEngine.getInstance().getNextNMessageIdsInDLC(dlcQueueName, startMessageId, messageLimit);
    }

    /**
     * Publishes a channel suspend/resume event to the disruptor.
     *
     * @param channelId           the channel id from which the request was received
     * @param active              whether the channel should be suspended/resumed. If set to true, channel will be
     *                            resumed and vis versa
     * @param channelFlowCallback the call back to be registered to send the response
     */
    public void notifySubscriptionFlow(UUID channelId, boolean active, DisruptorEventCallback channelFlowCallback) {

        InboundChannelFlowEvent inboundChannelFlowEvent = new InboundChannelFlowEvent(channelId, active,
                channelFlowCallback);
        inboundEventManager.publishStateEvent(inboundChannelFlowEvent);
    }
}

