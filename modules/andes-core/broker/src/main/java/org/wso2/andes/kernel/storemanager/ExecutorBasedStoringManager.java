package org.wso2.andes.kernel.storemanager;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.MessageStoreManager;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.store.MessageContentRemoverTask;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ExecutorBasedStoringManager extends BasicStoringManager implements MessageStoreManager {

    private static Log log = LogFactory.getLog(ExecutorBasedStoringManager.class);

    private MessageStoreManager directMessageStoreManager;

    private MessageStore messageStore;

    //Metadata list buffered in-memory
    private List<AndesMessageMetadata> metaList = new ArrayList<AndesMessageMetadata>();

    //Message content list buffered in-memory
    private List<AndesMessagePart> partList = new ArrayList<AndesMessagePart>();

    //.Ack list buffered in-memory
    private List<AndesAckData> ackList = new ArrayList<AndesAckData>();

    //Lock to acquire when updating message metadata and content buffers
    private ReentrantReadWriteLock messageBufferFlushingLock = new ReentrantReadWriteLock();

    //Lock to acquire when updating acl list
    private ReentrantReadWriteLock ackFlushingLock = new ReentrantReadWriteLock();

    //Executor service to submit async tasks used in store manager
    private ScheduledExecutorService asyncStoreTasksScheduler;

    //Size of the executor service above
    private int asyncStoreThreadPoolCount;

    //Interval in seconds message flushing to store should happen
    private int bufferFlushingInterval;

    //Interval in seconds ack flushing to store should happen
    private int ackFlushingInterval;

    //Messages will be flushed when message buffer has this num of items
    private int mesageBufferingThrold;

    //Acks will be flushed when ack buffer has this num of items
    private int ackBufferingThreshold;

    //Map to keep message count difference not flushed to disk of each queue
    private Map<String, AtomicInteger> messageCountDifferenceMap = new HashMap<String,
            AtomicInteger>();

    //message count will be flushed to DB in these interval in seconds
    private int messageCountFlushInterval;

    //message count will be flushed to DB when count difference reach this val
    private int messageCountFlushNumberGap;

    //This task will asynchronously remove message content
    private MessageContentRemoverTask messageContentRemoverTask;

    //Content deletion task will run once this num of seconds
    private Integer messageContentDeletionTaskInterval;

    //content removal time difference in seconds
    private int contentRemovalTimeDifference;


    /**
     * Create an instance of ExecutorBasedStoringManager
     * @param messageStore message store to operate on
     * @throws AndesException in case of init
     */
    public ExecutorBasedStoringManager(MessageStore messageStore) throws AndesException,
            ConfigurationException {
        super(messageStore);
        this.messageStore = messageStore;
        initialise(messageStore);
    }



    private void initialise(final MessageStore messageStore) throws AndesException, ConfigurationException {

        bufferFlushingInterval = 15;

        ackFlushingInterval = 15;

        asyncStoreThreadPoolCount = 10;

        mesageBufferingThrold = 50;

        ackBufferingThreshold = 50;

        messageCountFlushInterval = 15;

        messageCountFlushNumberGap = 100;

        contentRemovalTimeDifference = 30;

        messageContentDeletionTaskInterval = AndesConfigurationManager.getInstance()
                .readConfigurationValue(AndesConfiguration.PERFORMANCE_TUNING_DELETION_CONTENT_REMOVAL_TASK_INTERVAL);

        directMessageStoreManager = MessageStoreManagerFactory
                .createDirectMessageStoreManager(messageStore);

        asyncStoreTasksScheduler = Executors.newScheduledThreadPool(asyncStoreThreadPoolCount);

        //this thread will flush messages to store time to time
        Thread messageFlusher = new Thread(new Runnable() {
            @Override
            public void run() {
                flushMessages();
            }
        });

        //this thread will handle acks time to time
        Thread ackFlusher = new Thread(new Runnable() {
            @Override
            public void run() {
                flushAcks();
            }
        });

        //this task will periodically flush message count value to the store
        Thread messageCountFlusher = new Thread(new Runnable() {
            @Override
            public void run() {
                for (Map.Entry<String, AtomicInteger> entry : messageCountDifferenceMap.entrySet()) {
                    try {
                        if (entry.getValue().get() > 0) {
                            AndesContext.getInstance().getAndesContextStore()
                                        .incrementMessageCountForQueue(entry.getKey(),
                                                                       entry.getValue().get());
                        } else if (entry.getValue().get() < 0) {
                            AndesContext.getInstance().getAndesContextStore()
                                        .incrementMessageCountForQueue(
                                                entry.getKey(),
                                                entry.getValue().get());
                        }
                        entry.getValue().set(0);
                    } catch (AndesException e) {
                        log.error("Error while updating message counts for queue " + entry.getKey());
                    }
                }
            }
        });

        //this task will periodically remove message contents from store
        messageContentRemoverTask = new MessageContentRemoverTask(messageStore);

        asyncStoreTasksScheduler.scheduleAtFixedRate(messageFlusher,
                                                     10,
                                                     bufferFlushingInterval,
                                                     TimeUnit.SECONDS);

        asyncStoreTasksScheduler.scheduleAtFixedRate(ackFlusher,
                                                     10,
                                                     ackFlushingInterval,
                                                     TimeUnit.SECONDS);

        asyncStoreTasksScheduler.scheduleAtFixedRate(messageCountFlusher,
                                                     10,
                                                     messageCountFlushInterval,
                                                     TimeUnit.SECONDS);

        asyncStoreTasksScheduler.scheduleAtFixedRate(messageContentRemoverTask,
                                                     10,
                messageContentDeletionTaskInterval,
                                                     TimeUnit.SECONDS);

    }

    /**
     * Flush buffered messages to store and clear up the buffers
     */
    private void flushMessages() {
        try {

            messageBufferFlushingLock.writeLock().lock();

            if(!partList.isEmpty()) {
                directMessageStoreManager.storeMessagePart(partList);
            }

            if(!metaList.isEmpty()) {
                directMessageStoreManager.storeMetaData(metaList);
            }

            partList.clear();
            metaList.clear();

        } catch (AndesException e) {
            //we are handling it inside so that exception will not stop message flushing forever
            log.error("Error while flushing messages to store. This might cause message lost", e);
        } finally {
            messageBufferFlushingLock.writeLock().unlock();
        }
    }

    /**
     * Check if message buffers should be flushed
     * @return if to flush
     */
    private boolean messageBufferThresholdReached(){
        return (metaList.size() + partList.size() >= mesageBufferingThrold);
    }

    /**
     * Check if ack buffer should be flushed
     * @return if to flush
     */
    private boolean ackBufferThresholdReached() {
        return ackList.size() >=ackBufferingThreshold;
    }

    /**
     * Process all acks buffered
     */
    private void flushAcks() {
        try {
            ackFlushingLock.writeLock().lock();
            if(!ackList.isEmpty()) {
                directMessageStoreManager.ackReceived(ackList);
                ackList.clear();
            }
        } catch (AndesException e) {
            //we are handling it inside so that exception will not stop ack handling forever
            log.error("Error while flushing acks to store. This might cause message redelivery" ,e);
        } finally {
            ackFlushingLock.writeLock().unlock();
        }
    }

    /**
     * schedule to delete messages
     *
     * @param nanoTimeToWait time gap to elapse from now until delete all is triggered
     * @param messageID      id of the message to be removed
     */
    private void addContentDeletionTask(long nanoTimeToWait, long messageID) {
        messageContentRemoverTask.put(nanoTimeToWait, messageID);
    }

    /**
     * Store metadata. This will put message to the buffer and return
     * @param metadata AndesMessageMetadata metadata to store
     * @throws AndesException
     */
    @Override
    public void storeMetadata(AndesMessageMetadata metadata) throws AndesException {
         try {
             messageBufferFlushingLock.writeLock().lock();
             metaList.add(metadata);
             if(messageBufferThresholdReached()) {
                 flushMessages();
             }
         } finally {
             messageBufferFlushingLock.writeLock().unlock();
         }
    }

    /**
     * Store metadata list. This will put messages to the buffer and return
     * @param messageMetadata metadata list to store
     * @throws AndesException
     */
    @Override
    public void storeMetaData(List<AndesMessageMetadata> messageMetadata) throws AndesException {
        try {
            messageBufferFlushingLock.writeLock().lock();
            metaList.addAll(messageMetadata);
            if(messageBufferThresholdReached()) {
                flushMessages();
            }
        } finally {
            messageBufferFlushingLock.writeLock().unlock();
        }
    }

    /**
     * Store a message chunk.This will put chunk to the buffer and return
     * @param messagePart
     *         AndesMessagePart to store message chunk to store
     * @throws AndesException
     */
    @Override
    public void storeMessagePart(AndesMessagePart messagePart) throws AndesException {
        try {
            messageBufferFlushingLock.writeLock().lock();
            partList.add(messagePart);
            if(messageBufferThresholdReached()) {
                flushMessages();
            }
        } finally {
            messageBufferFlushingLock.writeLock().unlock();
        }
    }

    /**
     * Store message content chunks.This will put chunks to the buffer and return
     * @param messageParts
     *         message parts to store message chunks to store
     * @throws AndesException
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> messageParts) throws AndesException {
        try {
            messageBufferFlushingLock.writeLock().lock();
            partList.addAll(messageParts);
            if(messageBufferThresholdReached()) {
                flushMessages();
            }
        } finally {
            messageBufferFlushingLock.writeLock().unlock();
        }
    }

    /**
     * Process acks. This will put ack to the buffer to process and return
     * @param ackData
     *         AndesAckData ack to process
     * @throws AndesException
     */
    @Override
    public void ackReceived(AndesAckData ackData) throws AndesException {
          try {
              ackFlushingLock.writeLock().lock();
              ackList.add(ackData);
              if(ackBufferThresholdReached()) {
                  flushAcks();
              }
          } finally {
              ackFlushingLock.writeLock().unlock();
          }
    }

    /**
     * Process acks. This will put acks to the buffer to process and return
     * @param ackList
     *         ack message list to process
     * @throws AndesException
     */
    @Override
    public void ackReceived(List<AndesAckData> ackList) throws AndesException {
        try {
            ackFlushingLock.writeLock().lock();
            ackList.addAll(ackList);
            if(ackBufferThresholdReached()) {
                flushAcks();
            }
        } finally {
            ackFlushingLock.writeLock().unlock();
        }
    }

    /**
     * schedule to remove message content chunks of messages
     *
     * @param messageIdList list of message ids whose content should be removed
     * @throws AndesException
     */
    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        for (Long messageId : messageIdList) {
            addContentDeletionTask(System.nanoTime() + contentRemovalTimeDifference * 1000000000,
                                   messageId);
        }
    }

    /**
     * decrement queue count by 1. Flush if difference is in tab
     * @param queueName
     *         name of the queue to decrement count
     * @param decrementBy
     *         decrement count by this value
     * @throws AndesException
     */
    @Override
    public void decrementQueueCount(String queueName, int decrementBy) throws AndesException {
        AtomicInteger msgCount = messageCountDifferenceMap.get(queueName);
        if (msgCount == null) {
            msgCount = new AtomicInteger(0);
            messageCountDifferenceMap.put(queueName, msgCount);
        }

        synchronized (this) {
            int currentVal = msgCount.get();
            int newVal = currentVal - decrementBy;
            msgCount.set(newVal);
        }

        //we flush this value to store in 100 message tabs
        if (msgCount.get() % messageCountFlushNumberGap == 0) {
            if (msgCount.get() > 0) {
                AndesContext.getInstance().getAndesContextStore()
                            .incrementMessageCountForQueue(queueName, msgCount.get());
            } else {
                AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(
                        queueName, msgCount.get());
            }
            msgCount.set(0);
        }
    }

    /**
     * Increment message count of the queue. This will actually update the DB
     * only if conditions are met
     * @param queueName name of the queue to increment count
     * @param incrementBy increment count by this value
     * @throws AndesException
     */
    @Override
    public void incrementQueueCount(String queueName, int incrementBy) throws AndesException {
        AtomicInteger msgCount = messageCountDifferenceMap.get(queueName);
        if (msgCount == null) {
            msgCount = new AtomicInteger(0);
            messageCountDifferenceMap.put(queueName, msgCount);
        }

        synchronized (this) {
            int currentVal = msgCount.get();
            int newVal = currentVal + incrementBy;
            msgCount.set(newVal);
        }

        //we flush this value to store in 100 message tabs
        if (msgCount.get() % messageCountFlushNumberGap == 0) {
            if (msgCount.get() > 0) {
                AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(
                        queueName, msgCount.get());
            } else {
                AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(
                        queueName, msgCount.get());
            }
            msgCount.set(0);
        }
    }

    /**
     * Delete messages in async way and optionally move to DLC
     *
     * @param messagesToRemove        messages to remove
     * @param moveToDeadLetterChannel whether to send to DLC
     * @throws AndesException
     */
    @Override
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove,
                               boolean moveToDeadLetterChannel) throws AndesException {
        List<Long> idsOfMessagesToRemove = new ArrayList<Long>();
        Map<String, List<AndesRemovableMetadata>> storageQueueSeparatedRemoveMessages = new HashMap<String, List<AndesRemovableMetadata>>();
        Map<String, Integer> destinationSeparatedMsgCounts = new HashMap<String, Integer>();

        for (AndesRemovableMetadata message : messagesToRemove) {
            idsOfMessagesToRemove.add(message.getMessageID());

            //update <storageQueue, metadata> map
            List<AndesRemovableMetadata> messages = storageQueueSeparatedRemoveMessages
                    .get(message.getStorageDestination());
            if (messages == null) {
                messages = new ArrayList
                        <AndesRemovableMetadata>();
            }
            messages.add(message);
            storageQueueSeparatedRemoveMessages.put(message.getStorageDestination(), messages);

            //update <destination, Msgcount> map
            Integer count = destinationSeparatedMsgCounts.get(message.getMessageDestination());
            if(count == null) {
                count = 0;
            }
            count = count + 1;
            destinationSeparatedMsgCounts.put(message.getMessageDestination(), count);

            //if to move, move to DLC. This is costy. Involves per message read and writes
            if (moveToDeadLetterChannel) {
                AndesMessageMetadata metadata = messageStore.getMetaData(message.getMessageID());
                messageStore
                        .addMetaDataToQueue(AndesConstants.DEAD_LETTER_QUEUE_NAME, metadata);
            }
        }

        //remove metadata
        for (String storageQueueName : storageQueueSeparatedRemoveMessages.keySet()) {
            messageStore.deleteMessageMetadataFromQueue(storageQueueName,
                                                        storageQueueSeparatedRemoveMessages
                                                                .get(storageQueueName));
        }
        //decrement message counts
        for(String destination: destinationSeparatedMsgCounts.keySet()) {
            decrementQueueCount(destination, destinationSeparatedMsgCounts.get(destination));
        }

        if (!moveToDeadLetterChannel) {
            //remove content
            //TODO: - hasitha if a topic message be careful as it is shared
            deleteMessageParts(idsOfMessagesToRemove);
        }

        if(moveToDeadLetterChannel) {
            //increment message count of DLC
            incrementQueueCount(AndesConstants.DEAD_LETTER_QUEUE_NAME, messagesToRemove.size());
        }
    }

}
