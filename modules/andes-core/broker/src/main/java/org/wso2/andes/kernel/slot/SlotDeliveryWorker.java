/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.slot;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.MessageFlusher;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.subscription.SubscriptionStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;

/**
 * SlotDelivery worker is responsible of distributing messages to subscribers. Messages will be
 * taken from a slot.
 */
public class SlotDeliveryWorker extends Thread implements StoreHealthListener{


    /**
     * keeps storage queue name vs actual destination it represent
     */
    private ConcurrentSkipListMap<String, String> storageQueueNameToDestinationMap;

    /**
     * Map to keep track of subscription to slots map.
     * There was no provision to remove messageBufferingTracker when last subscriber close before receive all messages in slot.
     * We use this map to delete remaining tracking when last subscriber close in particular destination.
     */
    private final ConcurrentMap<String, Map<String,Slot>> storageQueueToSlotTracker = new ConcurrentHashMap<>();

    private SubscriptionStore subscriptionStore;
    private static Log log = LogFactory.getLog(SlotDeliveryWorker.class);

    /**
     * This map contains slotId to slot hash map against queue name
     */
    private volatile boolean running;
    private MessageFlusher messageFlusher;
    private SlotCoordinator slotCoordinator;

    /**
     * Indicates and provides a barrier if messages stores become offline.
     * marked as volatile since this value could be set from a different thread
     * (other than those of Disruptor)
     */
    private volatile SettableFuture<Boolean> messageStoresUnavailable;

    /**
     * Maximum number to retries retrieve metadata list for a given storage
     * queue ( in the errors occur in message stores)
     */
    private static final int MAX_META_DATA_RETRIEVAL_COUNT = 5;
    
    public SlotDeliveryWorker() {
        messageFlusher = MessageFlusher.getInstance();
        this.storageQueueNameToDestinationMap = new ConcurrentSkipListMap<>();
        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        slotCoordinator = MessagingEngine.getInstance().getSlotCoordinator();
        messageStoresUnavailable = null;
        FailureObservingStoreManager.registerStoreHealthListener(this);
    }

    public void rescheduleMessagesForDelivery(String storageQueueName, List<DeliverableAndesMetadata> messages) {
        String destination = storageQueueNameToDestinationMap.get(storageQueueName);
        MessageFlusher.getInstance().addAlreadyTrackedMessagesToBuffer(destination, messages);
    }

    @Override
    public void run() {
        /**
         * This while loop is necessary since whenever there are messages this thread should
         * deliver them
         */
        running = true;
        while (running) {

            //Iterate through all the queues registered in this thread
            int idleQueueCounter = 0;

            for (String storageQueueName : storageQueueNameToDestinationMap.keySet()) {
                String destinationOfMessagesInQueue = storageQueueNameToDestinationMap.get(storageQueueName);
                Collection<LocalSubscription> subscriptions4Queue;
                try {
                    subscriptions4Queue = 
                            subscriptionStore.getActiveLocalSubscribersForQueuesAndTopics(destinationOfMessagesInQueue);
                    if (subscriptions4Queue != null && !subscriptions4Queue.isEmpty()) {
                        //Check in memory buffer in MessageFlusher has room
                        if (messageFlusher.getMessageDeliveryInfo(destinationOfMessagesInQueue)
                                .isMessageBufferFull()) {
                            
                            //get a slot from coordinator.
                            Slot currentSlot = requestSlot(storageQueueName);
                            currentSlot.setDestinationOfMessagesInSlot(destinationOfMessagesInQueue);
                            
                            /**
                             * If the slot is empty
                             */
                            if (0 == currentSlot.getEndMessageId()) {

                                    /*
                                    If the message buffer in MessageFlusher is not empty
                                     send those messages
                                     */
                                if (log.isDebugEnabled()) {
                                    log.debug("Received an empty slot from slot manager");
                                }
                                boolean sentFromMessageBuffer = sendFromMessageBuffer(destinationOfMessagesInQueue);
                                if (!sentFromMessageBuffer) {
                                    //No available free slots
                                    idleQueueCounter++;
                                    if (idleQueueCounter == storageQueueNameToDestinationMap.size()) {
                                        try {
                                            if (log.isDebugEnabled()) {
                                                log.debug("Sleeping Slot Delivery Worker");
                                            }
                                            Thread.sleep(100);
                                        } catch (InterruptedException ignored) {
                                            //Silently ignore
                                        }
                                    }
                                }
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("Received slot for storage queue " + storageQueueName + " " +
                                            "is: " + currentSlot.getStartMessageId() +
                                            " - " + currentSlot.getEndMessageId() +
                                            "Thread Id:" + Thread.currentThread().getId());
                                }
                                List<DeliverableAndesMetadata> messagesRead = getMetaDataListBySlot(storageQueueName,
                                                                                                currentSlot);

                                if (messagesRead != null && !messagesRead.isEmpty()) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Number of messages read from slot " + currentSlot.getStartMessageId()
                                                  + " - " + currentSlot.getEndMessageId() + " is " + messagesRead.size()
                                                  + " storage queue= " + storageQueueName);
                                    }

                                    storageQueueToSlotTracker.putIfAbsent(storageQueueName,new HashMap<String, Slot>());

                                    Map<String, Slot> subscriptionSlots = storageQueueToSlotTracker.get(storageQueueName);

                                    Slot trackedSlot = subscriptionSlots.get(currentSlot.getId());
                                    if (trackedSlot == null) {
                                        subscriptionSlots.put(currentSlot.getId(), currentSlot);
                                        trackedSlot = currentSlot;
                                    } else {
                                        if (log.isDebugEnabled()) {
                                            log.debug("Overlapped slot received. Slot ID " + trackedSlot.getId());
                                        }
                                    }

                                    filterOverlappedMessages(trackedSlot, messagesRead);
                                    MessageFlusher.getInstance().sendMessageToBuffer(messagesRead, trackedSlot);
                                    MessageFlusher.getInstance()
                                                  .sendMessagesInBuffer(trackedSlot
                                                          .getDestinationOfMessagesInSlot());
                                } else {
                                    currentSlot.setSlotInActive();
                                    SlotDeletionExecutor.getInstance().executeSlotDeletion(currentSlot);
                                }
                            }

                        } else {
                                /*If there are messages to be sent in the message
                                            buffer in MessageFlusher send them */
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "The queue " + storageQueueName + " has no room. Thus sending " +
                                                "from buffer.");
                            }
                            sendFromMessageBuffer(destinationOfMessagesInQueue);
                        }
                    } else {
                        idleQueueCounter++;
                        if (idleQueueCounter == storageQueueNameToDestinationMap.size()) {
                            try {
                                if (log.isDebugEnabled()) {
                                    log.debug("Sleeping Slot Delivery Worker");
                                }
                                Thread.sleep(100);
                            } catch (InterruptedException ignored) {
                                //Silently ignore
                            }
                        }
                    }
                } catch (AndesException e) {
                    log.error("Error running Message Store Reader " + e.getMessage(), e);
                } catch (ConnectionException e) {
                    log.error("Error occurred while connecting to the thrift coordinator " +
                            e.getMessage(), e);
                    setRunning(false);
                    //Any exception should be caught here. Otherwise SDW thread will stop
                    //and MB node will become useless
                } catch (Exception e) {
                    log.error("Error while running Slot Delivery Worker. ", e);
                }
            }
        }

    }

    /**
     * This will remove already buffered messages from the messagesRead list. This is to avoid resending a message.
     *
     * @param slot
     *         Slot which contains the given messages
     * @param messages
     *         Messages of the given slots
     */
    private void filterOverlappedMessages(Slot slot, List<DeliverableAndesMetadata> messages) {
        Iterator<DeliverableAndesMetadata> readMessageIterator = messages.iterator();

        // Filter already read messages
        while (readMessageIterator.hasNext()) {
            DeliverableAndesMetadata currentMessage = readMessageIterator.next();
            if (slot.checkIfMessageIsAlreadyAdded(currentMessage.getMessageID())) {
                if(log.isDebugEnabled()) {
                    log.debug("Tracker rejected message id= " + currentMessage.getMessageID()
                            + " from buffering " + "to deliver. This is an already buffered message");
                }
                readMessageIterator.remove();
            } else {
                currentMessage.changeSlot(slot);
                slot.addMessageToSlotIfAbsent(currentMessage);
            }
        }
    }

    public void stopDeliveryForQueue(String storageQueue) {
        MessageFlusher.getInstance().clearUpAllBufferedMessagesForDelivery
                (storageQueueNameToDestinationMap.get(storageQueue));
        Map<String, Slot> orphanedSlots = storageQueueToSlotTracker.remove(storageQueue);

        // Check if there are any orphaned slots
        if (null != orphanedSlots) {
            for (Slot slot : orphanedSlots.values()) {
                if (log.isDebugEnabled()) {
                    log.debug("Orphan slot situation and clear tracking of messages for slot = " + slot);
                }
                slot.markMessagesOfSlotAsReturned();
            }
        }
    }

    /**
     * Returns a list of {@link AndesMessageMetadata} in specified slot
     * @param storageQueueName name of the storage queue which this slot belongs to
     * @param slot the slot which messages are retrieved.
     * @return a list of {@link AndesMessageMetadata}
     * @throws AndesException an exception if there are errors at message store level.
     */
    private List<DeliverableAndesMetadata> getMetaDataListBySlot(String storageQueueName,
                                                             Slot slot) throws AndesException {
        return getMetadataListBySlot(storageQueueName, slot, 0);
    }

    /**
     * Returns a list of {@link AndesMessageMetadata} in specified slot. This method is recursive.
     * @param storageQueueName storage queue of the slot
     * @param slot Retrieve metadata relevant to the given {@link org.wso2.andes.kernel.slot.Slot}
     * @param numberOfRetriesBefore retry count for the query
     * @return return a list of {@link org.wso2.andes.kernel.AndesMessageMetadata}
     * @throws AndesException
     */
    private List<DeliverableAndesMetadata> getMetadataListBySlot(String storageQueueName,
                                                             Slot slot,
                                                             int numberOfRetriesBefore) throws AndesException {

        List<DeliverableAndesMetadata> messagesRead;
               
        if ( messageStoresUnavailable != null){
            try {
                
                log.info("Message store has become unavailable therefore "+ 
                          "waiting until store becomes available. thread id: " + this.getId());
                messageStoresUnavailable.get();
                messageStoresUnavailable = null; // we are passing the blockade (therefore clear it).
                log.info("Message store became available. resuming work. thread id: " + this.getId());
                
            } catch (InterruptedException e) {
                throw new AndesException("Thread interrupted while waiting for message stores to come online", e);
            } catch (ExecutionException e){
                throw new AndesException("Error occurred while waiting for message stores to come online", e);
            }
        }
        
        try{
            
            long firstMsgId = slot.getStartMessageId();
            long lastMsgId = slot.getEndMessageId();
            //Read messages in the slot
            messagesRead = MessagingEngine.getInstance().getMetaDataList(slot,
                            storageQueueName, firstMsgId, lastMsgId);
            
            if (log.isDebugEnabled()) {
                StringBuilder messageIDString = new StringBuilder();
                for (DeliverableAndesMetadata metadata : messagesRead) {
                    messageIDString.append(metadata.getMessageID()).append(" , ");
                }
                log.debug("Messages Read: " + messageIDString);
            }
            
        }catch (AndesException aex){

            if (numberOfRetriesBefore <= MAX_META_DATA_RETRIEVAL_COUNT) {
                String errorMsg =
                                  String.format("error occurred retrieving metadata list for slot :"
                                                + " %s, retry count = %d",
                                                slot.toString(), numberOfRetriesBefore);
                log.error(errorMsg, aex);
                messagesRead = getMetadataListBySlot(storageQueueName, slot, numberOfRetriesBefore + 1);
            } else {
                String errorMsg =
                         String.format("error occurred retrieving metadata list for slot : %s, in final attempt = %d. "
                                       + "this slot will not be delivered and become stale in message store",
                                       slot.toString(), numberOfRetriesBefore);
                throw new AndesException(errorMsg, aex);
            }
            
        }
        
        return messagesRead;

    }
    
    
    /** 
     * Get a slot from the Slot to deliver ( from the coordinator if the MB is clustered)
     * @param storageQueueName the storage queue name for from which a slot should be returned.
     * @return a {@link Slot}
     * @throws ConnectionException if connectivity to coordinator is lost.
     */
    private Slot requestSlot(String storageQueueName) throws ConnectionException {
        long startTime = System.currentTimeMillis();
        Slot currentSlot = slotCoordinator.getSlot(storageQueueName);
        long endTime = System.currentTimeMillis();

        if (log.isDebugEnabled()) {
            log.debug(
                    (endTime - startTime) + " milliSec took to get a slot" +
                            " from slot manager");
        }
        return currentSlot;
    }


    /**
     * Send messages from buffer in MessageFlusher if the buffer is not empty
     *
     * @param msgDestination queue/topic message is addressed to
     * @return whether the messages are sent from message buffer or not
     * @throws AndesException
     */
    private boolean sendFromMessageBuffer(String msgDestination) throws AndesException {
        boolean sentFromMessageBuffer = false;
        if (!messageFlusher.isMessageBufferEmpty(msgDestination)) {
            messageFlusher.sendMessagesInBuffer(msgDestination);
            sentFromMessageBuffer = true;
        }
        return sentFromMessageBuffer;
    }

    /**
     * Add a queue to queue list of this SlotDeliveryWorkerThread
     *
     * @param storageQueueName queue name of the newly added queue
     */
    public void addQueueToThread(String storageQueueName, String destination) throws AndesException {
        getStorageQueueNameToDestinationMap().put(storageQueueName, destination);
        messageFlusher.prepareForDelivery(destination);
    }

    /**
     * Get queue list belongs to this thread
     *
     * @return queue list
     */
    public ConcurrentSkipListMap<String, String> getStorageQueueNameToDestinationMap() {
        return storageQueueNameToDestinationMap;
    }


    /**
     * @return Whether the worker thread is in running state or not
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Set state of the worker thread
     *
     * @param running new state of the worker
     */
    public void setRunning(boolean running) {
        this.running = running;
    }


    /**
     * Submit slot to execute delete
     *
     * @param slot slot to delete
     */
    public void deleteSlot(Slot slot) {
        SlotDeletionExecutor.getInstance().executeSlotDeletion(slot);
        if (log.isDebugEnabled()) {
            log.debug("Releasing tracking of messages for slot " + slot.toString());
        }
        slot.deleteAllMessagesInSlot();
        storageQueueToSlotTracker.get(slot.getStorageQueueName()).remove(slot.getId());
    }

    /**
     * Dump all message status of the slots owned by this slot delivery worker
     * @param fileToWrite file to dump
     * @throws AndesException
     */
    public void dumpAllSlotInformationToFile(File fileToWrite) throws AndesException {
        try {
            FileWriter information = new FileWriter(fileToWrite);
            for (Map.Entry<String, Map<String, Slot>> storageQueueToSlotEntry : storageQueueToSlotTracker.entrySet()) {
                String storageQueue = storageQueueToSlotEntry.getKey();
                Map<String, Slot> slotIdToSlotMap = storageQueueToSlotEntry.getValue();
                for (Map.Entry<String, Slot> slotEntry : slotIdToSlotMap.entrySet()) {
                    String slotID = slotEntry.getKey();
                    List<DeliverableAndesMetadata> messagesOfSlot = slotEntry.getValue().getAllMessagesOfSlot();
                    if(!messagesOfSlot.isEmpty()) {

                        int writerFlushCounter = 0;
                        for(DeliverableAndesMetadata message : messagesOfSlot) {
                            information.append(storageQueue)
                                    .append(",")
                                    .append(slotID).append(",")
                                    .append(message.dumpMessageStatus())
                                    .append("\n");
                            writerFlushCounter = writerFlushCounter + 1;
                            if(writerFlushCounter % 10 == 0) {
                                information.flush();
                            }
                        }

                        information.flush();
                    }
                }
            }

            information.flush();
            information.close();

        } catch (FileNotFoundException e) {
            log.error("File to write is not found", e);
            throw new AndesException("File to write is not found", e);
        } catch (IOException e) {
            log.error("Error while dumping message status to file", e);
            throw new AndesException("Error while dumping message status to file", e);
        }

    }

    /**
     * {@inheritDoc}
     * <p> Creates a {@link SettableFuture} indicating message store became offline.
     */
    @Override
    public void storeNonOperational(HealthAwareStore store, Exception ex) {

        log.info("Message stores became not operational therefore waiting");
        messageStoresUnavailable = SettableFuture.create();

    }

    /**
     * {@inheritDoc}
     * <p> Sets a value for {@link SettableFuture} indicating message store became online.
     */
    @Override
    public void storeOperational(HealthAwareStore store) {
        log.info("Message stores became operational therefore resuming work");
        messageStoresUnavailable.set(false);

    }
}

