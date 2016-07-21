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

package org.wso2.andes.kernel.slot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessageFlusher;

import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.task.Task;

/**
 * Handle message delivery {@link Task} implementation for a given queue
 */
final class MessageDeliveryTask extends Task {

    private static Log log = LogFactory.getLog(MessageDeliveryTask.class);

    /**
     * The storage queue handled by this task.
     */
    private StorageQueue storageQueue;

    /**
     * Reference to {@link MessageFlusher} to deliver messages
     */
    private MessageFlusher messageFlusher;

    /**
     * Reference to slot coordinator to retrieve slots
     */
    private SlotCoordinator slotCoordinator;


    MessageDeliveryTask(StorageQueue storageQueue,
                        SlotCoordinator slotCoordinator,
                        MessageFlusher messageFlusher) {

        this.storageQueue = storageQueue;
        this.slotCoordinator = slotCoordinator;
        this.messageFlusher = messageFlusher;
    }

    /**
     * Slot delivery task
     * {@inheritDoc}
     */
    @Override
    public TaskHint call() throws Exception {

        TaskHint taskHint = TaskHint.ACTIVE;

        String storageQueueName = storageQueue.getName();

        if (storageQueue.checkForReadMessageBufferLimit()) {

            // Get a slot from coordinator.
            Slot currentSlot = requestSlot(storageQueueName);

            // If the slot is empty
            if (0 == currentSlot.getEndMessageId()) {

                // If the message buffer in MessageFlusher is not empty send those messages
                if (log.isDebugEnabled()) {
                    log.debug("Received an empty slot from slot manager");
                }
                //there is no fresh slot. Thus flush buffered messages
                int numberOfMessageSent = sendMessagesToSubscriptions(storageQueue);
                if (numberOfMessageSent == 0) {
                    taskHint = TaskHint.IDLE; // Didn't do productive work
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Received slot for storage queue " + storageQueueName + " is: " +
                                      currentSlot.getStartMessageId() + " - " + currentSlot.getEndMessageId() +
                                      "Thread Id:" + Thread.currentThread().getId());
                }

                storageQueue.loadMessagesForDelivery(currentSlot);
                sendMessagesToSubscriptions(storageQueue);
            }

        } else {
            //If there are messages to be sent in the message buffer in MessageFlusher send them
            if (log.isDebugEnabled()) {
                log.debug("The queue " + storageQueueName + " has no room to buffer messages. " +
                        "Thus flushing the messages to subscriptions");
            }
            sendMessagesToSubscriptions(storageQueue);
        }
        return taskHint;
    }


    /**
     * Flush messages of queue to bounded subscriptions. This will get
     * a copy of active subscribers at the moment and send messages to them
     *
     * @return how many messages sent
     * @throws AndesException
     */
    public int sendMessagesToSubscriptions(StorageQueue queue) throws AndesException {
        return messageFlusher.sendMessagesToSubscriptions(queue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onAdd() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onRemove() {
        onStopDelivery();
    }

    /**
     * unque id of the {@link Task}
     * @return name of storage queue handle by this {@link MessageDeliveryTask}
     */
    @Override
    public String getId() {
        return storageQueue.getName();
    }

    /**
     * Get a slot from the Slot to deliver ( from the coordinator if the MB is clustered)
     *
     * @param storageQueueName the storage queue name for from which a slot should be returned.
     * @return a {@link Slot}
     * @throws ConnectionException if connectivity to coordinator is lost.
     */
    private Slot requestSlot(String storageQueueName) throws ConnectionException {

        long startTime = System.currentTimeMillis();
        Slot currentSlot = slotCoordinator.getSlot(storageQueueName);
        long endTime = System.currentTimeMillis();
        currentSlot.setDestinationOfMessagesInSlot(storageQueueName);

        if (log.isDebugEnabled()) {
            log.debug((endTime - startTime) + " milli seconds to get a slot from slot manager");
        }
        return currentSlot;
    }

    /**
     * Update slot states when delivery stop
     */
    private void onStopDelivery() {
        storageQueue.clearMessagesReadToBufferForDelivery();
    }
}
