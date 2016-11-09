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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible of allocating SloDeliveryWorker threads to each queue
 */
public class SlotDeliveryWorkerManager {

    private Map<Integer, SlotDeliveryWorker> slotDeliveryWorkerMap = new ConcurrentHashMap<>();

    private ExecutorService slotDeliveryWorkerExecutor;

    private static Log log = LogFactory.getLog(SlotDeliveryWorkerManager.class);

    /**
     * Number of slot delivery worker threads running inn one MB node
     */
    private static ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat
            ("SlotDeliveryWorkerExecutor-%d").build();


    /**
    Number of slot delivery worker threads running in one MB node
     */
    private Integer numberOfThreads;

    /**
     * SlotDeliveryWorker instance
     */
    private static SlotDeliveryWorkerManager slotDeliveryWorkerManagerManager = new SlotDeliveryWorkerManager();


    private SlotDeliveryWorkerManager() {
        numberOfThreads = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SLOTS_WORKER_THREAD_COUNT);
        this.slotDeliveryWorkerExecutor = Executors.newFixedThreadPool(numberOfThreads, namedThreadFactory);
    }

    /**
     * @return SlotDeliveryWorkerManager instance
     */
    public static SlotDeliveryWorkerManager getInstance() {
        return slotDeliveryWorkerManagerManager;
    }

    public void rescheduleMessagesForDelivery(String storageQueueName, List<DeliverableAndesMetadata> messages) {
        SlotDeliveryWorker slotWorker = getSlotWorker(storageQueueName);

        if (null != slotWorker) {
            slotWorker.rescheduleMessagesForDelivery(storageQueueName, messages);
        }
    }

    /**
     * When a subscription is added this method will be called. This method will decide which
     * SlotDeliveryWorker thread is assigned to which queue. If a worker is already running on
     * the queue, it will not start a new one.
     *
     * @param storageQueueName  name of the queue to start slot delivery worker for
     * @param destination The destination name
     * @param protocolType The protocol which the messages in this storage queue belongs to
     * @param destinationType The destination type which the messages in this storage queue belongs to
     */
    public synchronized void startSlotDeliveryWorker(String storageQueueName, String destination,
            ProtocolType protocolType, DestinationType destinationType) throws AndesException {

        int slotDeliveryWorkerId = getIdForSlotDeliveryWorker(storageQueueName);
        if (getSlotDeliveryWorkerMap().containsKey(slotDeliveryWorkerId)) {

            SlotDeliveryWorker slotDeliveryWorker = getSlotDeliveryWorkerMap().get(slotDeliveryWorkerId);

            if (slotDeliveryWorker.isShutdownTriggered()) {

                if (log.isDebugEnabled()) {
                    log.debug("SlotDeliveryWorker " + slotDeliveryWorkerId +
                            " has been scheduled to shutdown. Waiting for shutdown " +
                            "completion before starting a new instance");
                }

                int waitCount = 0;
                while(!slotDeliveryWorker.isShutdownComplete()) {

                    if (waitCount++ > 10) {
                        throw new AndesException("Couldn't start a new SlotDeliveryWorker for Id " +
                                slotDeliveryWorkerId + " since the previous instance is still running");
                    }

                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        throw new AndesException("Error waiting for SlotDeliveryWorker " + slotDeliveryWorkerId +
                                " to shutdown", e);
                    }
                }

                if (log.isDebugEnabled()) {
                    log.debug("SlotDeliveryWorker : " + slotDeliveryWorker.getId()
                            + "has been scheduled for shutdown. Therefore starting a new instance.");
                }

                startNewSlotDeliveryWorkerInstance(slotDeliveryWorkerId, storageQueueName, destination, protocolType,
                        destinationType);

            } else if (!getSlotDeliveryWorkerMap().get(slotDeliveryWorkerId).isStorageQueueAdded(storageQueueName)) {
                slotDeliveryWorker.startDeliveryForQueue(storageQueueName, destination, protocolType, destinationType);
            }
        } else {
            startNewSlotDeliveryWorkerInstance(slotDeliveryWorkerId, storageQueueName, destination,
                    protocolType, destinationType);
        }
    }

    /**
     * Starts a new SlotDeliveryWorker instance and assigns the given storage to it.
     *
     * @param slotDeliveryWorkerId The Id of the new SlotDeliveryWorker
     * @param storageQueueName The storage queue name to start working on
     * @param destination The destination of the storage queue
     * @param protocolType The protocol type of the storage queue
     * @param destinationType The destination type of the storage queue
     * @throws AndesException
     */
    private void startNewSlotDeliveryWorkerInstance(int slotDeliveryWorkerId, String storageQueueName,
            String destination, ProtocolType protocolType, DestinationType destinationType) throws AndesException {

        SlotDeliveryWorker slotDeliveryWorker = new SlotDeliveryWorker();
        slotDeliveryWorker.startDeliveryForQueue(storageQueueName, destination, protocolType, destinationType);
        getSlotDeliveryWorkerMap().put(slotDeliveryWorkerId, slotDeliveryWorker);
        slotDeliveryWorkerExecutor.execute(slotDeliveryWorker);

        if(log.isDebugEnabled()) {
            log.debug("A new instance of SlotDeliveryWorker with Id " + slotDeliveryWorkerId +
                    " started for storage queue " + storageQueueName + " with message destination " + destination);
        }
    }

    /**
     * This method is to decide slotDeliveryWorkerId for the queue
     *
     * @param queueName name of the newly created queue
     * @return slot delivery worker ID
     */
    public int getIdForSlotDeliveryWorker(String queueName) {
        // Get the absolute value since String.hashCode() can give both positive and negative values.
        return Math.abs(queueName.hashCode() % numberOfThreads);
    }

    /**
     * Stop delivery task for the given storage queue locally. This is normally called when all the subscribers for a
     * destination leave the local node.
     *
     * @param storageQueueName
     *         Name of the Storage queue
     */
    public void stopDeliveryForDestination(String storageQueueName) {
        SlotDeliveryWorker slotWorker = getSlotWorker(storageQueueName);

        // Check if there is a slot delivery worker for the storageQueueName
        if (null != slotWorker) {
            if (log.isDebugEnabled()) {
                log.debug("Stopping delivery for storage queue " + storageQueueName +
                        " with SlotDeliveryWorker : " + slotWorker.getId());
            }
            slotWorker.stopDeliveryForQueue(storageQueueName);
        }
    }


    /**
     * Stop all stop delivery workers in the thread pool
     */
    public void stopSlotDeliveryWorkers() {
        Set<Map.Entry<Integer, SlotDeliveryWorker>> slotDeliveryWorkerEntries = getSlotDeliveryWorkerMap().entrySet();
        for (Map.Entry<Integer, SlotDeliveryWorker> slotDeliveryWorkerEntry : slotDeliveryWorkerEntries) {
            slotDeliveryWorkerEntry.getValue().scheduleForShutdown();
        }
    }

    /**
     * @return SlotDeliveryWorkerMap  a map which stores slot delivery worker ID against
     * SlotDelivery
     * Worker
     * object references
     */
    private Map<Integer, SlotDeliveryWorker> getSlotDeliveryWorkerMap() {
        return slotDeliveryWorkerMap;
    }


    /**
     * Start slot delivery worker executor.
     */
    public synchronized void startSlotDelivery() {
        if (slotDeliveryWorkerExecutor.isShutdown()) {
            this.slotDeliveryWorkerExecutor = Executors.newFixedThreadPool(numberOfThreads, namedThreadFactory);
        }
    }

    /**
     * Stop all slot delivery workers and shutdown the executor service
     */
    public synchronized void stopSlotDelivery() {
        stopSlotDeliveryWorkers();

        slotDeliveryWorkerExecutor.shutdownNow();
        try {
            // Maximum time waited for a SDW to terminate in minutes
            int maxTerminationAwaitTime = 1;

            boolean terminationSuccessful = slotDeliveryWorkerExecutor
                    .awaitTermination(maxTerminationAwaitTime, TimeUnit.MINUTES);

            if (!terminationSuccessful) {
                log.error("Could not stop SlotDeliveryWorkers.");
            }
        } catch (InterruptedException e) {
            log.error("Slot delivery executor shutdown process was interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Start all the SlotDeliveryWorkers if not already in running state.
     *
     * @param activeLocalSubscribers All active local subscribers to start SlotDeliveryWorker on
     * @throws AndesException
     */
    public void startAllSlotDeliveryWorkers(Collection<AndesSubscription> activeLocalSubscribers)
            throws AndesException {

        for (AndesSubscription subscription : activeLocalSubscribers) {
            startSlotDeliveryWorker(subscription.getStorageQueueName(), AndesUtils.getDestination(subscription),
                    subscription.getProtocolType(), subscription.getDestinationType());
        }
    }

    /**
     * Returns SlotDeliveryWorker mapped to a given queue
     *
     * @param queueName name of the queue
     * @return SlotDeliveryWorker instance
     */
    public SlotDeliveryWorker getSlotWorker(String queueName) {
        return slotDeliveryWorkerMap.get(getIdForSlotDeliveryWorker(queueName));
    }

    /**
     * Dump all message status of the slots owned by this slot delivery worker
     * @param fileToWrite file to dump
     * @throws AndesException
     */
    public void dumpAllSlotInformationToFile(File fileToWrite) throws AndesException{
        for (SlotDeliveryWorker slotDeliveryWorker : slotDeliveryWorkerMap.values()) {
            slotDeliveryWorker.dumpAllSlotInformationToFile(fileToWrite);
        }
    }
}
