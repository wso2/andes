/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel.registry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesConstants;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.server.information.management.DurableTopicSubscriptionInformationMBean;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import javax.management.ObjectName;

/**
 * This is the storage queue registry. Every storage queue created
 * MUST be registered here. There cannot be two storage queue instances by same name
 */
public class StorageQueueRegistry {

    private Map<String, StorageQueue> storageQueueMap;

    private static final String ANDES_DOMAIN = "org.wso2.andes";
    private static final String TOPIC_INFO_MBEAN_TYPE = "DurableTopicSubscriptionInformation";
    private static Log log = LogFactory.getLog(StorageQueueRegistry.class);

    /**
     * Create a in-memory registry for keeping storage queues created in broker
     */
    public StorageQueueRegistry() {
        storageQueueMap = new HashMap<>();
    }

    /**
     * Create and register a storage queue
     *
     * @param queueName   name of the queue
     * @param isDurable   true if queue is durable
     * @param isShared    true if queue is shared
     * @param queueOwner  name of the owner of the queue
     * @param isExclusive true if queue is exclusive
     * @return StorageQueue instance
     */
    public StorageQueue registerStorageQueue(String queueName,
                                             boolean isDurable,
                                             boolean isShared,
                                             String queueOwner,
                                             boolean isExclusive) {

        StorageQueue storageQueue = storageQueueMap.get(queueName);
        if (null == storageQueue) {
            storageQueue = new StorageQueue(queueName, isDurable, isShared, queueOwner, isExclusive);
            storageQueueMap.put(queueName, storageQueue);
        }
        //register subscription mbean for a durable subscription
        if (storageQueue.getName().startsWith(AndesConstants.DURABLE_SUBSCRIPTION_QUEUE_PREFIX)) {
            try {
                DurableTopicSubscriptionInformationMBean mBean = new DurableTopicSubscriptionInformationMBean(
                        storageQueue.getName());
                mBean.register();
            } catch (Exception e) {
                log.error("Unable to register subscription mbean!", e);
            }
        }
        return storageQueue;
    }

    /**
     * Remove registered storage queue from in-memory registry
     *
     * @param queueName name of the queue
     * @return removed queue. Null if nothing is removed
     * @throws AndesException
     */
    public StorageQueue removeStorageQueue(String queueName) throws AndesException {
        StorageQueue storageQueue = storageQueueMap.remove(queueName);
        storageQueue.unbindQueueFromMessageRouter();
        //unregister subscription mbean of a durable subscription
        if (queueName.startsWith(AndesConstants.DURABLE_SUBSCRIPTION_QUEUE_PREFIX) && isMbeanRegistered(queueName)) {
            Hashtable<String, String> tab = getMbeanObjectnameProp(queueName);
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(ANDES_DOMAIN, tab));
            } catch (Exception e) {
                log.error("Unable to unregister subscription mbean!", e);
            }
        }
        return storageQueue;
    }

    /**
     * Get queue instance registered by name
     *
     * @param queueName name of the queue to get
     * @return StorageQueue registered. Null if not found.
     */
    public StorageQueue getStorageQueue(String queueName) {
        return storageQueueMap.get(queueName);
    }

    /**
     * Get all storage queues registered in the broker.
     *
     * @return a list of queues
     */
    public List<StorageQueue> getAllStorageQueues() {
        return new ArrayList<>(storageQueueMap.values());
    }

    /**
     * Get a list of names of queues registered in the broker
     *
     * @return a name list of registered queues
     */
    public List<String> getAllStorageQueueNames() {
        return new ArrayList<>(storageQueueMap.keySet());
    }


    /**
     * Dump all message status of the slots owned by this slot delivery worker
     *
     * @param fileToWrite file to dump
     * @throws AndesException
     */
    public void dumpAllSlotInformationToFile(File fileToWrite) throws AndesException {
        for (StorageQueue storageQueue : storageQueueMap.values()) {
            storageQueue.dumpAllSlotInformationToFile(fileToWrite);
        }
    }

    //Check if an mbean is registered
    private boolean isMbeanRegistered(String storageQueue) {
        Hashtable<String, String> tab = getMbeanObjectnameProp(storageQueue);
        boolean isRegistered = false;
        try {
            isRegistered = ManagementFactory.getPlatformMBeanServer()
                    .isRegistered(new ObjectName(ANDES_DOMAIN, tab));
        } catch (Exception e) {
            log.error("Unable to check if an mbean is registered!", e);
        }
        return isRegistered;
    }

    private Hashtable<String, String> getMbeanObjectnameProp(String storageQueue) {
        Hashtable<String, String> tab = new Hashtable<>();
        tab.put("type", TOPIC_INFO_MBEAN_TYPE);
        tab.put("name", ObjectName.quote(storageQueue));
        return tab;
    }
}
