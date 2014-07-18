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
package org.wso2.andes.server.cluster;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.QueueAddress;
import org.wso2.andes.server.ClusterResourceHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * <code>GlobalQueueManager</code> Manage the Global queues
 */
public class GlobalQueueManager {

    private MessageStore messageStore;


    private Map<String,GlobalQueueWorker> queueWorkerMap =
            new ConcurrentHashMap<String,GlobalQueueWorker>();


    private ExecutorService globalQueueManagerexecutorService;

    private static Log log = LogFactory.getLog(GlobalQueueManager.class);

    public GlobalQueueManager(MessageStore store) {
        this.messageStore = store;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("GlobalQueueManager-%d").build();
        this.globalQueueManagerexecutorService = Executors.newCachedThreadPool(namedThreadFactory);
    }

    public void scheduleWorkForGlobalQueue(String queueName) {
        if(queueWorkerMap.containsKey(queueName)) {
            startWorker(queueName);
            return;
        }
        int batchSize = ClusterResourceHolder.getInstance().getClusterConfiguration().
                getGlobalQueueWorkerMessageBatchSize();
        GlobalQueueWorker worker = new GlobalQueueWorker(queueName, messageStore,batchSize);
        worker.setRunning(true);
        queueWorkerMap.put(queueName, worker);
        log.info("Starting Global Queue Worker for Global Queue : " + queueName);
        globalQueueManagerexecutorService.execute(worker);
    }

    /**
     * get the list of global Queue names on which workers are running in this node
     * @return  list of global queue names
     */
    public List<String> getWorkerRunningGlobalQueueNames() {
        List<String> globalQueueNames = new ArrayList<String>();
        globalQueueNames.addAll(this.queueWorkerMap.keySet());
        return globalQueueNames;
    }

    /**
     * returns worker for the given global queue name if exists, otherwise null
     * @param globalQueueName name of global queue
     * @return  worker
     */
    public GlobalQueueWorker getWorkerForGlobalQueueName(String globalQueueName) {
        GlobalQueueWorker gqw = null;
        gqw = this.queueWorkerMap.get(globalQueueName);
        return gqw;
    }

    /**
     * Instructs all global queue workers running in this node to reset themselves
     */
    public void resetGlobalQueueWorkerIfRunning(String globalQueueName) {
        List<String> globalQueues = getWorkerRunningGlobalQueueNames();
        if(globalQueues.contains(globalQueueName)) {
            queueWorkerMap.get(globalQueueName).resetMessageReading();
        }
    }

    public void removeWorker(String queueName) {

        log.info("Removing Global Queue Worker for Global Queue : " + queueName);
        GlobalQueueWorker worker = queueWorkerMap.get(queueName);
        if (worker != null) {
            worker.setRunning(false);
            queueWorkerMap.remove(queueName);
        }
    }

    public void stopWorker(String queueName) {

        log.debug("Stopping Global Queue Worker for Queue Locally : " + queueName);
        GlobalQueueWorker worker = queueWorkerMap.get(queueName);
        if (worker != null && worker.isRunning()) {
            worker.setRunning(false);
        }
    }

    public void startWorker(String queueName) {

        GlobalQueueWorker worker = queueWorkerMap.get(queueName);
        if (worker != null && !worker.isRunning()) {
            log.debug("Starting Global Queue Worker for Queue Locally: " + queueName);
            worker.setRunning(true);
        }
    }

    public int getMessageCountOfGlobalQueue(String globalQueueName) throws AndesException {
        QueueAddress queueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE,globalQueueName);
        return messageStore.countMessagesOfQueue(queueAddress, null);
    }

//    public int getSubscriberCount(String queueName) throws Exception{
//        return ClusterResourceHolder.getInstance().getClusterManager().getZkNodes().size();
//    }

    public void removeAllQueueWorkersLocally() throws Exception {

        log.info("Stopping all locally existing global queue workers");
        Set<String> queueList = queueWorkerMap.keySet();
        for(String queue :queueList) {
            removeWorker(queue);
        }
    }

    public void stopAllQueueWorkersLocally() {

        Set<String> queueList = queueWorkerMap.keySet();
        for(String queue :queueList) {
            stopWorker(queue);
        }
    }

    public void startAllQueueWorkersLocally() {

        Set<String> queueList = queueWorkerMap.keySet();
        for(String queue :queueList) {
            startWorker(queue);
        }
    }

}
