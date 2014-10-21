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

package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.configuration.BrokerConfiguration;

import java.util.List;

/**
 * This thread will keep looking for expired messages within the broker and remove them.
 */
public class MessageExpirationWorker extends Thread {

    private static Log log = LogFactory.getLog(MessageExpirationWorker.class);
    private volatile boolean working = false;

    //configurations
    private final int workerWaitInterval;
    private final int messageBatchSize;
    private final boolean saveExpiredToDLC;

    //for measuring purposes
    private long failureCount = 0l;
    private long iterations = 0l;

    public MessageExpirationWorker() {

        BrokerConfiguration clusterConfiguration = ClusterResourceHolder.getInstance().getClusterConfiguration();

        workerWaitInterval = clusterConfiguration.getJMSExpirationCheckInterval();
        messageBatchSize = clusterConfiguration.getExpirationMessageBatchSize();
        saveExpiredToDLC = clusterConfiguration.getSaveExpiredToDLC();

        this.start();
        this.setWorking();
    }

    @Override
    public void run() {

        int failureCount = 0;

        while (true) {
            if (working) {
                try {
                    //Get Expired messages
                    List<AndesRemovableMetadata> expiredMessages = MessagingEngine.getInstance().getExpiredMessages(messageBatchSize);

                    if (expiredMessages == null || expiredMessages.size() == 0 )  {

                        sleepForWaitInterval(workerWaitInterval);

                    } else {
                        MessagingEngine.getInstance().deleteMessages(expiredMessages, saveExpiredToDLC);
                        sleepForWaitInterval(workerWaitInterval);
                    }

                } catch (Throwable e) {
                    /**
                     * When there is a error, we will wait to avoid looping.
                     */
                    long waitTime = workerWaitInterval;
                    failureCount++;
                    long faultWaitTime = Math.max(waitTime * 5, failureCount * waitTime);
                    try {
                        Thread.sleep(faultWaitTime);
                    } catch (InterruptedException e1) {
                        //silently ignore
                    }
                    log.error("Error running Message Expiration Checker" + e.getMessage(), e);
                }
            } else {
                sleepForWaitInterval(workerWaitInterval);
            }
        }
    }

    /**
     * get if Message Expiration Worker is active
     *
     * @return isWorking
     */
    public boolean isWorking() {
        return working;
    }

    /**
     * set Message Expiration Worker active
     */
    public void setWorking() {
        working = true;
    }

    public void stopWorking() {
        working = false;
        log.info("Shutting down message expiration checker.");
    }

    private void sleepForWaitInterval(int sleepInterval) {
        try {
            Thread.sleep(sleepInterval);
        } catch (InterruptedException e) {
            //ignored
        }
    }

    public static boolean isExpired(Long msgExpiration) {
        if (msgExpiration > 0) {
            return (System.currentTimeMillis() > msgExpiration) ;
        } else {
            return false;
        }
    }
}
