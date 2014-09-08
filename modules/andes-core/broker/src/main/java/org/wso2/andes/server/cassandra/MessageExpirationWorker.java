package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.messageStore.CassandraConstants;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.configuration.ClusterConfiguration;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;

import java.util.List;

/**
 * This thread will keep looking for expired messages within the broker and remove them.
 */
public class MessageExpirationWorker extends Thread {

    private static Log log = LogFactory.getLog(MessageExpirationWorker.class);
    private boolean working = false;

    //references
    private MessageStore messageStore;

    //configurations
    private final int workerWaitInterval;
    private final int messageBatchSize;
    private final boolean saveExpiredToDLC;

    //for measuring purposes
    private long failureCount = 0l;
    private long iterations = 0l;

    public MessageExpirationWorker() {

        ClusterConfiguration clusterConfiguration = ClusterResourceHolder.getInstance().getClusterConfiguration();

        workerWaitInterval = clusterConfiguration.getJMSExpirationCheckInterval();
        messageBatchSize = clusterConfiguration.getExpirationMessageBatchSize();
        saveExpiredToDLC = clusterConfiguration.getSaveExpiredToDLC();

        if (ClusterResourceHolder.getInstance().getClusterConfiguration().isInMemoryMode()) {
            messageStore = MessagingEngine.getInstance().getInMemoryMessageStore();
        } else {
            messageStore = MessagingEngine.getInstance().getDurableMessageStore();
        }

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
                    List<AndesRemovableMetadata> expiredMessages = messageStore.getExpiredMessages(messageBatchSize);
//                            CQLDataAccessHelper.getExpiredMessages(messageBatchSize, CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY,CassandraConstants.KEYSPACE);

                    if (expiredMessages == null || expiredMessages.size() == 0 )  {

                        sleepForWaitInterval(workerWaitInterval);

                    } else {
                        messageStore.deleteMessages(expiredMessages, saveExpiredToDLC);
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
