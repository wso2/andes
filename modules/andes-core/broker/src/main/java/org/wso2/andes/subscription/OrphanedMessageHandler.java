package org.wso2.andes.subscription;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.server.util.QueueMessageRemovalLock;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * This class will handle message moving to NQ to GQ and updating flags
 * where to read from now. This listener is interested in all subscription changes
 */
public class OrphanedMessageHandler implements SubscriptionListener {

    private static Log log = LogFactory
            .getLog(OrphanedMessageHandler.class);


    @Override
    public void handleClusterSubscriptionsChanged(AndesSubscription subscription, SubscriptionChange changeType) throws AndesException {

    }

    @Override
    public void handleLocalSubscriptionsChanged(LocalSubscription subscription, SubscriptionChange changeType) throws AndesException {
        /**
         * When a subscription has removed, there may be messages committed to
         * that subscription in a node queue. If that node queue has no other
         * subscriptions on the same queue, we need to move those messages back
         * to global queue.
         */
        switch (changeType) {
            case Deleted:
                if (!subscription.isBoundToTopic()) {
                    // problem happens only with Queues
                    SubscriptionStore subscriptionStore = AndesContext
                            .getInstance().getSubscriptionStore();
                    String destination = subscription
                            .getSubscribedDestination();
                    Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore
                            .getActiveLocalSubscribers(destination, false);
                    if (localSubscribersForQueue.size() == 0) {
                        moveMessagesBackToGlobalQueue(destination);
                    }

                }
                break;
            case Disconnected:
                if (!subscription.isBoundToTopic()) {
                    // problem happens only with Queues
                    SubscriptionStore subscriptionStore = AndesContext
                            .getInstance().getSubscriptionStore();
                    String destination = subscription
                            .getSubscribedDestination();
                    Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore
                            .getActiveLocalSubscribers(destination, false);
                    if (localSubscribersForQueue.size() == 0) {
                        moveMessagesBackToGlobalQueue(destination);
                    }

                }
                break;
        }

        //if running in standalone mode short-circuit cluster notification
        if (!AndesContext.getInstance().isClusteringEnabled()) {
            handleClusterSubscriptionsChanged(new BasicSubscription(subscription.encodeAsStr()), changeType);
        }
    }

    private void moveMessagesBackToGlobalQueue(String destinationQueue)
            throws AndesException {
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            //silently ignore
        }
        log.info("Moving messages for " + destinationQueue
                + " from node queue " + MessagingEngine.getMyNodeQueueName()
                + " back to global queue");
        String globalQueueName = AndesUtils
                .getGlobalQueueNameForDestinationQueue(destinationQueue);
        //TODO:should we stop GQ worker temporarily here?why reset?
        ClusterResourceHolder.getInstance().getClusterManager()
                .getGlobalQueueManager()
                .resetGlobalQueueWorkerIfRunning(globalQueueName);
        // if in clustered mode copy messages addressed to that queue back to
        // global queue
        if (AndesContext.getInstance().isClusteringEnabled()) {
            handleMessageRemoval(destinationQueue);
        }
    }

    private void handleMessageRemoval(String destinationQueue) throws AndesException {
        synchronized (QueueMessageRemovalLock.class) {
            //we wait a bit for any on-flight messages to settle
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                // silently ignore
            }
            // move messages from node queue to respective global queues
            MessageStore messageStore = MessagingEngine.getInstance().getDurableMessageStore();
            String nodeQueueName = MessagingEngine.getMyNodeQueueName();
            long ignoredFirstMessageId = Long.MAX_VALUE;
            int numberOfMessagesMoved = 0;
            long lastProcessedMessageID = 0;
            QueueAddress sourceQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueueName);
            String globalQueue = AndesUtils.getGlobalQueueNameForDestinationQueue(destinationQueue);
            QueueAddress targetQueueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE, globalQueue);

            List<AndesMessageMetadata> messageList = messageStore.getNextNMessageMetadataFromQueue(sourceQueueAddress, lastProcessedMessageID, 40);
            while (messageList.size() != 0) {
                Iterator<AndesMessageMetadata> metadataIterator = messageList.iterator();
                while (metadataIterator.hasNext()) {
                    AndesMessageMetadata metadata = metadataIterator.next();

                    /**
                     * check and move metadata of messages of relevant destination queue to global queue
                     */
                    String destinationQueueAddressed = metadata.getDestination();
                    if (destinationQueueAddressed.equals(destinationQueue)) {
                        OnflightMessageTracker.getInstance()
                                .scheduleToDeleteMessageFromReadMessageFromNodeQueueMap(
                                        metadata.getMessageID());
                    } else {
                        metadataIterator.remove();
                    }
                    lastProcessedMessageID = metadata.getMessageID();
                }
                messageStore.moveMessageMetaData(sourceQueueAddress, targetQueueAddress, messageList);
                numberOfMessagesMoved += messageList.size();
                messageList = messageStore.getNextNMessageMetadataFromQueue(sourceQueueAddress, lastProcessedMessageID, 40);
            }

            log.info("Moved " + numberOfMessagesMoved
                    + " Number of Messages Addressed to Queue "
                    + destinationQueue + " from Node Queue "
                    + nodeQueueName + "to Global Queue " + globalQueue);

            if (lastProcessedMessageID != 0) {
                ignoredFirstMessageId = lastProcessedMessageID;
            }

            ClusterResourceHolder.getInstance().getClusterManager()
                    .removeInMemoryMessagesAccumulated(destinationQueue);

            updateQueueDeliveryInformation(destinationQueue,
                    ignoredFirstMessageId);

        }

    }

    /**
     * This will update flags from where to read messages from now
     *
     * @param queueName             queue name whose messages were moved
     * @param ignoredFirstMessageID first id to read from now
     * @throws AndesException
     */
    private void updateQueueDeliveryInformation(String queueName, long ignoredFirstMessageID) throws AndesException {
        QueueDeliveryWorker queueDeliveryWorker = ClusterResourceHolder.getInstance().getQueueDeliveryWorker();
        if (queueDeliveryWorker == null) {
            return;
        }
        QueueDeliveryWorker.QueueDeliveryInfo queueDeliveryInfo = queueDeliveryWorker.getQueueDeliveryInfo(queueName);
        if (queueDeliveryInfo == null) {
            return;
        }
       // queueDeliveryInfo.setIgnoredFirstMessageId(ignoredFirstMessageID);
       // queueDeliveryInfo.setNeedToReset(true);
        log.debug("TRACING>> DCESM-updateQueueDeliveryInformation >> Updated the QDI Object of queue-" + queueName + "-to ignoredFirstMessageID = " + ignoredFirstMessageID);
    }
}
