package org.wso2.andes.subscription;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQStoreException;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.server.util.QueueMessageRemovalLock;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;


public class OrphanedMessagesDueToUnsubscriptionHandler implements
		SubscriptionListener {
	private static Log log = LogFactory
			.getLog(OrphanedMessagesDueToUnsubscriptionHandler.class);

	@Override
	public void notifyClusterSubscriptionHasChanged(AndesSubscription subscrption,
			SubscriptionChange changeType) {
		// TODO Auto-generated method stub

	}

	@Override
	public void notifyLocalSubscriptionHasChanged(
			LocalSubscription subscription, SubscriptionChange changeType) {
		/**
		 * When a subscription has removed, there may be messages commited to
		 * that subscription in a node queue. If that node queue has no other
		 * subscrptions on the same queue, we need to move those messages back
		 * to global queue.
		 */
		try {
			switch (changeType) {
			case Deleted:
			case Disconnected:
				if (!subscription.isBoundToTopic()) {
					// problem happens only with Queues
					SubscriptionStore subscriptionStore = AndesContext
							.getInstance().getSubscriptionStore();
					String destination = subscription
							.getSubscribedDestination();
					Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore
							.getActiveLocalSubscribersForQueue(destination);
					if (localSubscribersForQueue.size() == 0) {
						moveMessagesBackToGlbalQueue(destination);
					}

				}
				break;
			}
		} catch (AndesException e) {
			log.error(e);
		}
	}

	public void moveMessagesBackToGlbalQueue(String destinationQueue)
			throws AndesException {
        try {
            Thread.sleep(10*1000);
        } catch (InterruptedException e) {
            //silently ignore
        }
        log.info("Moving messages for " + destinationQueue
				+ " from node queue " + MessagingEngine.getMyNodeQueueName()
				+ " back to global queue");
		String globalQueueName = AndesUtils
				.getGlobalQueueNameForDestinationQueue(destinationQueue);
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
			try {
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
                QueueAddress sourceQueueAddress =  new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE,nodeQueueName);
                String globalQueue = AndesUtils.getGlobalQueueNameForDestinationQueue(destinationQueue);
                QueueAddress targetQueueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE,globalQueue);

                List<AndesMessageMetadata>  messageList = messageStore.getNextNMessageMetadataFromQueue(sourceQueueAddress, lastProcessedMessageID, 40);
                while (messageList.size() != 0) {
                    Iterator<AndesMessageMetadata> metadataIterator = messageList.iterator();
                    while (metadataIterator.hasNext()) {
                        AndesMessageMetadata metadata = metadataIterator.next();

                        /**
                         * check and move metadata of messages of relevant destination queue to global queue
                         */
                        String destinationQueueAddressed = metadata.getDestination();
                        if(destinationQueueAddressed.equals(destinationQueue)) {
                            OnflightMessageTracker.getInstance()
                                    .scheduleToDeleteMessageFromReadMessageFromNodeQueueMap(
                                            metadata.getMessageID());
                        } else{
                            metadataIterator.remove();
                        }
                        lastProcessedMessageID = metadata.getMessageID();
                    }
                    messageStore.moveMessageMetaData(sourceQueueAddress,targetQueueAddress,messageList);
                    numberOfMessagesMoved+=messageList.size();
                    messageList = messageStore.getNextNMessageMetadataFromQueue(sourceQueueAddress, lastProcessedMessageID, 40);
                }

                log.info("Moved " + numberOfMessagesMoved
                        + " Number of Messages Addressed to Queue "
                        + destinationQueue + " from Node Queue "
                        + nodeQueueName + "to Global Queue " + globalQueue);

                if(lastProcessedMessageID != 0) {
                    ignoredFirstMessageId = lastProcessedMessageID;
                }

                //CassandraMessageStore messageStore = ClusterResourceHolder.getInstance().getCassandraMessageStore();

/*				int numberOfMessagesMoved = 0;
				long lastProcessedMessageID = 0;
				List<CassandraQueueMessage> messages = messageStore
						.getMessagesFromNodeQueue(nodeQueueName, 40,
								lastProcessedMessageID);
				while (messages.size() != 0) {

					List<CassandraQueueMessage> messageToMoved = new ArrayList<CassandraQueueMessage>();
					for (CassandraQueueMessage msg : messages) {
						String destinationQueueName = msg
								.getDestinationQueueName();
						// move messages addressed to this destination queue
						// only
						if (destinationQueueName.equals(destinationQueue)) {
							messageToMoved.add(msg);
							// numberOfMessagesMoved++;
							// messageStore.removeMessageFromNodeQueue(nodeQueueName,
							// msg.getMessageId());
							// try {
							// //when adding back to global queue we mark it as
							// an message that was already came in (as un-acked)
							// //we do not evaluate if message addressed queue
							// is bound to topics as it is not used. Just pass
							// false for that.
							// //for message properties just pass default values
							// as they will not be written to Cassandra again.
							// //we should add it to relevant globalQueue also
							// //even if messages are addressed to durable
							// subscriptions we need to add (force)
							//
							// messageStore.addMessageToGlobalQueue(globalQueue,
							// msg.getNodeQueue(), msg.getMessageId(),
							// msg.getMessage(), false, 0, false, true);
							// if (log.isDebugEnabled()) {
							// String header =
							// (String)msg.getAmqMessage().getMessageHeader().getHeader("msgID");
							// log.debug("TRACING>> DCESM- Moving message-"+
							// header==null?"":header +
							// "- with MessageID-"+msg.getMessageId() +
							// "-from NQ " + nodeQueueName +
							// " to GQ-"+globalQueue);
							// }
							// } catch (Exception e) {
							// log.error(e);
							// }
						}
						messageStore.moveMessageMetaData(new QueueAddress(CassandraConstants.NODE_QUEUES_COLUMN_FAMILY,nodeQueueName), 
								messageToMoved);
						numberOfMessagesMoved = numberOfMessagesMoved
								+ messageToMoved.size();
						lastProcessedMessageID = msg.getMessageId();
						if (ignoredFirstMessageId > lastProcessedMessageID) {
							ignoredFirstMessageId = lastProcessedMessageID;
						}
						OnflightMessageTracker.getInstance()
								.scheduleToDeleteMessageFromReadMessageFromNodeQueueMap(
										lastProcessedMessageID);
					}
					messages = messageStore.getMessagesFromNodeQueue(
							nodeQueueName, 40, lastProcessedMessageID);
				}*/

                /*OnflightMessageTracker.getInstance()
                        .scheduleToDeleteMessageFromReadMessageFromNodeQueueMap(
                                lastProcessedMessageId);*/

				// remove any in-memory messages accumulated for the queue
				ClusterResourceHolder.getInstance().getClusterManager()
						.removeInMemoryMessagesAccumulated(destinationQueue);
/*
				log.info("Moved " + numberOfMessagesMoved
						+ " Number of Messages Addressed to Queue "
						+ destinationQueue + " from Node Queue "
						+ nodeQueueName + "to Global Queue");*/
				updateQueueDeliveryInformation(destinationQueue,
						ignoredFirstMessageId);
			} catch (AndesException e) {
				new AMQStoreException("Error removing messages addressed to "
						+ destinationQueue + "from relevant node queue", e);
			}

		}

	}
	
	private void updateQueueDeliveryInformation(String queueName, long ignoredFirstMessageID) throws AndesException{
        QueueDeliveryWorker queueDeliveryWorker =  ClusterResourceHolder.getInstance().getQueueDeliveryWorker();
        if(queueDeliveryWorker == null) {
            return;
        }
        QueueDeliveryWorker.QueueDeliveryInfo queueDeliveryInfo = queueDeliveryWorker.getQueueDeliveryInfo(queueName);
        if(queueDeliveryInfo == null) {
            return;
        }
        queueDeliveryInfo. setIgnoredFirstMessageId(ignoredFirstMessageID);
        queueDeliveryInfo.setNeedToReset(true);
        log.debug("TRACING>> DCESM-updateQueueDeliveryInformation >> Updated the QDI Object of queue-"+queueName+"-to ignoredFirstMessageID = " + ignoredFirstMessageID);
    }

}
