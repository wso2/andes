package org.wso2.andes.messageStore;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.stats.MessageCounter;
import org.wso2.andes.server.stats.MessageCounterKey;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.server.util.AndesUtils;

public class InMemoryMessageStore implements MessageStore {

    private ConcurrentHashMap<String, TreeMap<Long, AndesMessageMetadata>> messageMetadata = new ConcurrentHashMap<String, TreeMap<Long, AndesMessageMetadata>>();

    private ConcurrentHashMap<Long, List<byte[]>> messageParts = new ConcurrentHashMap<Long, List<byte[]>>();

    private HashMap<String, Long> messageCountTable = new HashMap<String, Long>();

    private boolean isMessageCoutingAllowed = ClusterResourceHolder.getInstance().getClusterConfiguration().getViewMessageCounts();

    private Map<String, Map<Long,Long[]>> messageStatuses = new HashMap<String, Map<Long, Long[]>>();

    @Override
    public void storeMessagePart(List<AndesMessagePart> part)
            throws AndesException {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteMessageParts(List<Long> messageID)
            throws AndesException {

    }

    @Override
    public AndesMessagePart getContent(String messageId, int offsetValue) throws AndesException{
        //todo:implement this
        return null;
    }

    @Override
    public void deleteMessageMetadataFromQueue(QueueAddress queueAddress, List<AndesRemovableMetadata> messagesToRemove) throws AndesException {
        TreeMap<Long, AndesMessageMetadata> queue = getQueue(getKeyFromQueueAddress(queueAddress));
        for (AndesRemovableMetadata message : messagesToRemove) {
            queue.remove(message.messageID);
            if (isMessageCoutingAllowed) {
                decrementQueueCount(message.destination, message.messageID);
            }
        }
    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(
            QueueAddress queueAddress, long startMsgID, int count)
            throws AndesException {
        TreeMap<Long, AndesMessageMetadata> queue = getQueue(getKeyFromQueueAddress(queueAddress));
        SortedMap<Long, AndesMessageMetadata> nextMessageIDs = queue.tailMap(startMsgID);

        List<AndesMessageMetadata> results = new ArrayList<AndesMessageMetadata>(count);
        int resultCount = 0;
        for (Entry<Long, AndesMessageMetadata> entry : nextMessageIDs.entrySet()) {
            results.add(entry.getValue());
            resultCount++;
            if (resultCount == count) {
                break;
            }
        }
        return results;
    }

    @Override
    public void ackReceived(List<AndesAckData> ackList) throws AndesException {
        // remove metadata
        List<AndesRemovableMetadata> messagesAddressedToQueues = new ArrayList<AndesRemovableMetadata>();
        List<AndesRemovableMetadata> messagesAddressedToTopics = new ArrayList<AndesRemovableMetadata>();
        List<Long> messageIdsOfQueueMessages = new ArrayList<Long>();
        List<Long> messageIdsOfTopicMessages = new ArrayList<Long>();

        long start = System.currentTimeMillis();
        for (AndesAckData ackData : ackList) {
            if (ackData.isTopic) {
                messagesAddressedToTopics.add(ackData.convertToRemovableMetaData());
                messageIdsOfTopicMessages.add(ackData.messageID);
            } else {
                messagesAddressedToQueues.add(ackData.convertToRemovableMetaData());
                messageIdsOfQueueMessages.add(ackData.messageID);
                OnflightMessageTracker onflightMessageTracker = OnflightMessageTracker.getInstance();
                onflightMessageTracker.updateDeliveredButNotAckedMessages(ackData.messageID);

                //decrement message count
                if (isMessageCoutingAllowed) {
                    decrementQueueCount(ackData.qName, ackData.messageID);
                }
            }

            PerformanceCounter.recordMessageRemovedAfterAck();

            if(ClusterResourceHolder.getInstance().getClusterConfiguration().isStatsEnabled()) {
                // Notify message counter of the received acknowledgement.
                MessageCounter.getInstance().updateOngoingMessageStatus(ackData.messageID, MessageCounterKey.MessageCounterType.ACKNOWLEDGED_COUNTER, ackData.qName, System.currentTimeMillis());
            }
        }

        //remove queue message metadata now
        String nodeQueueName = MessagingEngine.getMyNodeQueueName();
        QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueueName);
        deleteMessageMetadataFromQueue(nodeQueueAddress, messagesAddressedToQueues);

        //remove topic message metadata now
        String topicNodeQueueName = AndesUtils.getTopicNodeQueueName();
        QueueAddress topicNodeQueueAddress = new QueueAddress(QueueAddress.QueueType.TOPIC_NODE_QUEUE, topicNodeQueueName);
        deleteMessageMetadataFromQueue(topicNodeQueueAddress, messagesAddressedToTopics);

        //remove queue and topic message content
        //TODO: hasitha - immediate message deletion will cause problems
        deleteMessageParts(messageIdsOfTopicMessages);
        deleteMessageParts(messageIdsOfQueueMessages);

    }

    @Override
    public void addMessageMetaData(QueueAddress queueAddress,
                                   List<AndesMessageMetadata> messageList) throws AndesException {
        if (queueAddress != null) {
            TreeMap<Long, AndesMessageMetadata> queue = getQueue(getKeyFromQueueAddress(queueAddress));
            for (AndesMessageMetadata metadta : messageList) {
                queue.put(metadta.getMessageID(), metadta);
                if (isMessageCoutingAllowed) {
                    incrementQueueCount(metadta.getDestination(), metadta.getMessageID());
                }
            }
        } else {
            for (AndesMessageMetadata metadta : messageList) {
                TreeMap<Long, AndesMessageMetadata> queue = getQueue(getKeyFromQueueAddress(metadta.queueAddress));
                queue.put(metadta.getMessageID(), metadta);
                if (isMessageCoutingAllowed) {
                    incrementQueueCount(metadta.getDestination(), metadta.getMessageID());
                }
            }
        }
    }

    @Override
    public void moveMessageMetaData(QueueAddress sourceAddress,
                                    QueueAddress targetAddress, List<AndesMessageMetadata> messageList)
            throws AndesException {
        TreeMap<Long, AndesMessageMetadata> srcQueue = getQueue(getKeyFromQueueAddress(targetAddress));
        for (AndesMessageMetadata md : messageList) {
            srcQueue.remove(md.getMessageID());
        }

        if (targetAddress != null) {
            TreeMap<Long, AndesMessageMetadata> targetQueue = getQueue(getKeyFromQueueAddress(targetAddress));
            for (AndesMessageMetadata metadta : messageList) {
                targetQueue.put(metadta.getMessageID(), metadta);
            }
        } else {
            for (AndesMessageMetadata metadta : messageList) {
                TreeMap<Long, AndesMessageMetadata> targetQueue = getQueue(getKeyFromQueueAddress(metadta.queueAddress));
                targetQueue.put(metadta.getMessageID(), metadta);
            }
        }
    }

    @Override
    public long moveAllMessageMetaDataOfQueue(QueueAddress sourceAddress, QueueAddress targetAddress, String destinationQueue) throws AndesException {
        TreeMap<Long, AndesMessageMetadata> srcQueue = getQueue(getKeyFromQueueAddress(targetAddress));
        Iterator<Long> it = srcQueue.keySet().iterator();
        long ignoredFirstMessageId = Long.MAX_VALUE;
        int numberOfMessagesMoved = 0;
        long lastProcessedMessageID = 0;
        while (it.hasNext()) {
            long messageID = it.next();
            AndesMessageMetadata metadata = srcQueue.get(messageID);
            if (metadata.getDestination().equals(destinationQueue)) {
                //remove from source
                it.remove();
                //insert to target
                if (targetAddress != null) {
                    TreeMap<Long, AndesMessageMetadata> targetQueue = getQueue(getKeyFromQueueAddress(targetAddress));
                    targetQueue.put(metadata.getMessageID(), metadata);
                } else {
                    TreeMap<Long, AndesMessageMetadata> targetQueue = getQueue(getKeyFromQueueAddress(metadata.queueAddress));
                    targetQueue.put(metadata.getMessageID(), metadata);
                }
                numberOfMessagesMoved++;
            }
            lastProcessedMessageID = metadata.getMessageID();
            if (ignoredFirstMessageId > lastProcessedMessageID) {
                ignoredFirstMessageId = lastProcessedMessageID;
            }
        }
/*        log.info("moved " + numberOfMessagesMoved + "number of messages from source -"
                +sourceAddress.queueName + "- to target -"+targetAddress.queueName+"-");*/
        return lastProcessedMessageID;
    }

    @Override
    public int countMessagesOfQueue(QueueAddress queueAddress, String destinationQueueNameToMatch) throws AndesException {
        TreeMap<Long, AndesMessageMetadata> queue = getQueue(getKeyFromQueueAddress(queueAddress));
        return queue.size();
    }

    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        return messageCountTable.get(destinationQueueName);
    }

    @Override
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException{
        //todo: implement this
        return null;
    }

    @Override
    public void close() {
        //todo: implement this
    }

    @Override
    public void initializeMessageStore(DurableStoreConnection cassandraconnection) throws AndesException {

    }

    private String getKeyFromQueueAddress(QueueAddress queueAddress) {
        return new StringBuffer().append(queueAddress.queueType.getType()).append(queueAddress.queueName).toString();
    }

    private TreeMap<Long, AndesMessageMetadata> getQueue(String key) {
        TreeMap<Long, AndesMessageMetadata> queue = messageMetadata.get(key);
        if (queue == null) {
            queue = new TreeMap<Long, AndesMessageMetadata>();
            messageMetadata.put(key, queue);
        }
        return queue;
    }

    private void addMessageCounterForQueue(String destinationQueueName) {
        messageCountTable.put(destinationQueueName, 0L);
    }

    private void removeMessageCounterForQueue(String destinationQueueName) {
        messageCountTable.remove(destinationQueueName);
    }

    private void incrementQueueCount(String destinationQueueName, long incrementBy) {
        long count = messageCountTable.get(destinationQueueName) + incrementBy;
        messageCountTable.put(destinationQueueName, count);
    }

    private void decrementQueueCount(String destinationQueueName, long decrementBy) {
        long count = messageCountTable.get(destinationQueueName) - decrementBy;
        messageCountTable.put(destinationQueueName, count);
    }

    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String destination) throws CassandraDataAccessException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel) throws AndesException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void moveToDeadLetterChannel(List<AndesRemovableMetadata> messageList) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(Long limit, String columnFamilyName, String keyspace) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addMessageStatusChange(long messageId, long timeMillis, MessageCounterKey messageCounterKey) throws AndesException {
        try {
            String queueName = messageCounterKey.getQueueName();

            Map<Long, Long[]> messageStatus = messageStatuses.get(queueName);

            if (messageStatus == null) {
                messageStatus = new HashMap<Long, Long[]>();
            }

            Long[] statusTakenTime = messageStatus.get(messageId);

            if (statusTakenTime == null) {
                statusTakenTime = new Long[3];
            }

            if (MessageCounterKey.MessageCounterType.PUBLISH_COUNTER.equals(messageCounterKey.getMessageCounterType())) {
                statusTakenTime[0] = timeMillis;
            } else if (MessageCounterKey.MessageCounterType.DELIVER_COUNTER.equals(messageCounterKey.getMessageCounterType())) {
                statusTakenTime[1] = timeMillis;
            } else if (MessageCounterKey.MessageCounterType.ACKNOWLEDGED_COUNTER.equals(messageCounterKey.getMessageCounterType())) {
                statusTakenTime[2] = timeMillis;
            }

            messageStatus.put(messageId, statusTakenTime);

            messageStatuses.put(messageCounterKey.getQueueName(), messageStatus);

        } catch (Exception e) {
            throw new AndesException("Error writing incoming messages to Cassandra", e);
        }
    }

    @Override
    public Map<MessageCounterKey.MessageCounterType, Map<Long, Integer>> getMessageRates(String queueName, Long minDate, Long maxDate) throws AndesException {
        try {

            Map<MessageCounterKey.MessageCounterType, Map<Long, Integer>> total = new HashMap<MessageCounterKey.MessageCounterType, Map<Long, Integer>>();
            Map<Long, Integer> published = new TreeMap<Long, Integer>();
            Map<Long, Integer> delivered = new TreeMap<Long, Integer>();
            Map<Long, Integer> acknowledged = new TreeMap<Long, Integer>();

            Map<Long, Long[]> messageStatus = messageStatuses.get(queueName);

            for (Entry<Long, Long[]> currItrValue : messageStatus.entrySet()) {
                Long[] values = currItrValue.getValue();
                Long publishedTime = values[0];
                Long deliveredTime = values[1];
                Long acknowledgedTime = values[2];

                if (publishedTime != null) {
                    Integer currentCount = published.get(publishedTime);
                    if (currentCount == null) {
                        currentCount = 1;
                    } else {
                        currentCount += 1;
                    }

                    published.put(publishedTime, currentCount);
                }

                if (deliveredTime != null && deliveredTime != 0) {
                    Integer currentCount = delivered.get(deliveredTime);
                    if (currentCount == null) {
                        currentCount = 1;
                    } else {
                        currentCount += 1;
                    }

                    delivered.put(deliveredTime, currentCount);

                }

                if (acknowledgedTime != null && acknowledgedTime != 0) {
                    Integer currentCount = acknowledged.get(acknowledgedTime);
                    if (currentCount == null) {
                        currentCount = 1;
                    } else {
                        currentCount += 1;
                    }

                    acknowledged.put(acknowledgedTime, currentCount);

                }
            }

            return total;

        } catch (Exception e) {
            throw new AndesException("Error retrieveing message status counts", e);
        }
    }

    @Override
    public Map<Long, Map<String, String>> getMessageStatuses(String queueName, Long minDate, Long maxDate) throws AndesException {
        try {
            Map<Long, Map<String, String>> messageStatusesOut = new HashMap<Long, Map<String, String>>();

            Map<Long, Long[]> messageStatus = messageStatuses.get(queueName);


            for (Entry<Long, Long[]> currItrValue : messageStatus.entrySet()) {
                Long[] values = currItrValue.getValue();

                Long deliveredTime = values[1];
                Long acknowledgedTime = values[2];

                Map<String, String> currentMessageStatus = new HashMap<String, String>();
                currentMessageStatus.put("queue_name", queueName);
                currentMessageStatus.put(MessageCounterKey.MessageCounterType.PUBLISH_COUNTER.getType(), values[0].toString());
                currentMessageStatus.put(MessageCounterKey.MessageCounterType.DELIVER_COUNTER.getType(), values[1].toString());
                currentMessageStatus.put(MessageCounterKey.MessageCounterType.ACKNOWLEDGED_COUNTER.getType(), values[2].toString());

                if (acknowledgedTime != null) {
                    currentMessageStatus.put("message_status", MessageCounterKey.MessageCounterType.ACKNOWLEDGED_COUNTER.getType());
                } else if (deliveredTime != null) {
                    currentMessageStatus.put("message_status", MessageCounterKey.MessageCounterType.DELIVER_COUNTER.getType());
                } else {
                    currentMessageStatus.put("message_status", MessageCounterKey.MessageCounterType.PUBLISH_COUNTER.getType());
                }

                messageStatusesOut.put(currItrValue.getKey(), currentMessageStatus);

            }


            return messageStatusesOut;
        } catch (Exception e) {
            throw new AndesException(e);
        }
    }
}
