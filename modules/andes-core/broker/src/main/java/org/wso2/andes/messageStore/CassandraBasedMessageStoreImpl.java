package org.wso2.andes.messageStore;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.CassandraConnection;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.stats.MessageCounterKey;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.server.store.util.CassandraDataAccessHelper;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor.PendingJob;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.wso2.andes.messageStore.CassandraConstants.*;

public class CassandraBasedMessageStoreImpl implements org.wso2.andes.kernel.MessageStore {
    private static Log log = LogFactory.getLog(CassandraBasedMessageStoreImpl.class);

    private Keyspace keyspace;
    private StringSerializer stringSerializer = StringSerializer.get();
    private LongSerializer longSerializer = LongSerializer.get();
    private ConcurrentSkipListMap<Long, Long> contentDeletionTasks = new ConcurrentSkipListMap<Long, Long>();
    private MessageContentRemoverTask messageContentRemoverTask;
    private boolean isMessageCoutingAllowed;
    private Cluster cluster;

    public CassandraBasedMessageStoreImpl() {
        isMessageCoutingAllowed = ClusterResourceHolder.getInstance().getClusterConfiguration().getViewMessageCounts();
    }

    public void initializeMessageStore(DurableStoreConnection cassandraconnection) throws AndesException {
        initializeCassandraMessageStore(cassandraconnection);
        messageContentRemoverTask = new MessageContentRemoverTask(ClusterResourceHolder.getInstance().getClusterConfiguration().
                getContentRemovalTaskInterval(), contentDeletionTasks, this, cassandraconnection);
        messageContentRemoverTask.start();
    }

    private void initializeCassandraMessageStore(DurableStoreConnection cassandraconnection) throws AndesException {
        try {
            cluster = ((CassandraConnection) cassandraconnection).getCluster();
            keyspace = ((CassandraConnection) cassandraconnection).getKeySpace();
            createColumnFamilies();
        } catch (CassandraDataAccessException e) {
            log.error("Error while initializing cassandra message store", e);
            throw new AndesException(e);
        }

    }

    /**
     * Create a cassandra column families for andes usage
     *
     * @throws CassandraDataAccessException
     */
    private void createColumnFamilies() throws CassandraDataAccessException {
        CassandraDataAccessHelper.createColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY, KEYSPACE, this.cluster, INTEGER_TYPE);
        CassandraDataAccessHelper.createColumnFamily(NODE_QUEUES_COLUMN_FAMILY, KEYSPACE, this.cluster, LONG_TYPE);
        CassandraDataAccessHelper.createColumnFamily(GLOBAL_QUEUES_COLUMN_FAMILY, KEYSPACE, this.cluster, LONG_TYPE);
        CassandraDataAccessHelper.createColumnFamily(PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY, KEYSPACE, this.cluster, LONG_TYPE);
        CassandraDataAccessHelper.createColumnFamily(MESSAGE_STATUS_CHANGE_COLUMN_FAMILY, KEYSPACE, this.cluster, LONG_TYPE);
        CassandraDataAccessHelper.createCounterColumnFamily(MESSAGE_COUNTERS_COLUMN_FAMILY, KEYSPACE, this.cluster);
    }

    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        try {
            Mutator<String> messageMutator = HFactory.createMutator(keyspace, stringSerializer);
            for (AndesMessagePart part : partList) {
                final String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX
                        + part.getMessageID();
                CassandraDataAccessHelper.addIntegerByteArrayContentToRaw(MESSAGE_CONTENT_COLUMN_FAMILY,
                        rowKey, part.getOffSet(), part.getData(), messageMutator, false);
                System.out.println("STORE >> message part id" + part.getMessageID() + " offset " + part.getOffSet());
            }
            messageMutator.execute();
        } catch (CassandraDataAccessException e) {
            //TODO handle Cassandra failures
            //When a error happened, we should remember that and stop accepting messages
            log.error(e);
            throw new AndesException("Error in adding the message part to the store", e);
        }
    }

    /**
     * get andes message meta-data staring from startMsgID + 1
     *
     * @param queueAddress source address to read metadata
     * @param startMsgID   starting message ID
     * @param count        message count to read
     * @return list of andes message meta-data
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(QueueAddress queueAddress, long startMsgID, int count) throws AndesException {

        try {
            ColumnSlice<Long, byte[]> messagesColumnSlice = CassandraDataAccessHelper.getMessagesFromQueue(queueAddress.queueName,
                    getColumnFamilyFromQueueAddress(queueAddress), keyspace, startMsgID, count);
            //if list is empty return
            if (messagesColumnSlice == null || messagesColumnSlice.getColumns().size() == 0) {
                return Collections.emptyList();
            }
            List<AndesMessageMetadata> metadataList = new ArrayList<AndesMessageMetadata>();
            //combining metadata with message properties create QueueEntries
            for (Object column : messagesColumnSlice.getColumns()) {
                if (column instanceof HColumn) {
                    long messageId = ((HColumn<Long, byte[]>) column).getName();
                    byte[] value = ((HColumn<Long, byte[]>) column).getValue();
                    metadataList.add(new AndesMessageMetadata(messageId, value, true));
                }
            }
            return metadataList;
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }

    }

    public void duplicateMessageContent(long messageId, long messageIdOfClone) throws AndesException {
        String originalRowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId;
        String cloneMessageKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageIdOfClone;
        try {

            int tryCount = 0;
            //read from store
            ColumnQuery columnQuery = HFactory.createColumnQuery(keyspace, stringSerializer,
                    integerSerializer, byteBufferSerializer);
            columnQuery.setColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY);
            columnQuery.setKey(originalRowKey.trim());

            SliceQuery<String, Integer, ByteBuffer> query = HFactory.createSliceQuery(keyspace, stringSerializer, integerSerializer, byteBufferSerializer);
            query.setColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY).setKey(originalRowKey).setRange(0, Integer.MAX_VALUE, false, Integer.MAX_VALUE);
            QueryResult<ColumnSlice<Integer, ByteBuffer>> result = query.execute();

            //if there are values duplicate them

            if (!result.get().getColumns().isEmpty()) {
                Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
                for (HColumn<Integer, ByteBuffer> column : result.get().getColumns()) {
                    int offset = column.getName();
                    final byte[] chunkData = bytesArraySerializer.fromByteBuffer(column.getValue());
                    CassandraDataAccessHelper.addIntegerByteArrayContentToRaw(MESSAGE_CONTENT_COLUMN_FAMILY, cloneMessageKey,
                            offset, chunkData, mutator, false);
                    System.out.println("DUPLICATE>> new id " + messageIdOfClone + " cloned from id " + messageId + " offset" + offset);

                }
                mutator.execute();
            } else {
                tryCount += 1;
                if (tryCount == 3) {
                    throw new AndesException("Original Content is not written. Cannot duplicate content. Tried 3 times");
                }
                try {
                    Thread.sleep(20 * tryCount);
                } catch (InterruptedException e) {
                    //silently ignore
                }

                this.duplicateMessageContent(messageId, messageIdOfClone);
            }

        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }
    }


    public AndesMessagePart getContent(String messageId, int offsetValue) throws AndesException{
        log.info("GET CONTENT >> id " + messageId + " offset " + offsetValue);
        byte[] content;
        AndesMessagePart messagePart;
        try {

            String rowKey = "mid" + messageId;
            ColumnQuery columnQuery = HFactory.createColumnQuery(keyspace, stringSerializer,
                    integerSerializer, byteBufferSerializer);
            columnQuery.setColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY);
            columnQuery.setKey(rowKey.trim());
            columnQuery.setName(offsetValue);

            QueryResult<HColumn<Integer, ByteBuffer>> result = columnQuery.execute();
            HColumn<Integer, ByteBuffer> column = result.get();
            if (column != null) {
                int offset = column.getName();
                content = bytesArraySerializer.fromByteBuffer(column.getValue());

                messagePart = new AndesMessagePart();
                messagePart.setData(content);
                messagePart.setMessageID(Long.parseLong(messageId));
                messagePart.setOffSet(offset);
                messagePart.setDataLength(content.length);
            } else {
                throw new RuntimeException("Unexpected Error , content already deleted for message id :" + messageId);
            }

        } catch (Exception e) {
            log.error("Error in reading content messageID= " + messageId + " offset=" + offsetValue, e);
            throw  new AndesException("Error in reading content messageID=" + messageId + " offset="+offsetValue, e);
        }
        return messagePart;
    }

    public void deleteMessageParts(long messageID, byte[] data) {
    }

    public void ackReceived(List<AndesAckData> ackList) throws AndesException {
        try {

            List<AndesRemovableMetadata> messagesAddressedToQueues = new ArrayList<AndesRemovableMetadata>();
            List<AndesRemovableMetadata> messagesAddressedToTopics = new ArrayList<AndesRemovableMetadata>();

            for (AndesAckData ackData : ackList) {
                if (ackData.isTopic) {

                    messagesAddressedToTopics.add(ackData.convertToRemovableMetaData());

                    //schedule to remove queue and topic message content
                    long timeGapConfigured = ClusterResourceHolder.getInstance().
                            getClusterConfiguration().getPubSubMessageRemovalTaskInterval() *1000000;
                    addContentDeletionTask(System.nanoTime() + timeGapConfigured,ackData.messageID);

                } else {

                    messagesAddressedToQueues.add(ackData.convertToRemovableMetaData());
                    OnflightMessageTracker onflightMessageTracker = OnflightMessageTracker.getInstance();
                    onflightMessageTracker.updateDeliveredButNotAckedMessages(ackData.messageID);

                    //decrement message count
                    if (isMessageCoutingAllowed) {
                        decrementQueueCount(ackData.qName, 1);
                    }

                    //schedule to remove queue and topic message content
                    addContentDeletionTask(System.nanoTime(),ackData.messageID);
                }

                PerformanceCounter.recordMessageRemovedAfterAck();
            }

            //remove queue message metadata now
            String nodeQueueName = MessagingEngine.getMyNodeQueueName();
            QueueAddress nodeQueueAddress = new QueueAddress
                    (QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueueName);
            deleteMessageMetadataFromQueue(nodeQueueAddress, messagesAddressedToQueues);

            //remove topic message metadata now
            String topicNodeQueueName = AndesUtils.getTopicNodeQueueName();
            QueueAddress topicNodeQueueAddress = new QueueAddress
                    (QueueAddress.QueueType.TOPIC_NODE_QUEUE, topicNodeQueueName);
            deleteMessageMetadataFromQueue(topicNodeQueueAddress, messagesAddressedToTopics);

        } catch (CassandraDataAccessException e) {
            //TODO: hasitha - handle Cassandra failures
            log.error(e);
            throw new AndesException("Error in handling acknowledgments ", e);
        }
    }

    private void addContentDeletionTask(long currentNanoTime, long messageID) {
        contentDeletionTasks.put(currentNanoTime, messageID);
    }

    public void addMessageMetaData(QueueAddress queueAddress, List<AndesMessageMetadata> messageList) throws AndesException {
        try {
            Mutator<String> messageMutator = HFactory.createMutator(keyspace, stringSerializer);
            HashMap<String, Integer> incomingMessagesMap = new HashMap<String, Integer>();
            for (AndesMessageMetadata md : messageList) {

                //TODO Stop deleting from QMD_ROW_NAME and GLOBAL_QUEUE_LIST_COLUMN_FAMILY

                //TODO this is to avoid having to group messages in AlternatingCassandraWriter
                if (queueAddress == null) {
                    queueAddress = md.queueAddress;
                }
                CassandraDataAccessHelper.addMessageToQueue(getColumnFamilyFromQueueAddress(queueAddress), queueAddress.queueName,
                        md.getMessageID(), md.getMetadata(), messageMutator, false);
                if (incomingMessagesMap.get(md.getDestination()) == null) {
                    incomingMessagesMap.put(md.getDestination(), 1);
                } else {
                    incomingMessagesMap.put(md.getDestination(), incomingMessagesMap.get(md.getDestination()) + 1);
                }

                PerformanceCounter.recordIncomingMessageWrittenToCassandra();
                log.info("Wrote message " + md.getMessageID() + " to Global Queue " + queueAddress.queueName);

            }
            long start = System.currentTimeMillis();
            messageMutator.execute();

            PerformanceCounter.recordIncomingMessageWrittenToCassandraLatency((int) (System.currentTimeMillis() - start));

            if (isMessageCoutingAllowed) {
                for (AndesMessageMetadata md : messageList) {
                    incrementQueueCount(md.getDestination(), 1);
                }
            }

            // Client waits for these message ID to be written, this signal those, if there is a error
            //we will not signal and client who tries to close the connection will timeout.
            //We can do this better, but leaving this as is or now.
            for (AndesMessageMetadata md : messageList) {
                PendingJob jobData = md.getPendingJobsTracker().get(md.getMessageID());
                if (jobData != null) {
                    jobData.semaphore.release();
                }
            }
        } catch (Exception e) {
            //TODO handle Cassandra failures
            //TODO may be we can write those message to a disk, or do something. Currently we will just loose them
            log.error("Error writing incoming messages to Cassandra", e);
            throw new AndesException("Error writing incoming messages to Cassandra", e);
        }
    }

    /**
     * Here if target address is null, we will try to find the address from each AndesMessageMetadata
     */
    public void moveMessageMetaData(QueueAddress sourceAddress, QueueAddress targetAddress, List<AndesMessageMetadata> messageList) throws AndesException {
        Mutator<String> messageMutator = HFactory.createMutator(keyspace, stringSerializer);
        try {
            for (AndesMessageMetadata messageMetaData : messageList) {
                if (targetAddress == null) {
                    targetAddress = messageMetaData.queueAddress;
                }
                CassandraDataAccessHelper.deleteLongColumnFromRaw(getColumnFamilyFromQueueAddress(sourceAddress), sourceAddress.queueName, messageMetaData.getMessageID(), messageMutator, false);
                CassandraDataAccessHelper.addMessageToQueue(getColumnFamilyFromQueueAddress(targetAddress), targetAddress.queueName,
                        messageMetaData.getMessageID(), messageMetaData.getMetadata(), messageMutator, false);
                if (log.isDebugEnabled()) {
                    log.debug("TRACING>> CMS-Removing messageID-" + messageMetaData.getMessageID() + "-from source Queue-" + sourceAddress.queueName
                            + "- to target Queue " + targetAddress.queueName);
                }
            }
            messageMutator.execute();
        } catch (CassandraDataAccessException e) {
            log.error("Error in moving messages ", e);
            throw new AndesException("Error in moving messages from source -" + getColumnFamilyFromQueueAddress(sourceAddress) + " - to target -" + getColumnFamilyFromQueueAddress(targetAddress), e);
        }

    }

    public long moveAllMessageMetaDataOfQueue(QueueAddress sourceAddress, QueueAddress targetAddress, String destinationQueue) throws AndesException {
        Mutator<String> messageMutator = HFactory.createMutator(keyspace, stringSerializer);
        try {
            long ignoredFirstMessageId = Long.MAX_VALUE;
            int numberOfMessagesMoved = 0;
            long lastProcessedMessageID = 0;
            List<AndesMessageMetadata> messageList = getNextNMessageMetadataFromQueue(sourceAddress, lastProcessedMessageID, 40);
            while (messageList.size() != 0) {
                int numberOfMessagesMovedInIteration = 0;
                for (AndesMessageMetadata messageMetaData : messageList) {
                    if (messageMetaData.getDestination().equals(destinationQueue)) {
                        if (targetAddress == null) {
                            targetAddress = messageMetaData.queueAddress;
                        }
                        CassandraDataAccessHelper.deleteLongColumnFromRaw(getColumnFamilyFromQueueAddress(sourceAddress),
                                sourceAddress.queueName, messageMetaData.getMessageID(), messageMutator, false);

                        CassandraDataAccessHelper.addMessageToQueue(getColumnFamilyFromQueueAddress(targetAddress), targetAddress.queueName,
                                messageMetaData.getMessageID(), messageMetaData.getMetadata(), messageMutator, false);

                        numberOfMessagesMovedInIteration++;
                        if (log.isDebugEnabled()) {
                            log.debug("TRACING>> CMS-Removed messageID-" + messageMetaData.getMessageID() + "-from Node Queue-" + sourceAddress.queueName
                                    + "- to GlobalQueue " + targetAddress.queueName);
                        }
                    }
                    lastProcessedMessageID = messageMetaData.getMessageID();
                    if (ignoredFirstMessageId > lastProcessedMessageID) {
                        ignoredFirstMessageId = lastProcessedMessageID;
                    }
                }
                messageMutator.execute();
                numberOfMessagesMoved = numberOfMessagesMoved + numberOfMessagesMovedInIteration;
                messageList = getNextNMessageMetadataFromQueue(sourceAddress, lastProcessedMessageID, 40);
            }
            log.info("moved " + numberOfMessagesMoved + "number of messages from source -"
                    + getColumnFamilyFromQueueAddress(sourceAddress) + "- to target -" + getColumnFamilyFromQueueAddress(targetAddress) + "-");

            return lastProcessedMessageID;
        } catch (CassandraDataAccessException e) {
            log.error("Error in moving messages ", e);
            throw new AndesException("Error in moving messages from source -"
                    + getColumnFamilyFromQueueAddress(sourceAddress) + " - to target -" + getColumnFamilyFromQueueAddress(targetAddress), e);
        }
    }

    @Override
    //TODO:hasitha - do we want this method?
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException {
        AndesMessageMetadata metadata = null;
        try {

            byte[] value = CassandraDataAccessHelper.getMessageMetaDataFromQueue(CassandraConstants.NODE_QUEUES_COLUMN_FAMILY, keyspace, messageId);
            if (value == null) {
                value = CassandraDataAccessHelper.getMessageMetaDataFromQueue(CassandraConstants.PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY, keyspace, messageId);
            }
            metadata = new AndesMessageMetadata(messageId, value, true);

        } catch (Exception e) {
            log.error("Error in getting meta data of provided message id", e);
            throw new AndesException("Error in getting meta data for messageID " + messageId, e);
        }
        return metadata;
    }


    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        try {
            List<String> rows2Remove = new ArrayList<String>();
            for (long messageId : messageIdList) {
                rows2Remove.add(new StringBuffer(
                        AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX).append(messageId).toString());
                System.out.println("REMOVE CONTENT>> id " + messageId);
            }
            //remove content
            if (!rows2Remove.isEmpty()) {
                CassandraDataAccessHelper.deleteIntegerRowListFromColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY, rows2Remove, keyspace);
            }
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }

    }

    @Override
    public void deleteMessageMetadataFromQueue(QueueAddress queueAddress,
                                               List<AndesRemovableMetadata> messagesToRemove) throws AndesException {
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            for (AndesRemovableMetadata message : messagesToRemove) {
                mutator.addDeletion(queueAddress.queueName, getColumnFamilyFromQueueAddress(queueAddress), message.messageID, longSerializer);
            }
            mutator.execute();
            if (isMessageCoutingAllowed) {
                for (AndesRemovableMetadata message : messagesToRemove) {
                    decrementQueueCount(message.destination, message.messageID);
                }
            }
        } catch (Exception e) {
            log.error("Error while deleting messages", e);
            throw new AndesException(e);
        }
    }

    public int countMessagesOfQueue(QueueAddress queueAddress, String destinationQueueNameToMatch) throws AndesException {
        long lastProcessedMessageID = 0;
        int messageCount = 0;
        List<AndesMessageMetadata> messageList = getNextNMessageMetadataFromQueue(queueAddress, lastProcessedMessageID, 500);
        while (messageList.size() != 0) {
            Iterator<AndesMessageMetadata> metadataIterator = messageList.iterator();
            while (metadataIterator.hasNext()) {
                AndesMessageMetadata metadata = metadataIterator.next();
                String destinationQueue = metadata.getDestination();
                if (destinationQueueNameToMatch != null) {
                    if (destinationQueue.equals(destinationQueueNameToMatch)) {
                        messageCount++;
                    } else {
                        metadataIterator.remove();
                    }
                } else {
                    messageCount++;
                }

                lastProcessedMessageID = metadata.getMessageID();

            }
            messageList = getNextNMessageMetadataFromQueue(queueAddress, lastProcessedMessageID, 500);
        }
        return messageCount;
    }

    private void incrementQueueCount(String destinationQueueName, long incrementBy) throws CassandraDataAccessException {
        CassandraDataAccessHelper.incrementCounter(destinationQueueName, MESSAGE_COUNTERS_COLUMN_FAMILY, MESSAGE_COUNTERS_RAW_NAME, keyspace, incrementBy);
    }

    private void decrementQueueCount(String destinationQueueName, long decrementBy) throws CassandraDataAccessException {

        CassandraDataAccessHelper.decrementCounter(destinationQueueName, MESSAGE_COUNTERS_COLUMN_FAMILY, MESSAGE_COUNTERS_RAW_NAME,
                keyspace, decrementBy);
    }

    private String getColumnFamilyFromQueueAddress(QueueAddress address) {
        String columnFamilyName;
        if (address.queueType.equals(QueueAddress.QueueType.QUEUE_NODE_QUEUE)) {
            columnFamilyName = CassandraConstants.NODE_QUEUES_COLUMN_FAMILY;
        } else if (address.queueType.equals(QueueAddress.QueueType.GLOBAL_QUEUE)) {
            columnFamilyName = CassandraConstants.GLOBAL_QUEUES_COLUMN_FAMILY;
        } else if (address.queueType.equals(QueueAddress.QueueType.TOPIC_NODE_QUEUE)) {
            columnFamilyName = CassandraConstants.PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY;
        } else {
            columnFamilyName = null;
        }
        return columnFamilyName;
    }

    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        long msgCount = 0;
        try {
            msgCount = CassandraDataAccessHelper.getCountValue(keyspace, MESSAGE_COUNTERS_COLUMN_FAMILY, destinationQueueName,
                    MESSAGE_COUNTERS_RAW_NAME);
        } catch (Exception e) {
            log.error("Error while getting message count for queue " + destinationQueueName);
            throw new AndesException(e);
        }
        return msgCount;
    }


    public void close() {
        if (messageContentRemoverTask != null && messageContentRemoverTask.isRunning())
            this.messageContentRemoverTask.setRunning(false);
    }

/*    public int removeMessaesOfQueue(QueueAddress queueAddress, String destinationQueueNameToMatch) throws AndesException {
        long lastProcessedMessageID = 0;
        int messageCount = 0;
        List<AndesMessageMetadata>  messageList = getNextNMessageMetadataFromQueue(queueAddress, lastProcessedMessageID, 500);
        while (messageList.size() != 0) {
            Iterator<AndesMessageMetadata> metadataIterator = messageList.iterator();
            while (metadataIterator.hasNext()) {
                AndesMessageMetadata metadata = metadataIterator.next();
                String destinationQueue = metadata.getDestination();
                if(destinationQueueNameToMatch != null) {
                    if (destinationQueue.equals(destinationQueueNameToMatch)) {
                        messageCount++;
                    } else {
                        metadataIterator.remove();
                    }
                }  else {
                    messageCount++;
                }

                lastProcessedMessageID = metadata.getMessageID();

            }
            messageList = getNextNMessageMetadataFromQueue(queueAddress, lastProcessedMessageID, 500);
        }
        return messageCount;
    } */

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

    /**
     * Generic interface report the message store about the message status changes.
     * @param messageId The message Id
     * @param timeMillis The timestamp which the change happened
     * @param messageCounterKey The combined key which contains the message queue name and the state it changed to
     * @throws AndesException
     */
    @Override
    public void addMessageStatusChange(long messageId, long timeMillis, MessageCounterKey messageCounterKey) throws AndesException {
        // Not implemented since this class is not being used
    }

    /**
     * Generic interface to get data to draw the message rate graph.
     * @param queueName The queue name to get data, if null all.
     * @return The message rate data. Map<MessageCounterType, Map<TimeInMillis, Number of messages>>
     * @throws AndesException
     */
    @Override
    public Map<MessageCounterKey.MessageCounterType, Map<Long, Integer>> getMessageRates(String queueName, Long minDate, Long maxDate) throws AndesException {
        // Not implemented since this class is not being used.
        return null;
    }

    /**
     * Get the status of each of messages.
     * @param queueName The queue name to get data, if null all.
     * @return Message Status data. Map<Message Id, Map<Property, Value>>
     */
    @Override
    public Map<Long, Map<String, String>> getMessageStatuses(String queueName, Long minDate, Long maxDate) {
        // Not implemented since this class is not being used.
        return null;
    }

}
