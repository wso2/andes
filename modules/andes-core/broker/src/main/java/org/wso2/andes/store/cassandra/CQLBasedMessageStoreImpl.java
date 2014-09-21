package org.wso2.andes.store.cassandra;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.slot.SlotMessageCounter;
import org.wso2.andes.store.cassandra.dao.CQLQueryBuilder;
import org.wso2.andes.store.cassandra.dao.CassandraHelper;
import org.wso2.andes.store.cassandra.dao.GenericCQLDAO;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.server.util.AlreadyProcessedMessageTracker;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.store.MessageContentRemoverTask;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor.PendingJob;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;

public class CQLBasedMessageStoreImpl implements org.wso2.andes.kernel.MessageStore {
    private static Log log = LogFactory.getLog(CQLBasedMessageStoreImpl.class);

    private ConcurrentSkipListMap<Long, Long> contentDeletionTasks = new ConcurrentSkipListMap<Long, Long>();
    private MessageContentRemoverTask messageContentRemoverTask;
    private AlreadyProcessedMessageTracker alreadyMovedMessageTracker;
    private boolean isMessageCountingAllowed;
    private DurableStoreConnection connection;
    private Cluster cluster;
    private static boolean isClusteringEnabled;

    public CQLBasedMessageStoreImpl() {
        isMessageCountingAllowed = ClusterResourceHolder.getInstance().getClusterConfiguration().getViewMessageCounts();
        isClusteringEnabled = AndesContext.getInstance().isClusteringEnabled();
    }

    public DurableStoreConnection initializeMessageStore() throws AndesException {

        // create connection object
        CQLConnection cqlConnection = new CQLConnection();
        cqlConnection.initialize(AndesContext.getInstance().getMessageStoreDataSourceName());

        // get cassandra cluster and create column families
        initializeCassandraMessageStore(cqlConnection);

        // start scheduled tasks
        alreadyMovedMessageTracker = new AlreadyProcessedMessageTracker("Message-move-tracker", 15000000000L, 10);
        messageContentRemoverTask = new MessageContentRemoverTask(ClusterResourceHolder.getInstance().getClusterConfiguration().
                getContentRemovalTaskInterval(), contentDeletionTasks, this, cqlConnection);
        messageContentRemoverTask.start();
        return cqlConnection;
    }

    private void initializeCassandraMessageStore(CQLConnection cqlConnection) throws
            AndesException {
        try {
            cluster = cqlConnection.getCluster();
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
        int gcGraceSeconds = ((CQLConnection) connection.getConnection()).getGcGraceSeconds();
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.NODE_QUEUES_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.GLOBAL_QUEUES_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.META_DATA_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createCounterColumnFamily(CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, gcGraceSeconds);
        CQLDataAccessHelper.createMessageExpiryColumnFamily(CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, gcGraceSeconds);
    }

    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        try {
            /*Mutator<String> messageMutator = HFactory.createMutator(keyspace, stringSerializer);*/
            List<Insert> inserts = new ArrayList<Insert>();
            for (AndesMessagePart part : partList) {
                final String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX
                        + part.getMessageID();
                Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE, CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY,
                        rowKey, part.getOffSet(), part.getData(), false);
                inserts.add(insert);
                //System.out.println("STORE >> message part id" + part.getMessageID() + " offset " + part.getOffSet());
            }
            /*messageMutator.execute();*/
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, inserts.toArray(new Insert[inserts.size()]));
/*            for(AndesMessagePart part : partList) {
                log.info("STORED PART ID: " + part.getMessageID() + " OFFSET: " + part.getOffSet() + " Data Len= " + part.getDataLength());
            }*/
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
            List<AndesMessageMetadata> metadataList = CQLDataAccessHelper.getMessagesFromQueue(queueAddress.queueName,
                    getColumnFamilyFromQueueAddress(queueAddress), CassandraConstants.KEYSPACE, startMsgID + 1, Long.MAX_VALUE, count, true, true);
            //combining metadata with message properties create QueueEntries
            /*for (Object column : messagesColumnSlice.getColumns()) {
                if (column instanceof HColumn) {
                    long messageId = ((HColumn<Long, byte[]>) column).getName();
                    byte[] value = ((HColumn<Long, byte[]>) column).getValue();
                    metadataList.add(new AndesMessageMetadata(messageId, value));
                }
            }*/
            return metadataList;
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }

    }

    public void duplicateMessageContent(long messageId, long messageIdOfClone) throws AndesException {
        String originalRowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId;
        String cloneMessageKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageIdOfClone;
        try {

            long tryCount = 0;
            //read from store
        /*    ColumnQuery columnQuery = HFactory.createColumnQuery(KEYSPACE, stringSerializer,
                    integerSerializer, byteBufferSerializer);
            columnQuery.setColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY);
            columnQuery.setKey(originalRowKey.trim());

            SliceQuery<String, Integer, ByteBuffer> query = HFactory.createSliceQuery(KEYSPACE, stringSerializer, integerSerializer, byteBufferSerializer);
            query.setColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY).setKey(originalRowKey).setRange(0, Integer.MAX_VALUE, false, Integer.MAX_VALUE);
            QueryResult<ColumnSlice<Integer, ByteBuffer>> result = query.execute();*/

            List<AndesMessageMetadata> messages = CQLDataAccessHelper.getMessagesFromQueue(originalRowKey.trim(), CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, CassandraConstants.KEYSPACE, 0, Long.MAX_VALUE, Long.MAX_VALUE, true, false);

            //if there are values duplicate them

            /*if (!result.get().getColumns().isEmpty()) {*/
            if (!messages.isEmpty()) {
               /* Mutator<String> mutator = HFactory.createMutator(KEYSPACE, stringSerializer);*/
                /*for (HColumn<Integer, ByteBuffer> column : result.get().getColumns()) {
                    int offset = column.getName();
                    final byte[] chunkData = bytesArraySerializer.fromByteBuffer(column.getValue());
                    CQLDataAccessHelper.addMessageToQueue(MESSAGE_CONTENT_COLUMN_FAMILY, cloneMessageKey,
                            offset, chunkData, mutator, false);
                    System.out.println("DUPLICATE>> new id " + messageIdOfClone + " cloned from id " + messageId + " offset" + offset);

                }*/
                //mutator.execute();
                for (AndesMessageMetadata msg : messages) {
                    long offset = msg.getMessageID();
                    final byte[] chunkData = msg.getMetadata();
                    CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE, CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, cloneMessageKey,
                            offset, chunkData, false);
                    System.out.println("DUPLICATE>> new id " + messageIdOfClone + " cloned from id " + messageId + " offset" + offset);
                }
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

    /**
     * Get message content part of a message.
     *
     * @param messageId The message Id
     * @param offsetValue Message content offset value
     * @return Message content part
     * @throws AndesException
     */
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        AndesMessagePart messagePart;
        try {
            String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId;
            messagePart = CQLDataAccessHelper.getMessageContent(rowKey.trim(), CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, CassandraConstants.KEYSPACE, messageId, offsetValue);
        } catch (Exception e) {
            log.error("Error in reading content messageID= " + messageId + " offset=" + offsetValue, e);
            throw new AndesException("Error in reading content messageID=" + messageId + " offset=" + offsetValue, e);
        }
        return messagePart;
    }

    public void deleteMessageParts(long messageID, byte[] data) {
    }

    public void ackReceived(List<AndesAckData> ackList) throws AndesException {
        try {

            List<AndesRemovableMetadata> messagesAddressedToQueues = new ArrayList<AndesRemovableMetadata>();
            List<AndesRemovableMetadata> messagesAddressedToTopics = new ArrayList<AndesRemovableMetadata>();

            List<Long> messageIds = new ArrayList<Long>();

            for (AndesAckData ackData : ackList) {
                if (ackData.isTopic) {

                    messagesAddressedToTopics.add(ackData.convertToRemovableMetaData());

                    //schedule to remove queue and topic message content
                    long timeGapConfigured = ClusterResourceHolder.getInstance().
                            getClusterConfiguration().getPubSubMessageRemovalTaskInterval() * 1000000;
                    addContentDeletionTask(System.nanoTime() + timeGapConfigured, ackData.messageID);

                } else {

                    messagesAddressedToQueues.add(ackData.convertToRemovableMetaData());
                    OnflightMessageTracker onflightMessageTracker = OnflightMessageTracker.getInstance();
                    onflightMessageTracker.updateDeliveredButNotAckedMessages(ackData.messageID);

                    //decrement message count
                    if (isMessageCountingAllowed) {
                        decrementQueueCount(ackData.qName, 1);
                    }

                    //schedule to remove queue and topic message content
                    addContentDeletionTask(System.nanoTime(), ackData.messageID);
                }

                PerformanceCounter.recordMessageRemovedAfterAck();

                messageIds.add(ackData.messageID);
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

            deleteMessagesFromExpiryQueue(messageIds);      // hasithad This cant be done here cos topic delivery logic comes here before a delivery :(

        } catch (CassandraDataAccessException e) {
            //TODO: hasitha - handle Cassandra failures
            log.error(e);
            throw new AndesException("Error in handling acknowledgments ", e);
        }
    }

    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {
        try {
            //  HashMap<String, Integer> incomingMessagesMap = new HashMap<String, Integer>();
            List<Insert> inserts = new ArrayList<Insert>();
            ConcurrentHashMap<String, Slot> queueSlotMap = new ConcurrentHashMap<String, Slot>();
            for (AndesMessageMetadata md : metadataList) {

                Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE, CassandraConstants.META_DATA_COLUMN_FAMILY, md.getDestination(),
                        md.getMessageID(), md.getMetadata(), false);

                inserts.add(insert);
                PerformanceCounter.recordIncomingMessageWrittenToCassandra();
                //log.info("Wrote message " + md.getMessageID() + " to Global Queue " + queueAddress.queueName);

            }
            long start = System.currentTimeMillis();
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, inserts.toArray(new Insert[inserts.size()]));

            PerformanceCounter.recordIncomingMessageWrittenToCassandraLatency((int) (System.currentTimeMillis() - start));

            if (isMessageCountingAllowed) {
                for (AndesMessageMetadata md : metadataList) {
                    incrementQueueCount(md.getDestination(), 1);
                }
            }

            // Client waits for these message ID to be written, this signal those, if there is a error
            //we will not signal and client who tries to close the connection will timeout.
            //We can do this better, but leaving this as is or now.
            for (AndesMessageMetadata md : metadataList) {
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

    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {

    }

    @Override
    public void addMetaDataToQueue(String queueName, AndesMessageMetadata metadata) throws AndesException {

    }

    @Override
    public void addMetadataToQueue(String queueName, List<AndesMessageMetadata> metadataList) throws AndesException {
    }

    private void addContentDeletionTask(long currentNanoTime, long messageID) {
        contentDeletionTasks.put(currentNanoTime, messageID);
    }

    public void addMessageMetaData(QueueAddress queueAddress, List<AndesMessageMetadata> messageList) throws AndesException {
        try {
/*            Mutator<String> messageMutator = HFactory.createMutator(KEYSPACE, stringSerializer);*/
            HashMap<String, Integer> incomingMessagesMap = new HashMap<String, Integer>();
            List<Insert> inserts = new ArrayList<Insert>();
            for (AndesMessageMetadata md : messageList) {

                //TODO Stop deleting from QMD_ROW_NAME and GLOBAL_QUEUE_LIST_COLUMN_FAMILY

                //TODO this is to avoid having to group messages in AlternatingCassandraWriter
                if (queueAddress == null) {
                    queueAddress = md.queueAddress;
                }
                Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE, getColumnFamilyFromQueueAddress(queueAddress), queueAddress.queueName,
                        md.getMessageID(), md.getMetadata(), false);
                if (incomingMessagesMap.get(md.getDestination()) == null) {
                    incomingMessagesMap.put(md.getDestination(), 1);
                } else {
                    incomingMessagesMap.put(md.getDestination(), incomingMessagesMap.get(md.getDestination()) + 1);
                }
                inserts.add(insert);
                PerformanceCounter.recordIncomingMessageWrittenToCassandra();
                //log.info("Wrote message " + md.getMessageID() + " to Global Queue " + queueAddress.queueName);

            }
            long start = System.currentTimeMillis();
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, inserts.toArray(new Insert[inserts.size()]));

/*            for (AndesMessageMetadata md : messageList) {
                log.info("METADATA STORED ID " + md.getMessageID());
            }*/
            //messageMutator.execute();

            PerformanceCounter.recordIncomingMessageWrittenToCassandraLatency((int) (System.currentTimeMillis() - start));

            if (isMessageCountingAllowed) {
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
        /*Mutator<String> messageMutator = HFactory.createMutator(KEYSPACE, stringSerializer);*/
        try {
            List<Statement> statements = new ArrayList<Statement>();
            List<Long> messageIDsSuccessfullyMoved = new ArrayList<Long>();
            for (AndesMessageMetadata messageMetaData : messageList) {
                if (targetAddress == null) {
                    targetAddress = messageMetaData.queueAddress;
                }
                if (msgOKToMove(messageMetaData.getMessageID(), sourceAddress, targetAddress)) {
                    Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(CassandraConstants.KEYSPACE, getColumnFamilyFromQueueAddress(sourceAddress), sourceAddress.queueName, messageMetaData.getMessageID(), false);
                    Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE, getColumnFamilyFromQueueAddress(targetAddress), targetAddress.queueName,
                            messageMetaData.getMessageID(), messageMetaData.getMetadata(), false);
                    if (log.isDebugEnabled()) {
                        log.debug("TRACING>> CMS-Removing messageID-" + messageMetaData.getMessageID() + "-from source Queue-" + sourceAddress.queueName
                                + "- to target Queue " + targetAddress.queueName);
                    }
                    statements.add(insert);
                    statements.add(delete);
                    messageIDsSuccessfullyMoved.add(messageMetaData.getMessageID());
                }
            }
            // messageMutator.execute();
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, statements.toArray(new Statement[statements.size()]));

            for (Long msgID : messageIDsSuccessfullyMoved) {
                alreadyMovedMessageTracker.addTaskToRemoveMessageFromTracking(msgID);
            }

        } catch (CassandraDataAccessException e) {
            log.error("Error in moving messages ", e);
            throw new AndesException("Error in moving messages from source -" + getColumnFamilyFromQueueAddress(sourceAddress) + " - to target -" + getColumnFamilyFromQueueAddress(targetAddress), e);
        }

    }

    public long moveAllMessageMetaDataOfQueue(QueueAddress sourceAddress, QueueAddress targetAddress, String destinationQueue) throws AndesException {
        //Mutator<String> messageMutator = HFactory.createMutator(KEYSPACE, stringSerializer);
        try {
            long ignoredFirstMessageId = Long.MAX_VALUE;
            int numberOfMessagesMoved = 0;
            long lastProcessedMessageID = 0;
            List<AndesMessageMetadata> messageList = getNextNMessageMetadataFromQueue(sourceAddress, lastProcessedMessageID, 40);
            List<Statement> statements = new ArrayList<Statement>();
            while (messageList.size() != 0) {
                int numberOfMessagesMovedInIteration = 0;
                List<Long> messageIDsSuccessfullyMoved = new ArrayList<Long>();
                for (AndesMessageMetadata messageMetaData : messageList) {
                    if (messageMetaData.getDestination().equals(destinationQueue)) {
                        if (targetAddress == null) {
                            targetAddress = messageMetaData.queueAddress;
                        }
                        if (msgOKToMove(messageMetaData.getMessageID(), sourceAddress, targetAddress)) {
                            Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(CassandraConstants.KEYSPACE, getColumnFamilyFromQueueAddress(sourceAddress),
                                    sourceAddress.queueName, messageMetaData.getMessageID(), false);

                            Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE, getColumnFamilyFromQueueAddress(targetAddress), targetAddress.queueName,
                                    messageMetaData.getMessageID(), messageMetaData.getMetadata(), false);

                            statements.add(insert);
                            statements.add(delete);

                            messageIDsSuccessfullyMoved.add(messageMetaData.getMessageID());
                            numberOfMessagesMovedInIteration++;
                        }

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
                GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, statements.toArray(new Statement[statements.size()]));
                for (Long msgID : messageIDsSuccessfullyMoved) {
                    alreadyMovedMessageTracker.addTaskToRemoveMessageFromTracking(msgID);
                }

                //messageMutator.execute();
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

            byte[] value = CQLDataAccessHelper.getMessageMetaDataFromQueue(CassandraConstants.NODE_QUEUES_COLUMN_FAMILY, CassandraConstants.KEYSPACE, messageId);
            if (value == null) {
                value = CQLDataAccessHelper.getMessageMetaDataFromQueue(CassandraConstants.PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY, CassandraConstants.KEYSPACE, messageId);
            }
            metadata = new AndesMessageMetadata(messageId, value, true);

        } catch (Exception e) {
            log.error("Error in getting meta data of provided message id", e);
            throw new AndesException("Error in getting meta data for messageID " + messageId, e);
        }
        return metadata;
    }

    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId, long lastMsgID) throws AndesException {
        try {
            //TODO: what should be put to count
            List<AndesMessageMetadata> metadataList = CQLDataAccessHelper.getMessagesFromQueue(queueName,
                    CassandraConstants.META_DATA_COLUMN_FAMILY, CassandraConstants.KEYSPACE, firstMsgId, lastMsgID, 1000, true, true);
            return metadataList;
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }


    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String queueName, long firstMsgId, int count) throws AndesException {
        try {
            List<AndesMessageMetadata> metadataList = CQLDataAccessHelper.getMessagesFromQueue(queueName,
                    CassandraConstants.META_DATA_COLUMN_FAMILY, CassandraConstants.KEYSPACE, firstMsgId + 1, Long.MAX_VALUE, count, true, true);
            //combining metadata with message properties create QueueEntries
            /*for (Object column : messagesColumnSlice.getColumns()) {
                if (column instanceof HColumn) {
                    long messageId = ((HColumn<Long, byte[]>) column).getName();
                    byte[] value = ((HColumn<Long, byte[]>) column).getValue();
                    metadataList.add(new AndesMessageMetadata(messageId, value));
                }
            }*/
            return metadataList;
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }

    }

    @Override
    public void deleteMessageMetadataFromQueue(String queueName, List<AndesRemovableMetadata> messagesToRemove) throws AndesException {

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
                CQLDataAccessHelper.deleteIntegerRowListFromColumnFamily(CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, rows2Remove, CassandraConstants.KEYSPACE);
            }
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }

    }

    @Override
    public void deleteMessageMetadataFromQueue(QueueAddress queueAddress,
                                               List<AndesRemovableMetadata> messagesToRemove) throws AndesException {
        try {
            // Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            List<Statement> statements = new ArrayList<Statement>();
            for (AndesRemovableMetadata message : messagesToRemove) {
                //mutator.addDeletion(queueAddress.queueName, getColumnFamilyFromQueueAddress(queueAddress), message.messageID, longSerializer);
                //Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(KEYSPACE, getColumnFamilyFromQueueAddress(queueAddress), queueAddress.queueName, message.messageID, false);
                Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(CassandraConstants.KEYSPACE, CassandraConstants.META_DATA_COLUMN_FAMILY, message.destination, message.messageID, false);
                statements.add(delete);
            }
            //mutator.execute();
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, statements.toArray(new Statement[statements.size()]));
            if (isMessageCountingAllowed) {
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
        List<AndesMessageMetadata> messageList = getNextNMessageMetadataFromQueue(destinationQueueNameToMatch, lastProcessedMessageID, 500);
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
            messageList = getNextNMessageMetadataFromQueue(destinationQueueNameToMatch, lastProcessedMessageID, 500);
        }
        return messageCount;
    }

    private void incrementQueueCount(String destinationQueueName, long incrementBy) throws CassandraDataAccessException {
        CQLDataAccessHelper.incrementCounter(destinationQueueName, CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, CassandraConstants.MESSAGE_COUNTERS_RAW_NAME, CassandraConstants.KEYSPACE, incrementBy);
    }

    private void decrementQueueCount(String destinationQueueName, long decrementBy) throws CassandraDataAccessException {

        CQLDataAccessHelper.decrementCounter(destinationQueueName, CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, CassandraConstants.MESSAGE_COUNTERS_RAW_NAME,
                CassandraConstants.KEYSPACE, decrementBy);
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
            msgCount = CQLDataAccessHelper.getCountValue(CassandraConstants.KEYSPACE, CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, destinationQueueName,
                    CassandraConstants.MESSAGE_COUNTERS_RAW_NAME);
        } catch (Exception e) {
            log.error("Error while getting message count for queue " + destinationQueueName);
            throw new AndesException(e);
        }
        return msgCount;
    }

    private boolean msgOKToMove(long msgID, QueueAddress sourceAddress, QueueAddress targetAddress) throws CassandraDataAccessException {
        if (alreadyMovedMessageTracker.checkIfAlreadyTracked(msgID)) {
            List<Statement> statements = new ArrayList<Statement>();
            Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(CassandraConstants.KEYSPACE, getColumnFamilyFromQueueAddress(sourceAddress), sourceAddress.queueName, msgID, false);
            statements.add(delete);
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, statements.toArray(new Statement[statements.size()]));
            if (log.isTraceEnabled()) {
                log.trace("Rejecting moving message id =" + msgID + " as it is already read from " + sourceAddress.queueName);
            }
            return false;
        } else {
            alreadyMovedMessageTracker.insertMessageForTracking(msgID, msgID);
            if (log.isTraceEnabled()) {
                log.trace("allowing to move message id - " + msgID + " from " + sourceAddress.queueName);
            }
            return true;
        }
    }

    public void close() {
        if (messageContentRemoverTask != null && messageContentRemoverTask.isRunning()) {
            this.messageContentRemoverTask.setRunning(false);
        }
        alreadyMovedMessageTracker.shutDownMessageTracker();
        connection.close();
    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) {
        // todo implement

        return new ArrayList<AndesRemovableMetadata>();
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

    /**
     * utility method to remove message metadata from either topic, queue or global column families
     *
     * @param messagesToRemove
     * @param queueType
     */
    private void deleteMessageMetadataFromColumnFamily(HashMap<String, List<AndesRemovableMetadata>> messagesToRemove, QueueAddress.QueueType queueType) throws AndesException, CassandraDataAccessException {
        for (Map.Entry<String, List<AndesRemovableMetadata>> rowEntry : messagesToRemove.entrySet()) {
            //remove message metadata now
            QueueAddress rowAddress = new QueueAddress(queueType, rowEntry.getKey());
            deleteMessageMetadataFromQueue(rowAddress, rowEntry.getValue());

            if (isMessageCountingAllowed) {
                decrementQueueCount(rowEntry.getKey(), 1);
            }
        }
    }

    public void deleteMessageMetadata(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDLC) throws AndesException {
    }

    @Override
/**
 * Method to delete entries from MESSAGES_FOR_EXPIRY_COLUMN_FAMILY Column Family
 */
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        try {
            List<Statement> statements = new ArrayList<Statement>();
            for (Long messageId : messagesToRemove) {

                CQLQueryBuilder.CqlDelete cqlDelete = new CQLQueryBuilder.CqlDelete(CassandraConstants.KEYSPACE, CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY);

                cqlDelete.addCondition(CQLDataAccessHelper.MESSAGE_ID, messageId, CassandraHelper.WHERE_OPERATORS.EQ);

                Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);

                statements.add(delete);
            }

            // There is another way. Can delete using a single where clause on the timestamp. (delete all messages with expiration time < current timestamp)
            // But that could result in inconsistent data overall.
            // If a message has been added to the queue between expiry checker invocation and this method, it could be lost.
            // Therefore, the above approach to delete.
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, statements.toArray(new Statement[statements.size()]));
        } catch (Exception e) {
            log.error("Error while deleting messages", e);
            throw new AndesException(e);
        }
    }

    @Override
/**
 * Adds the received JMS Message ID along with its expiration time to "MESSAGES_FOR_EXPIRY_COLUMN_FAMILY" queue
 * @param messageId
 * @param expirationTime
 * @throws CassandraDataAccessException
 */
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String destination) throws CassandraDataAccessException {

        final String columnFamily = CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY;
        //final String rowKey = CassandraConstants.MESSAGES_FOR_EXPIRY_ROW_NAME;

        if (columnFamily == null || messageId == 0) {
            throw new CassandraDataAccessException("Can't add data with queueType = " + columnFamily +
                    " and messageId  = " + messageId + " expirationTime = " + expirationTime);
        }

        Map<String, Object> keyValueMap = new HashMap<String, Object>();
        keyValueMap.put(CQLDataAccessHelper.MESSAGE_ID, messageId);
        keyValueMap.put(CQLDataAccessHelper.MESSAGE_EXPIRATION_TIME, expirationTime);
        keyValueMap.put(CQLDataAccessHelper.MESSAGE_DESTINATION, destination);
        keyValueMap.put(CQLDataAccessHelper.MESSAGE_IS_FOR_TOPIC, isMessageForTopic);

        Insert insert = CQLQueryBuilder.buildSingleInsert(CassandraConstants.KEYSPACE, columnFamily, keyValueMap);

        GenericCQLDAO.execute(CassandraConstants.KEYSPACE, insert.getQueryString());

        log.info("Wrote message " + messageId + " to Column Family " + CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY);

    }

    @Override
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDLC) throws AndesException {
        try {

            HashMap<String, List<AndesRemovableMetadata>> messagesForTopics = new HashMap<String, List<AndesRemovableMetadata>>();
            HashMap<String, List<AndesRemovableMetadata>> messagesForQueues = new HashMap<String, List<AndesRemovableMetadata>>();
            HashMap<String, List<AndesRemovableMetadata>> messagesInGlobalQueues = new HashMap<String, List<AndesRemovableMetadata>>();

            SubscriptionStore subscriptionStore = MessagingEngine.getInstance().getSubscriptionStore();

            List<Long> messageIds = new ArrayList<Long>();

            for (AndesRemovableMetadata msg : messagesToRemove) {

                List<String> associatedQueueRows = new ArrayList<String>(subscriptionStore.getNodeQueuesHavingSubscriptionsForQueue(msg.destination));
                List<String> associatedTopicRows = new ArrayList<String>(subscriptionStore.getNodeQueuesHavingSubscriptionsForTopic(msg.destination));

            /* Find message inside global queues */
                String associatedGlobalQueue = AndesUtils.getGlobalQueueNameForDestinationQueue(msg.destination);

                if (!messagesInGlobalQueues.containsKey(associatedGlobalQueue)) {
                    messagesInGlobalQueues.put(associatedGlobalQueue, new ArrayList<AndesRemovableMetadata>());
                }
                messagesInGlobalQueues.get(associatedGlobalQueue).add(msg);
            /*------------------------------------*/

                if (msg.isForTopic) {

                    for (String nodeRowName : associatedTopicRows) {
                        if (!messagesForTopics.containsKey(nodeRowName)) {
                            messagesForTopics.put(nodeRowName, new ArrayList<AndesRemovableMetadata>());
                        }
                        messagesForTopics.get(nodeRowName).add(msg);
                    }

                    if (!moveToDLC) {
                        //schedule to remove queue and topic message content
                        long timeGapConfigured = ClusterResourceHolder.getInstance().
                                getClusterConfiguration().getPubSubMessageRemovalTaskInterval() * 1000000;
                        addContentDeletionTask(System.nanoTime() + timeGapConfigured, msg.messageID);
                    }

                } else {

                    for (String nodeRowName : associatedQueueRows) {
                        if (!messagesForQueues.containsKey(nodeRowName)) {
                            messagesForQueues.put(nodeRowName, new ArrayList<AndesRemovableMetadata>());
                        }
                        messagesForQueues.get(nodeRowName).add(msg);
                    }

                    if (!moveToDLC) {
                        //schedule to remove queue and message content
                        addContentDeletionTask(System.nanoTime(), msg.messageID);
                    }
                }
                messageIds.add(msg.messageID);
            }

            deleteMessageMetadataFromColumnFamily(messagesForQueues, QueueAddress.QueueType.QUEUE_NODE_QUEUE);
            deleteMessageMetadataFromColumnFamily(messagesForTopics, QueueAddress.QueueType.TOPIC_NODE_QUEUE);
            deleteMessageMetadataFromColumnFamily(messagesInGlobalQueues, QueueAddress.QueueType.GLOBAL_QUEUE);
            deleteMessagesFromExpiryQueue(messageIds);

            if (moveToDLC) {
                moveToDeadLetterChannel(messagesToRemove);
            }

        } catch (Exception e) {
            log.error(e);
            throw new AndesException("Error during message deletion ", e);
        }
    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(Long limit, String columnFamilyName, String keyspace) {

        if (keyspace == null) {
            log.error("Can't access Data , no keyspace provided ");
        }

        if (columnFamilyName == null) {
            log.error("Can't access data with queueType = " + columnFamilyName);
        }

        List<AndesRemovableMetadata> expiredMessages = new ArrayList<AndesRemovableMetadata>();

        try {

            Long currentTimestamp = System.currentTimeMillis();

            CQLQueryBuilder.CqlSelect cqlSelect = new CQLQueryBuilder.CqlSelect(columnFamilyName, limit, true);
            cqlSelect.addColumn(CQLDataAccessHelper.MESSAGE_ID);
            cqlSelect.addColumn(CQLDataAccessHelper.MESSAGE_DESTINATION);
            cqlSelect.addColumn(CQLDataAccessHelper.MESSAGE_IS_FOR_TOPIC);
            cqlSelect.addColumn(CQLDataAccessHelper.MESSAGE_EXPIRATION_TIME);

            cqlSelect.addCondition(CQLDataAccessHelper.MESSAGE_EXPIRATION_TIME, currentTimestamp, CassandraHelper.WHERE_OPERATORS.LTE);

            Select select = CQLQueryBuilder.buildSelect(cqlSelect);

            if (log.isDebugEnabled()) {
                log.debug(" getExpiredMessages : " + select.toString());
            }

            ResultSet result = GenericCQLDAO.execute(keyspace, select.getQueryString());
            List<Row> rows = result.all();
            Iterator<Row> iter = rows.iterator();

            while (iter.hasNext()) {
                Row row = iter.next();

                AndesRemovableMetadata arm = new AndesRemovableMetadata(row.getLong(CQLDataAccessHelper.MESSAGE_ID), row.getString(CQLDataAccessHelper.MESSAGE_DESTINATION));
                arm.isForTopic = row.getBool(CQLDataAccessHelper.MESSAGE_IS_FOR_TOPIC);

                if (arm.messageID > 0) {
                    expiredMessages.add(arm);
                }
            }

            return expiredMessages;

        } catch (Exception e) {
            log.error("Error while getting data from " + columnFamilyName, e);
        }

        return expiredMessages;

    }

    @Override
    public void moveToDeadLetterChannel(List<AndesRemovableMetadata> messageList) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
