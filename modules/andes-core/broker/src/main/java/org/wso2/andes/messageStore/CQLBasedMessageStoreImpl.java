package org.wso2.andes.messageStore;

import java.util.*;
import java.util.concurrent.*;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.CQLConnection;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.cassandra.dao.CQLQueryBuilder;
import org.wso2.andes.server.cassandra.dao.CassandraHelper;
import org.wso2.andes.server.cassandra.dao.GenericCQLDAO;
import org.wso2.andes.server.stats.MessageCounterKey;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.server.util.AlreadyProcessedMessageTracker;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor.PendingJob;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;

import static org.wso2.andes.messageStore.CassandraConstants.*;

public class CQLBasedMessageStoreImpl implements org.wso2.andes.kernel.MessageStore {
    private static Log log = LogFactory.getLog(CQLBasedMessageStoreImpl.class);

    private ConcurrentSkipListMap<Long, Long> contentDeletionTasks = new ConcurrentSkipListMap<Long, Long>();
    private MessageContentRemoverTask messageContentRemoverTask;
    private AlreadyProcessedMessageTracker alreadyMovedMessageTracker;
    private boolean isMessageCountingAllowed;
    private DurableStoreConnection connection;
    private Cluster cluster;
    private boolean statsEnabled;
    private int statsTimeToLive;

    private List<Statement> messageStatusInserts;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    ScheduledFuture<?> messageStatusUpdateSchedule;


    public CQLBasedMessageStoreImpl() {
        isMessageCountingAllowed = ClusterResourceHolder.getInstance().getClusterConfiguration().getViewMessageCounts();
        statsEnabled = ClusterResourceHolder.getInstance().getClusterConfiguration().isStatsEnabled();
        statsTimeToLive = ClusterResourceHolder.getInstance().getClusterConfiguration().getStatsTimeToLive();
    }

    public void initializeMessageStore(DurableStoreConnection cassandraconnection) throws AndesException {
        connection = cassandraconnection;
        initializeCassandraMessageStore(cassandraconnection);
        alreadyMovedMessageTracker = new AlreadyProcessedMessageTracker("Message-move-tracker", 15000000000L, 10);
        messageContentRemoverTask = new MessageContentRemoverTask(ClusterResourceHolder.getInstance().getClusterConfiguration().
                getContentRemovalTaskInterval(), contentDeletionTasks, this, cassandraconnection);
        messageContentRemoverTask.start();
    }


    private void initializeCassandraMessageStore(DurableStoreConnection cassandraconnection) throws AndesException {
        try {
            cluster = ((CQLConnection) cassandraconnection).getCluster();
            createColumnFamilies();

            initializeMessageStatusInserts();
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
        CQLDataAccessHelper.createColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY, KEYSPACE, this.cluster, LONG_TYPE, DataType.blob(),gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(NODE_QUEUES_COLUMN_FAMILY, KEYSPACE, this.cluster, LONG_TYPE, DataType.blob(),gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(GLOBAL_QUEUES_COLUMN_FAMILY, KEYSPACE, this.cluster, LONG_TYPE, DataType.blob(),gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY, KEYSPACE, this.cluster, LONG_TYPE, DataType.blob(),gcGraceSeconds);
        CQLDataAccessHelper.createCounterColumnFamily(MESSAGE_COUNTERS_COLUMN_FAMILY, KEYSPACE, this.cluster,gcGraceSeconds);
        CQLDataAccessHelper.createMessageExpiryColumnFamily(CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY, KEYSPACE, this.cluster,gcGraceSeconds);

        if (statsEnabled) {
            CQLDataAccessHelper.createMessageStatusColumnFamily(MESSAGE_STATUS_CHANGE_COLUMN_FAMILY, KEYSPACE, this.cluster, LONG_TYPE, DataType.bigint(), gcGraceSeconds);
        }
    }

    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        try {
            /*Mutator<String> messageMutator = HFactory.createMutator(keyspace, stringSerializer);*/
        	List<Insert> inserts = new ArrayList<Insert>();
            for (AndesMessagePart part : partList) {
                final String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX
                        + part.getMessageID();
                Insert insert = CQLDataAccessHelper.addMessageToQueue(KEYSPACE,MESSAGE_CONTENT_COLUMN_FAMILY,
                        rowKey, part.getOffSet(), part.getData(), false);
                inserts.add(insert);
                //System.out.println("STORE >> message part id" + part.getMessageID() + " offset " + part.getOffSet());
            }
            /*messageMutator.execute();*/
            GenericCQLDAO.batchExecute(KEYSPACE, inserts.toArray(new Insert[inserts.size()]));
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
                    getColumnFamilyFromQueueAddress(queueAddress), KEYSPACE, startMsgID + 1, Long.MAX_VALUE, count,true,true);
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
            
            List<AndesMessageMetadata> messages = CQLDataAccessHelper.getMessagesFromQueue(originalRowKey.trim(), MESSAGE_CONTENT_COLUMN_FAMILY, KEYSPACE, 0,Long.MAX_VALUE, Long.MAX_VALUE,true, false);

            //if there are values duplicate them

            /*if (!result.get().getColumns().isEmpty()) {*/
            if(!messages.isEmpty()){
               /* Mutator<String> mutator = HFactory.createMutator(KEYSPACE, stringSerializer);*/
                /*for (HColumn<Integer, ByteBuffer> column : result.get().getColumns()) {
                    int offset = column.getName();
                    final byte[] chunkData = bytesArraySerializer.fromByteBuffer(column.getValue());
                    CQLDataAccessHelper.addMessageToQueue(MESSAGE_CONTENT_COLUMN_FAMILY, cloneMessageKey,
                            offset, chunkData, mutator, false);
                    System.out.println("DUPLICATE>> new id " + messageIdOfClone + " cloned from id " + messageId + " offset" + offset);

                }*/
                //mutator.execute();
            	for(AndesMessageMetadata msg : messages){
            		long offset =  msg.getMessageID();
                    final byte[] chunkData = msg.getMetadata();
                    CQLDataAccessHelper.addMessageToQueue(KEYSPACE, MESSAGE_CONTENT_COLUMN_FAMILY, cloneMessageKey,
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


    public AndesMessagePart getContent(String messageId, int offsetValue) throws AndesException{
        //log.info("REQUEST GET CONTENT >> id " + messageId + " offset " + offsetValue);
        byte[] content;
        AndesMessagePart messagePart;
        try {
            String rowKey = "mid" + messageId;
            	List<AndesMessageMetadata> messages = CQLDataAccessHelper.getMessagesFromQueue(rowKey.trim(), MESSAGE_CONTENT_COLUMN_FAMILY, KEYSPACE, 0, 0, 10,false,false);
                if (!messages.isEmpty()) {
                	AndesMessageMetadata msg = messages.iterator().next();
                    int offset = (int) msg.getMessageID();//column.getName();
                    content = msg.getMetadata();//bytesArraySerializer.fromByteBuffer(column.getValue());

                    messagePart = new AndesMessagePart();
                    messagePart.setData(content);
                    messagePart.setMessageID(Long.parseLong(messageId));
                    messagePart.setOffSet(offset);
                    messagePart.setDataLength(content.length);


                } else {
                    throw new RuntimeException("Unexpected Error , content not available for message id :" + messageId);
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

            List<Long> messageIds = new ArrayList<Long>();

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
                    if (isMessageCountingAllowed) {
                        decrementQueueCount(ackData.qName, 1);
                    }

                    //schedule to remove queue and topic message content
                    addContentDeletionTask(System.nanoTime(),ackData.messageID);
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
                Insert insert = CQLDataAccessHelper.addMessageToQueue(KEYSPACE, getColumnFamilyFromQueueAddress(queueAddress), queueAddress.queueName,
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
            GenericCQLDAO.batchExecute(KEYSPACE, inserts.toArray(new Insert[inserts.size()]));

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
                if(msgOKToMove(messageMetaData.getMessageID(), sourceAddress, targetAddress)) {
                    Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(KEYSPACE,getColumnFamilyFromQueueAddress(sourceAddress), sourceAddress.queueName, messageMetaData.getMessageID(), false);
                    Insert insert = CQLDataAccessHelper.addMessageToQueue(KEYSPACE,getColumnFamilyFromQueueAddress(targetAddress), targetAddress.queueName,
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
            GenericCQLDAO.batchExecute(KEYSPACE, statements.toArray(new Statement[statements.size()]));

            for(Long msgID : messageIDsSuccessfullyMoved) {
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
                        if(msgOKToMove(messageMetaData.getMessageID(), sourceAddress, targetAddress)) {
                            Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(KEYSPACE, getColumnFamilyFromQueueAddress(sourceAddress),
                                    sourceAddress.queueName, messageMetaData.getMessageID(), false);

                            Insert insert = CQLDataAccessHelper.addMessageToQueue(KEYSPACE, getColumnFamilyFromQueueAddress(targetAddress), targetAddress.queueName,
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
                GenericCQLDAO.batchExecute(KEYSPACE, statements.toArray(new Statement[statements.size()]));
                for(Long msgID : messageIDsSuccessfullyMoved) {
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
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException{
        AndesMessageMetadata metadata = null;
        try {

            byte[] value = CQLDataAccessHelper.getMessageMetaDataFromQueue(CassandraConstants.NODE_QUEUES_COLUMN_FAMILY, KEYSPACE, messageId);
            if (value == null) {
                value = CQLDataAccessHelper.getMessageMetaDataFromQueue(CassandraConstants.PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY, KEYSPACE, messageId);
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
                CQLDataAccessHelper.deleteIntegerRowListFromColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY, rows2Remove, KEYSPACE);
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
                Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(KEYSPACE, getColumnFamilyFromQueueAddress(queueAddress),queueAddress.queueName, message.messageID, false);
                statements.add(delete);
            }
            //mutator.execute();
            GenericCQLDAO.batchExecute(KEYSPACE, statements.toArray(new Statement[statements.size()]));
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
        CQLDataAccessHelper.incrementCounter(destinationQueueName, MESSAGE_COUNTERS_COLUMN_FAMILY, MESSAGE_COUNTERS_RAW_NAME, KEYSPACE, incrementBy);
    }

    private void decrementQueueCount(String destinationQueueName, long decrementBy) throws CassandraDataAccessException {

        CQLDataAccessHelper.decrementCounter(destinationQueueName, MESSAGE_COUNTERS_COLUMN_FAMILY, MESSAGE_COUNTERS_RAW_NAME,
                KEYSPACE, decrementBy);
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
            msgCount = CQLDataAccessHelper.getCountValue(KEYSPACE, MESSAGE_COUNTERS_COLUMN_FAMILY, destinationQueueName,
                    MESSAGE_COUNTERS_RAW_NAME);
        } catch (Exception e) {
            log.error("Error while getting message count for queue " + destinationQueueName);
            throw new AndesException(e);
        }
        return msgCount;
    }


    private boolean msgOKToMove(long msgID, QueueAddress sourceAddress, QueueAddress targetAddress) throws CassandraDataAccessException {
        if (alreadyMovedMessageTracker.checkIfAlreadyTracked(msgID)) {
            List<Statement> statements = new ArrayList<Statement>();
            Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(KEYSPACE,getColumnFamilyFromQueueAddress(sourceAddress), sourceAddress.queueName, msgID, false);
            statements.add(delete);
            GenericCQLDAO.batchExecute(KEYSPACE, statements.toArray(new Statement[statements.size()]));
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
     * @param messagesToRemove
     * @param queueType
     */
    private void deleteMessageMetadataFromColumnFamily(HashMap<String, List<AndesRemovableMetadata>> messagesToRemove,QueueAddress.QueueType queueType) throws AndesException, CassandraDataAccessException {
        for (Map.Entry<String, List<AndesRemovableMetadata>> rowEntry : messagesToRemove.entrySet()) {
            //remove message metadata now
            QueueAddress rowAddress = new QueueAddress(queueType, rowEntry.getKey());
            deleteMessageMetadataFromQueue(rowAddress, rowEntry.getValue());

            if (isMessageCountingAllowed) {
                decrementQueueCount(rowEntry.getKey(), 1);
            }
        }
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
/**
 * Method to delete entries from MESSAGES_FOR_EXPIRY_COLUMN_FAMILY Column Family
 */
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        try {
            List<Statement> statements = new ArrayList<Statement>();
            for (Long messageId : messagesToRemove) {

                CQLQueryBuilder.CqlDelete cqlDelete = new CQLQueryBuilder.CqlDelete(KEYSPACE, CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY);

                cqlDelete.addCondition(CQLDataAccessHelper.MESSAGE_ID, messageId, CassandraHelper.WHERE_OPERATORS.EQ);

                Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);

                statements.add(delete);
            }

            // There is another way. Can delete using a single where clause on the timestamp. (delete all messages with expiration time < current timestamp)
            // But that could result in inconsistent data overall.
            // If a message has been added to the queue between expiry checker invocation and this method, it could be lost.
            // Therefore, the above approach to delete.
            GenericCQLDAO.batchExecute(KEYSPACE, statements.toArray(new Statement[statements.size()]));
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

        Insert insert = CQLQueryBuilder.buildSingleInsert(KEYSPACE, columnFamily, keyValueMap);

        GenericCQLDAO.execute(KEYSPACE, insert.getQueryString());

        log.info("Wrote message " + messageId + " to Column Family " + CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY);

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

    /**
     * Generic interface report the message store about the message status changes.
     * @param messageId The message Id
     * @param timeMillis The timestamp which the change happened
     * @param messageCounterKey The combined key which contains the message queue name and the state it changed to
     * @throws AndesException
     */
    @Override
    public void addMessageStatusChange(long messageId, long timeMillis, MessageCounterKey messageCounterKey) throws AndesException {
        try {
            if (MessageCounterKey.MessageCounterType.PUBLISH_COUNTER.equals(messageCounterKey.getMessageCounterType())) {
                messageStatusInserts.add(CQLDataAccessHelper.insertMessageStatusChange(KEYSPACE, MESSAGE_STATUS_CHANGE_COLUMN_FAMILY, messageId, timeMillis, messageCounterKey, false, statsTimeToLive));

            } else {
                messageStatusInserts.add(CQLDataAccessHelper.updateMessageStatusChange(KEYSPACE, MESSAGE_STATUS_CHANGE_COLUMN_FAMILY, messageId, timeMillis, messageCounterKey, false, statsTimeToLive));
            }

            if (messageStatusInserts.size() > 9999) {
                processMessageStatusInserts();
            }

        } catch (Exception e) {
            //TODO handle Cassandra failures
            //TODO may be we can write those message to a disk, or do something. Currently we will just loose them
            log.error("Error writing incoming messages to Cassandra", e);
            throw new AndesException("Error writing incoming messages to Cassandra", e);
        }
    }

    /**
     *
     * @param queueName The queue name the message is in.
     * @param minDate The min value for the time range to retrieve in timemillis.
     * @param maxDate The max value for the time range to retrieve in timemillis.
     * @param minMessageId The min messageId to retrieve (use for paginated data retrieval. Else null).
     * @param limit Limit of the number of records to retrieve. The messages will be retrieved in ascending messageId order. If null MAX value of long will be set.
     * @param compareAllStatuses Compare all the statuses that are changed within the given period, else only the published time will be compared.
     * @return Message Status data. Map<Message Id, Map<Property, Value>>.
     * @throws AndesException
     */
    @Override
    public Map<Long, Map<String, String>> getMessageStatuses(String queueName, Long minDate, Long maxDate, Long minMessageId, Long limit, Boolean compareAllStatuses) throws AndesException {
        try {
            Map<Long, Map<String, String>> messageStatuses = CQLDataAccessHelper.getMessageStatuses(MESSAGE_STATUS_CHANGE_COLUMN_FAMILY, KEYSPACE, queueName, minDate, maxDate, minMessageId, limit, MessageCounterKey.MessageCounterType.PUBLISH_COUNTER);

            if (compareAllStatuses) {
                messageStatuses.putAll(CQLDataAccessHelper.getMessageStatuses(MESSAGE_STATUS_CHANGE_COLUMN_FAMILY, KEYSPACE, queueName, minDate, maxDate, minMessageId, limit, MessageCounterKey.MessageCounterType.PUBLISH_COUNTER));
                messageStatuses.putAll(CQLDataAccessHelper.getMessageStatuses(MESSAGE_STATUS_CHANGE_COLUMN_FAMILY, KEYSPACE, queueName, minDate, maxDate, minMessageId, limit, MessageCounterKey.MessageCounterType.PUBLISH_COUNTER));
            }
            return messageStatuses;
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }
    }

    /**
     * Get the row cout for the given message status changes within the period.
     *
     * @param queueName The queue name the message is in.
     * @param minDate The min value for the time range to retrieve in timemillis.
     * @param maxDate The max value for the time range to retrieve in timemillis.
     * @return The row count.
     * @throws AndesException
     */
    @Override
    public Long getMessageStatusCount(String queueName, Long minDate, Long maxDate) throws AndesException {
        try{
            return CQLDataAccessHelper.getMessageStatusCount(MESSAGE_STATUS_CHANGE_COLUMN_FAMILY, KEYSPACE, queueName, minDate, maxDate);
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }
    }

    /**
     *
     * @param queueName The queue name the message is in.
     * @param minDate The min value for the time range to retrieve in timemillis.
     * @param maxDate The max value for the time range to retrieve in timemillis.
     * @param minMessageId The min messageId to retrieve (use for paginated data retrieval. Else null).
     * @param limit Limit of the number of records to retrieve. The messages will be retrieved in ascending messageId order. If null MAX value of long will be set.
     * @param rangeColumn Message status change type to compare and return.
     * @return Map<MessageId StatusChangeTime>
     * @throws AndesException
     */
    @Override
    public Map<Long, Long> getMessageStatusChangeTimes(String queueName, Long minDate, Long maxDate, Long minMessageId, Long limit, MessageCounterKey.MessageCounterType rangeColumn) throws AndesException {
        try{
            return CQLDataAccessHelper.getMessageStatusChangeTimes(MESSAGE_STATUS_CHANGE_COLUMN_FAMILY, KEYSPACE, queueName, minDate, maxDate, minMessageId, limit, rangeColumn);
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }
    }

    /**
     * Batch execute collected message status inserts/updates.
     * @throws AndesException
     */
    public void processMessageStatusInserts() throws AndesException {
        try {
            if (messageStatusInserts.size() > 0) {
                // Do a hard copy of the statements from the original list so the original list can be cleaned.
                List<Statement> statementsToExecute;
                synchronized (this.getClass()) {
                    statementsToExecute = new ArrayList<Statement>(messageStatusInserts);
                    initializeMessageStatusInserts();
                }
                GenericCQLDAO.batchExecute(KEYSPACE, statementsToExecute.toArray(new Statement[statementsToExecute.size()]));
            }
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error batch processing message status inserts.", e);
        }
    }

    /**
     * Reset message status insert/update list.
     */
    private void initializeMessageStatusInserts() {
        messageStatusInserts = new ArrayList<Statement>();
    }

    /**
     * Execute buffered message status update statements every 10 seconds.
     */
    @Override
    public void startMessageStatusUpdateExecutor() {
        if (statsEnabled) {
            // Schedule status update to take place after every 10 seconds.
            messageStatusUpdateSchedule = scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        CQLBasedMessageStoreImpl messageStore = (CQLBasedMessageStoreImpl) MessagingEngine.getInstance().getDurableMessageStore();
                        messageStore.processMessageStatusInserts();
                    } catch (AndesException e) {
                        log.error("Error processing message status insert by schedule.", e);
                    }
                }
            }, 0, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Stop executing buffered message status updates every 10 seconds.
     * After invoking, message statuses will be updated only when buffer is overflowed.
     */
    @Override
    public void stopMessageStatusUpdateExecutor() {
        if(statsEnabled) {
            messageStatusUpdateSchedule.cancel(false);
        }
    }
}
