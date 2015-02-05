/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

/**
 * CQL 3 based Cassandra MessageStore implementation. This is intended to support Cassandra 2.xx series upwards.
 */
public class CQLBasedMessageStoreImpl implements MessageStore {

    // Message Store tables
    protected static final String CONTENT_TABLE = "MB_CONTENT";
    protected static final String METADATA_TABLE = "MB_METADATA";
    // Message Store table columns
    protected static final String MESSAGE_ID = "MESSAGE_ID";

    protected static final String QUEUE_NAME = "QUEUE_NAME";
    protected static final String METADATA = "MESSAGE_METADATA";
    protected static final String MESSAGE_OFFSET = "CONTENT_OFFSET";
    protected static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";

    // Message expiration feature moved to MB 3.1.0
/*
    protected static final String EXPIRATION_TABLE = "MB_EXPIRATION_DATA";
    protected static final String EXPIRATION_TIME = "EXPIRATION_TIME";
    protected static final String DESTINATION_QUEUE = "MESSAGE_DESTINATION";
    protected static final String EXPIRATION_TIME_RANGE = "TIME_RANGE";
*/
    private CassandraConfig config;
    private CQLConnection cqlConnection;

    /**
     * CQL prepared statement to insert message content to DB
     * Params to bind
     * - MESSAGE_ID
     * - MESSAGE_OFFSET
     * - MESSAGE_CONTENT
     */
    private static final String PS_INSERT_MESSAGE_PART =
            "INSERT INTO " + CONTENT_TABLE + " ( " +
                    MESSAGE_ID + ", " + MESSAGE_OFFSET + ", " +
                    MESSAGE_CONTENT + ") " +
                    "VALUES (?,?,?)";

    /**
     * CQL prepared statement to delete message content of a given message ID
     * Params to bind
     * - MESSAGE_ID
     */
    private static final String PS_DELETE_MESSAGE_CONTENT =
            "DELETE FROM " + CONTENT_TABLE +
                    " WHERE " + MESSAGE_ID + " =?";

    /**
     * CQL prepared statement to insert message metadata to DB
     * Params to bind
     * - QUEUE_NAME
     * - MESSAGE_ID
     * - METADATA
     */
    private static final String PS_INSERT_METADATA =
            "INSERT INTO " + METADATA_TABLE + " ( " +
                    QUEUE_NAME + "," + MESSAGE_ID + "," + METADATA + ") " +
                    " VALUES (?,?,?);";

    /**
     * CQL prepared statement to delete metadata from DB
     * Params to bind:
     * - MESSAGE_ID
     */
    private static final String PS_DELETE_METADATA =
            "DELETE FROM " + METADATA_TABLE +
                    " WHERE " + QUEUE_NAME + "=? AND " +
                    MESSAGE_ID + "=?";

    // Prepared Statements bound to session
    private PreparedStatement psInsertMessagePart;
    private PreparedStatement psDeleteMessagePart;
    private PreparedStatement psInsertMetadata;
    private PreparedStatement psDeleteMetadata;

    public CQLBasedMessageStoreImpl() {
        config = new CassandraConfig();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection initializeMessageStore(ConfigurationProperties connectionProperties) throws AndesException {

        cqlConnection = new CQLConnection();
        cqlConnection.initialize(connectionProperties);

        config.parse(connectionProperties);
        createSchema(cqlConnection);

        Session session = cqlConnection.getSession();

        // CQL we can keep the prepared statements for a given session.
        // creating prepared statements per method request is not efficient
        psInsertMessagePart = session.prepare(PS_INSERT_MESSAGE_PART);
        psDeleteMessagePart = session.prepare(PS_DELETE_MESSAGE_CONTENT);
        psInsertMetadata = session.prepare(PS_INSERT_METADATA);
        psDeleteMetadata = session.prepare(PS_DELETE_METADATA);

        return cqlConnection;
    }

    /**
     * Uses the connection and connection properties to create a new key space if the current KeySpace doesn't exist
     *
     * @param connection CQLConnection
     */
    private void createSchema(CQLConnection connection) {

        Session session = connection.getSession();

        Statement statement = SchemaBuilder.createTable(config.getKeyspace(), CONTENT_TABLE).ifNotExists().
                addPartitionKey(MESSAGE_ID, DataType.bigint()).
                addClusteringColumn(MESSAGE_OFFSET, DataType.cint()).
                addColumn(MESSAGE_CONTENT, DataType.blob()).
                withOptions().gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());
        session.execute(statement);

        statement = SchemaBuilder.createTable(config.getKeyspace(), METADATA_TABLE).ifNotExists().
                addPartitionKey(QUEUE_NAME, DataType.text()).
                addClusteringColumn(MESSAGE_ID, DataType.bigint()).
                addColumn(METADATA, DataType.blob()).
                withOptions().clusteringOrder(MESSAGE_ID, SchemaBuilder.Direction.ASC).
                gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());
        session.execute(statement);

        // Message expiration feature moved to MB 3.1.0
/*
        statement = SchemaBuilder.createTable(config.getKeyspace(), EXPIRATION_TABLE).ifNotExists().
                addPartitionKey(EXPIRATION_TIME_RANGE, DataType.bigint()).
                addClusteringColumn(EXPIRATION_TIME, DataType.bigint()).
                addColumn(MESSAGE_ID, DataType.bigint()).
                addColumn(DESTINATION_QUEUE, DataType.bigint()).
                withOptions().gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());
        session.execute(statement);
*/
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {

        BatchStatement batchStatement = new BatchStatement();
        batchStatement.setConsistencyLevel(config.getWriteConsistencyLevel());

        for (AndesMessagePart andesMessagePart : partList) {
            batchStatement.add(psInsertMessagePart.bind(
                            andesMessagePart.getMessageID(),
                            andesMessagePart.getOffSet(),
                            ByteBuffer.wrap(andesMessagePart.getData()))
            );
        }

        execute(batchStatement, "adding message parts list. List size " + partList.size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageParts(Collection<Long> messageIdList) throws AndesException {

        BatchStatement batchStatement = new BatchStatement();
        batchStatement.setConsistencyLevel(config.getWriteConsistencyLevel());

        for (Long messageID : messageIdList) {
            batchStatement.add(psDeleteMessagePart.bind(messageID));
        }

        execute(batchStatement, "deleting message part list. List size " + messageIdList.size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {

        Statement statement = QueryBuilder.select().all().
                from(config.getKeyspace(), CONTENT_TABLE).
                where(eq(MESSAGE_ID, messageId)).and(eq(MESSAGE_OFFSET, offsetValue)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving message part for msg id " + messageId +
                " offset " + offsetValue);

        AndesMessagePart messagePart = null;
        Row row = resultSet.one();
        if (null != row) {
            ByteBuffer buffer = row.getBytes(MESSAGE_CONTENT);
            byte[] content = new byte[buffer.remaining()];
            buffer.get(content);

            messagePart = new AndesMessagePart();
            messagePart.setMessageID(row.getLong(MESSAGE_ID));
            messagePart.setOffSet(row.getInt(MESSAGE_OFFSET));
            messagePart.setData(content);
            messagePart.setDataLength(content.length);
        }

        return messagePart;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {

        BatchStatement batchStatement = new BatchStatement();

        for (AndesMessageMetadata metadata : metadataList) {
            addMetadataToBatch(batchStatement, metadata, metadata.getStorageQueueName());
        }

        execute(batchStatement, " adding metadata list. list size " + metadataList.size());

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {

        BatchStatement batchStatement = new BatchStatement();
        addMetadataToBatch(batchStatement, metadata, metadata.getStorageQueueName());

        execute(batchStatement, "adding metadata with msg id " + metadata.getMessageID() + " storage queue "
                + metadata.getStorageQueueName());
    }

    /**
     * Helper method to avoid code duplication for binding andes message to prepared statement.
     * In an event of change in metadata insertion query only need to chang the logic in this method
     *
     * @param batchStatement BatchStatement
     * @param metadata       AndesMessageMetadata to be added to DB
     * @param queueName      storage queue name
     */
    private void addMetadataToBatch(BatchStatement batchStatement,
                                    AndesMessageMetadata metadata,
                                    String queueName) {

        batchStatement.add(psInsertMetadata.bind(
                        queueName,
                        metadata.getMessageID(),
                        ByteBuffer.wrap(metadata.getMetadata()))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaDataToQueue(String queueName,
                                   AndesMessageMetadata metadata) throws AndesException {

        BatchStatement batchStatement = new BatchStatement();
        addMetadataToBatch(batchStatement, metadata, queueName);

        execute(batchStatement, "adding metadata to queue " + queueName +
                " with msg id " + metadata.getMessageID());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadataToQueue(String queueName,
                                   List<AndesMessageMetadata> metadataList) throws AndesException {

        BatchStatement batchStatement = new BatchStatement();

        for (AndesMessageMetadata metadata : metadataList) {
            addMetadataToBatch(batchStatement, metadata, queueName);
        }

        execute(batchStatement, "adding metadata list to queue ");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetaDataToQueue(long messageId,
                                    String currentQueueName,
                                    String targetQueueName) throws AndesException {
        List<AndesMessageMetadata> metadataList = getNextNMessageMetadataFromQueue(currentQueueName, messageId, 1);

        if (metadataList.isEmpty()) {
            throw new AndesException("Message MetaData not found to move the message to Dead Letter Channel");
        }

        List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>(1);
        removableMetadataList.add(new AndesRemovableMetadata(messageId, currentQueueName, currentQueueName));

        addMetaDataToQueue(targetQueueName, metadataList.get(0));
        deleteMessageMetadataFromQueue(currentQueueName, removableMetadataList);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMetaDataInformation(String currentQueueName,
                                          List<AndesMessageMetadata> metadataList) throws AndesException {

        // Step 1 add metadata to the new queue
        addMetaData(metadataList);

        // Step 2 - Delete the old meta data when inserting new meta is complete to avoid
        // losing messages
        List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>(metadataList.size());
        for (AndesMessageMetadata metadata : metadataList) {
            removableMetadataList.add(new AndesRemovableMetadata(metadata.getMessageID(),
                            metadata.getDestination(),
                            currentQueueName)
            );
        }
        deleteMessageMetadataFromQueue(currentQueueName, removableMetadataList);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeRetainedMessages(List<AndesMessage> retainList) throws AndesException {
        // TODO: implement this method
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllRetainedTopics() throws AndesException {
        // TODO: implement this method
        return  null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getRetainedMetaData(String destination) throws AndesException {
        // TODO: implement this method
        return  null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {
        // TODO: implement this method
        return  null;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This method is very expensive in Cassandra. The partition key of table is queue name. Therefore to get the
     * metadata with given message id query needs to go through different queues (rows in table) stored in different
     * nodes in Cassandra cluster.
     * <p/>
     * USE THIS WITH CAUTION! DON'T USE IN CRITICAL PATH
     */
    @Override
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException {

        AndesMessageMetadata metadata = null;

        Statement statement = QueryBuilder.select().column(METADATA).
                from(config.getKeyspace(), METADATA_TABLE).
                where(eq(MESSAGE_ID, messageId)).
                limit(1).
                allowFiltering().
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving metadata for msg id " + messageId);
        Row row = resultSet.one();

        if (null != row) {
            metadata = getMetadataFromRow(row, messageId);
        }

        return metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName,
                                                      long firstMsgId,
                                                      long lastMsgID) throws AndesException {

        Statement statement = QueryBuilder.select().column(MESSAGE_ID).column(METADATA).
                from(config.getKeyspace(), METADATA_TABLE).
                where(eq(QUEUE_NAME, queueName)).
                and(gte(MESSAGE_ID, firstMsgId)).
                and(lte(MESSAGE_ID, lastMsgID)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving metadata list from queue " + queueName +
                "between msg id " + firstMsgId + " and " + lastMsgID);
        List<AndesMessageMetadata> metadataList =
                new ArrayList<AndesMessageMetadata>(resultSet.getAvailableWithoutFetching());

        for (Row row : resultSet) {
            metadataList.add(getMetadataFromRow(row, row.getLong(MESSAGE_ID)));
        }

        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String storageQueueName,
                                                                       long firstMsgId,
                                                                       int count) throws AndesException {

        Statement statement = QueryBuilder.select().column(METADATA).column(MESSAGE_ID).
                from(config.getKeyspace(), METADATA_TABLE).
                where(eq(QUEUE_NAME, storageQueueName)).
                and(gte(MESSAGE_ID, firstMsgId)).
                limit(count).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving metadata list from " + storageQueueName +
                " with starting msg id " + firstMsgId + " and limit " + count);
        List<AndesMessageMetadata> metadataList =
                new ArrayList<AndesMessageMetadata>(resultSet.getAvailableWithoutFetching());

        for (Row row : resultSet) {
            metadataList.add(getMetadataFromRow(row, row.getLong(MESSAGE_ID)));
        }

        return metadataList;
    }

    /**
     * Retrieves AndesMessageMetadata from a given row. (ResultSet row)
     * @param row Row
     * @param messageID messageID of metadata
     * @return AndesMessageMetadata
     */
    private AndesMessageMetadata getMetadataFromRow(Row row, long messageID) {
        ByteBuffer buffer = row.getBytes(METADATA);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new AndesMessageMetadata(messageID, bytes, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(String storageQueueName,
                                               List<AndesRemovableMetadata> messagesToRemove) throws AndesException {

        BatchStatement batchStatement = new BatchStatement();

        for (AndesRemovableMetadata metadata : messagesToRemove) {
            batchStatement.add(psDeleteMetadata.bind(
                    storageQueueName,
                    metadata.getMessageID()
            ));
        }

        execute(batchStatement, "deleting metadata list from " + storageQueueName +
                " list size " + messagesToRemove.size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        // Message expiration feature moved to MB 3.1.0
        return new ArrayList<AndesRemovableMetadata>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        // Message expiration feature moved to MB 3.1.0
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageToExpiryQueue(Long messageId,
                                        Long expirationTime,
                                        boolean isMessageForTopic,
                                        String destination) throws AndesException {
        // Message expiration feature moved to MB 3.1.0
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllMessageMetadata(String storageQueueName) throws AndesException {

        Statement statement = QueryBuilder.delete().from(config.getKeyspace(), METADATA_TABLE).
                where(eq(QUEUE_NAME, storageQueueName)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "deleting all metadata from " + storageQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deleteAllMessageMetadataFromDLC(String storageQueueName,
                                               String DLCQueueName) throws AndesException {

        Statement query = QueryBuilder.select().column(MESSAGE_ID).column(METADATA).
                from(config.getKeyspace(), METADATA_TABLE).
                where(eq(QUEUE_NAME, DLCQueueName)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(query, "retrieving metadata from DLC " + DLCQueueName);

        // Internally CQL retrieves results as pages. Doesn't load all the result at once to client side
        // using the same fetch size to iterate through the messages and delete from DLC
        int fetchSize = query.getFetchSize();
        int removedMessageCount = 0;
        List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>(fetchSize);
        AndesMessageMetadata metadata;

        for (Row row : resultSet) {
            metadata = getMetadataFromRow(row, row.getLong(MESSAGE_ID));

            if (metadata.getStorageQueueName().equals(storageQueueName)) {

                AndesRemovableMetadata removableMetadata = new AndesRemovableMetadata(
                        metadata.getMessageID(),
                        metadata.getDestination(),
                        metadata.getStorageQueueName()
                );
                removableMetadataList.add(removableMetadata);
            }

            // When the end of current fetched results is reached we delete that found removable messages from DLC
            if (resultSet.getAvailableWithoutFetching() == 0 && !removableMetadataList.isEmpty()) {
                deleteMessageMetadataFromQueue(DLCQueueName, removableMetadataList);
                removedMessageCount = removedMessageCount + removableMetadataList.size();
                removableMetadataList.clear();
            }
        }

        return removedMessageCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getMessageIDsAddressedToQueue(String storageQueueName) throws AndesException {

        Statement statement = QueryBuilder.select().column(MESSAGE_ID).
                from(config.getKeyspace(), METADATA_TABLE).
                where(eq(QUEUE_NAME, storageQueueName)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving message ids addressed to " + storageQueueName);
        List<Long> msgIDList = new ArrayList<Long>(resultSet.getAvailableWithoutFetching());

        for (Row row : resultSet) {
            msgIDList.add(row.getLong(MESSAGE_ID));
        }
        return msgIDList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        cqlConnection.close();
    }

    /**
     * Executes the statement using the given session and returns the resultSet.
     * Additionally this catches any NoHostAvailableException or QueryExecutionException thrown by CQL driver and
     * rethrows an AndesException
     *
     * @param statement statement to be executed
     * @param task description of the task that is done by the statement
     * @return ResultSet
     * @throws AndesException
     */
    private ResultSet execute(Statement statement, String task) throws AndesException {
        try {
            return cqlConnection.getSession().execute(statement);
        } catch (NoHostAvailableException e) {
            throw new AndesException("Error occurred while " + task, e);
        } catch (QueryExecutionException e) {
            throw new AndesException("Error occurred while " + task, e);
        }
    }
}
