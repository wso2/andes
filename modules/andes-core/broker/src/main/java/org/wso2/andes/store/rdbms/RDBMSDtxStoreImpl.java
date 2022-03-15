/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store.rdbms;

import org.wso2.andes.dtx.XidImpl;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DtxStore;
import org.wso2.andes.kernel.dtx.AndesPreparedMessageMetadata;
import org.wso2.andes.kernel.dtx.DtxBranch;
import org.wso2.andes.server.ClusterResourceHolder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.transaction.xa.Xid;

/**
 * Implementation of distributed transaction store. This supports RDBMS
 */
public class RDBMSDtxStoreImpl implements DtxStore {

    private RDBMSMessageStoreImpl rdbmsMessageStore;

    private RDBMSStoreUtils rdbmsStoreUtils;

    /**
     * Instead of using auto increment IDs which is JDBS driver dependant, we can use this
     * to generate unique IDs.
     */
    private Andes uniqueIdGenerator;

    RDBMSDtxStoreImpl(RDBMSMessageStoreImpl rdbmsMessageStore, RDBMSStoreUtils rdbmsStoreUtils) {
        this.rdbmsMessageStore = rdbmsMessageStore;
        this.rdbmsStoreUtils = rdbmsStoreUtils;
        uniqueIdGenerator = Andes.getInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long storeDtxRecords(Xid xid, List<AndesMessage> enqueueRecords,
                                List<? extends AndesMessageMetadata> dequeueRecords) throws AndesException {

        Connection connection = null;
        String taskDescription = "Storing dtx enqueue/dequeue records for dtx.prepare";

        try {

            connection = rdbmsMessageStore.getConnection();

            long internalXid = prepareToStoreXid(xid, connection);

            prepareToStoreEnqueuedRecords(enqueueRecords, internalXid, connection);

            if (!dequeueRecords.isEmpty()) {
                prepareToBackupDequeueRecords(dequeueRecords, internalXid, connection);
                rdbmsMessageStore.prepareToDeleteMessages(connection, dequeueRecords);
            }

            connection.commit();
            return internalXid;

        } catch (AndesException e) {
            rdbmsMessageStore.rollback(connection, taskDescription);
            throw e;
        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, taskDescription);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while storing dtx records for dtx.prepare", e);
        } finally {
            rdbmsMessageStore.close(connection, taskDescription);
        }
    }

    /**
     * Set dequeued records to the prepared statements
     *
     * @param dequeueRecords {@link List} od acknowledged messages
     * @param internalXid internal {@link Xid}
     * @param connection {@link Connection} to database
     * @throws SQLException Thrown when JDBC errors occur when batching
     * @throws AndesException Thrown when errors occur in retrieving the cached queue ID
     */
    private void prepareToBackupDequeueRecords(List<? extends AndesMessageMetadata> dequeueRecords, long internalXid,
                                               Connection connection) throws SQLException, AndesException {

        PreparedStatement storeDequeueRecordMetadataPS = null;
        PreparedStatement backupDequeueMessagesPS = null;

        String taskDescription = "prepare to store dequeued messages for dtx prepare stage";
        try {
            storeDequeueRecordMetadataPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_DEQUEUE_RECORD);
            backupDequeueMessagesPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_DEQUEUE_MESSAGE_PART);
            for (AndesMessageMetadata messageMetadata : dequeueRecords) {

                storeDequeueRecordMetadataPS.setLong(1, internalXid);
                storeDequeueRecordMetadataPS.setLong(2, messageMetadata.getMessageID());
                storeDequeueRecordMetadataPS.setString(3, messageMetadata.getStorageQueueName());
                storeDequeueRecordMetadataPS.setBinaryStream(4,
                        new ByteArrayInputStream(messageMetadata.getMetadata()));
                storeDequeueRecordMetadataPS.addBatch();

                backupDequeueMessagesPS.setLong(1, internalXid);
                backupDequeueMessagesPS.setLong(2, messageMetadata.getMessageID());
                backupDequeueMessagesPS.addBatch();
            }

            storeDequeueRecordMetadataPS.executeBatch();
            backupDequeueMessagesPS.executeBatch();
        } finally {
            rdbmsMessageStore.close(storeDequeueRecordMetadataPS, taskDescription);
            rdbmsMessageStore.close(backupDequeueMessagesPS, taskDescription);
        }
    }

    /**
     * Store enqueued messages using the provided database connection
     *
     * @param enqueueRecords {@link List} of {@link AndesMessage} to store
     * @param internalXid internal Xid of the transaction
     * @param connection JDBC {@link Connection}
     * @throws SQLException throws {@link SQLException} on database driver related exception
     */
    private void prepareToStoreEnqueuedRecords(List<AndesMessage> enqueueRecords, long internalXid,
                                               Connection connection) throws SQLException {
        PreparedStatement storeMetadataPS = null;
        PreparedStatement storeContentPS = null;

        String taskDescription = "prepare to store enqueued records for dtx prepare stage";
        try {
            storeMetadataPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_ENQUEUE_METADATA_RECORD);
            storeContentPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_ENQUEUE_MESSAGE_PART);
            for (AndesMessage message : enqueueRecords) {

                long temporaryMessageId = uniqueIdGenerator.generateUniqueId();

                AndesMessageMetadata metadata = message.getMetadata();
                storeMetadataPS.setLong(1, internalXid);
                storeMetadataPS.setLong(2, temporaryMessageId);
                storeMetadataPS.setBinaryStream(3, new ByteArrayInputStream(metadata.getMetadata()));
                storeMetadataPS.addBatch();

                for (AndesMessagePart messagePart : message.getContentChunkList()) {
                    storeContentPS.setLong(1,internalXid);
                    storeContentPS.setLong(2, temporaryMessageId);
                    storeContentPS.setInt(3, messagePart.getOffset());
                    storeContentPS.setBinaryStream(4, new ByteArrayInputStream(messagePart.getData()));
                    storeContentPS.addBatch();
                }
            }
            storeMetadataPS.executeBatch();
            storeContentPS.executeBatch();
        } finally {
            rdbmsMessageStore.close(storeMetadataPS, taskDescription);
            rdbmsMessageStore.close(storeContentPS, taskDescription);
        }
    }

    /**
     * Store the provided xid in the dtx store
     * @param xid {@link Xid} object to be stored
     * @param connection JDBC {@link Connection}
     * @return return the internal xid relating to the {@link Xid}
     * @throws SQLException throws {@link SQLException} on database driver related exception
     */
    private long prepareToStoreXid(Xid xid, Connection connection) throws SQLException {
        PreparedStatement storeXidPS = null;
        String taskDescription = "prepare to store new Xid";
        try {
            storeXidPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_ENTRY);
            String nodeId = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();

            long internalXid = uniqueIdGenerator.generateUniqueId();
            storeXidPS.setLong(1, internalXid);
            storeXidPS.setString(2, nodeId);
            storeXidPS.setInt(3, xid.getFormatId());
            storeXidPS.setBinaryStream(4, new ByteArrayInputStream(xid.getGlobalTransactionId()));
            storeXidPS.setBinaryStream(5, new ByteArrayInputStream(xid.getBranchQualifier()));
            storeXidPS.executeUpdate();
            return internalXid;
        } finally {
            rdbmsMessageStore.close(storeXidPS, taskDescription);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOnCommit(long internalXid, List<AndesMessage> enqueueRecords) throws AndesException {
        Connection connection = null;

        String task = "Updating records on dtx.commit ";

        try {
            connection = rdbmsMessageStore.getConnection();
            if (!enqueueRecords.isEmpty()) {
                rdbmsMessageStore.prepareToStoreMessages(connection, enqueueRecords);
            }

            removePreparedRecords(internalXid, connection);
            connection.commit();

        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, task);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while executing dtx commit event", e);
        } finally {
            rdbmsMessageStore.close(connection, RDBMSConstants.TASK_DTX_COMMIT);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void updateOnOnePhaseCommit(List<AndesMessage> enqueueRecords, List<AndesPreparedMessageMetadata> dequeueRecordsMetadata) throws
            AndesException {
        Connection connection = null;

        String task = "Updating records on dtx.onephase.commit ";

        try {
            connection = rdbmsMessageStore.getConnection();
            if (!enqueueRecords.isEmpty()) {
                rdbmsMessageStore.prepareToStoreMessages(connection, enqueueRecords);
            }

            if (!dequeueRecordsMetadata.isEmpty()) {
                rdbmsMessageStore.prepareToDeleteMessages(connection, dequeueRecordsMetadata);
            }
            connection.commit();

        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, task);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while executing dtx commit event", e);
        } finally {
            rdbmsMessageStore.close(connection, RDBMSConstants.TASK_DTX_COMMIT);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOnRollback(long internalXid, List<AndesPreparedMessageMetadata> messagesToRestore) throws AndesException {
        Connection connection = null;
        String task = "Rolling back records on dtx.rollback ";
        try {
            connection = rdbmsMessageStore.getConnection();
            restoreRolledBackAcknowledgedMessages(messagesToRestore, connection);
            removePreparedRecords(internalXid, connection);
            connection.commit();
        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, task);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while executing dtx commit event", e);
        } finally {
            rdbmsMessageStore.close(connection, RDBMSConstants.TASK_DELETING_DTX_PREPARED_XID);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long recoverBranchData(DtxBranch branch, String nodeId) throws AndesException {
        Connection connection = null;
        String task = "Recovering DtxBranch in node " + nodeId;
        try {
            connection = rdbmsMessageStore.getConnection();
            long internalXid = getInternalXid(branch.getXid(), nodeId, connection);
            if (internalXid == DtxBranch.NULL_XID) {
                return DtxBranch.NULL_XID;
            }
            recoverEnqueueMessages(internalXid, branch, connection);
            recoverDequeueMessages(internalXid, branch, connection);
            connection.commit();
            return internalXid;
        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, task);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while recovering DtxBranch ", e);
        } finally {
            rdbmsMessageStore.close(connection, task);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<XidImpl> getStoredXidSet(String nodeId) throws AndesException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String task = "Retrieving stored xids for node " + nodeId;
        Set<XidImpl> xidSet = new HashSet<>();
        ResultSet resultSet = null;
        try {
            connection = rdbmsMessageStore.getConnection();
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_PREPARED_XID);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                        InputStream inputStreamBranchId = resultSet.getBinaryStream(RDBMSConstants.DTX_BRANCH_ID);
                        byte[] b1 = rdbmsStoreUtils.getBytesFromInputStream(inputStreamBranchId);
                        InputStream inputStreamGlobalId = resultSet.getBinaryStream(RDBMSConstants.DTX_GLOBAL_ID);
                        byte[] b2 = rdbmsStoreUtils.getBytesFromInputStream(inputStreamGlobalId);
                        XidImpl xid = new XidImpl(b1, resultSet.getInt(RDBMSConstants.DTX_FORMAT), b2);
                        xidSet.add(xid);
            }
            connection.commit();
            return xidSet;
        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, task);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while recovering DtxBranch ", e);
        } finally {
            rdbmsMessageStore.close(resultSet, task);
            rdbmsMessageStore.close(preparedStatement, task);
            rdbmsMessageStore.close(connection, task);
        }
    }

    /**
     * Retrieve the dequeued messages from the dtx store
     * @param internalXid internal Xid of the transaction
     * @param branch {@link DtxBranch} of the transaction
     * @param connection JDBC {@link Connection}
     * @throws AndesException throws {@link AndesException} related to database error
     */
    private void recoverDequeueMessages(long internalXid, DtxBranch branch, Connection connection)
            throws AndesException {
        PreparedStatement preparedStatement = null;
        String task = "Recovering dequeued messages for internal xid " + internalXid;
        try {
            preparedStatement = connection.prepareStatement(RDBMSConstants.PS_SELECT_DTX_DEQUEUE_METADATA);
            preparedStatement.setLong(1, internalXid);
            ResultSet resultSet = preparedStatement.executeQuery();

            List<AndesPreparedMessageMetadata> dtxMetadataList = new ArrayList<>();
            while (resultSet.next()) {
                InputStream inputStream = resultSet.getBinaryStream(RDBMSConstants.METADATA);
                byte[] b = rdbmsStoreUtils.getBytesFromInputStream(inputStream);
                AndesMessageMetadata metadata = new AndesMessageMetadata(
                        resultSet.getLong(RDBMSConstants.MESSAGE_ID), b, true);
                metadata.setStorageQueueName(resultSet.getString(RDBMSConstants.QUEUE_NAME));
                AndesPreparedMessageMetadata dtxMetadata = new AndesPreparedMessageMetadata(metadata);
                dtxMetadataList.add(dtxMetadata);
            }
            branch.setMessagesToRestore(dtxMetadataList);
        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, task);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while recovering DtxBranch ", e);
        }
    }

    /**
     * Retrieve enqueued incoming messages that were not committed but prepared
     *
     * @param internalXid internal Xid of the transaction
     * @param branch {@link DtxBranch} of the transaction
     * @param connection JDBC {@link Connection}
     * @throws AndesException throws {@link AndesException} on database error
     */
    private void recoverEnqueueMessages(long internalXid, DtxBranch branch, Connection connection)
            throws AndesException {
        PreparedStatement retrieveMetadataPS = null;
        PreparedStatement retrieveContentPS = null;
        ResultSet metadataResultSet = null;
        ResultSet contentResultSet = null;
        String task = "Recovering enqueue records for internal xid " + internalXid;
        try {
            retrieveMetadataPS = connection.prepareStatement(RDBMSConstants.PS_SELECT_DTX_ENQUEUED_METADATA);
            retrieveContentPS = connection.prepareStatement(RDBMSConstants.PS_SELECT_DTX_ENQUEUED_CONTENT);

            retrieveMetadataPS.setLong(1,internalXid);
            metadataResultSet = retrieveMetadataPS.executeQuery();
            Map<Long, AndesMessage> messageMap = new HashMap<>();
            while (metadataResultSet.next()) {
                InputStream inputStream = metadataResultSet.getBinaryStream(RDBMSConstants.METADATA);
                byte[] b = rdbmsStoreUtils.getBytesFromInputStream(inputStream);
                AndesMessageMetadata metadata = new AndesMessageMetadata(
                        metadataResultSet.getLong(RDBMSConstants.MESSAGE_ID), b, true);
                AndesMessage andesMessage = new AndesMessage(metadata);
                messageMap.put(metadata.getMessageID(), andesMessage);
            }
            retrieveContentPS.setLong(1, internalXid);
            contentResultSet = retrieveContentPS.executeQuery();
            while (contentResultSet.next()) {
                long messageId = contentResultSet.getLong(RDBMSConstants.MESSAGE_ID);
                AndesMessage message = messageMap.get(messageId);

                AndesMessagePart contentChunk = new AndesMessagePart();

                contentChunk.setMessageID(messageId);
                contentChunk.setOffSet(contentResultSet.getInt(RDBMSConstants.MSG_OFFSET));
                InputStream inputStream = contentResultSet.getBinaryStream(RDBMSConstants.MESSAGE_CONTENT);
                byte[] data = rdbmsStoreUtils.getBytesFromInputStream(inputStream);
                contentChunk.setData(data);

                message.addMessagePart(contentChunk);
            }
            branch.setMessagesToStore(messageMap.values());

        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, task);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while recovering enqueued messages for " +
                                                              "internal xid " + internalXid, e);
        } finally {
            rdbmsMessageStore.close(metadataResultSet, task);
            rdbmsMessageStore.close(retrieveMetadataPS, task);
            rdbmsMessageStore.close(contentResultSet, task);
            rdbmsMessageStore.close(retrieveContentPS, task);
        }
    }

    /**
     * Retrieve the internal Xid from dtx store
     *
     * @param xid {@link Xid} of the transaction
     * @param nodeId Node id of the server
     * @param connection JDBC {@link Connection}
     * @return internal Xid
     * @throws AndesException throws {@link AndesException} on database error
     */
    private long getInternalXid(Xid xid, String nodeId, Connection connection) throws AndesException {
        PreparedStatement retreiveXidPS = null;
        String task = "Recovering DtxBranch in node " + nodeId;
        try {
            retreiveXidPS = connection.prepareStatement(RDBMSConstants.PS_SELECT_INTERNAL_XID);
            retreiveXidPS.setLong(1, xid.getFormatId());
            retreiveXidPS.setBinaryStream(2, new ByteArrayInputStream(xid.getBranchQualifier()));
            retreiveXidPS.setBinaryStream(3, new ByteArrayInputStream(xid.getGlobalTransactionId()));
            retreiveXidPS.setString(4, nodeId);
            ResultSet resultSet = retreiveXidPS.executeQuery();

            if (resultSet.first()) {
                return resultSet.getLong(RDBMSConstants.INTERNAL_XID);
            }
            return DtxBranch.NULL_XID;
        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, task);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while recovering DtxBranch internal Xid ", e);
        } finally {
            rdbmsMessageStore.close(retreiveXidPS, task);
        }


    }

    /**
     * Restore acknowledged but not committed messages back in the message store
     *
     * @param messagesToRestore {@link List} of {@link AndesPreparedMessageMetadata}
     * @param connection JDBC {@link Connection}
     * @throws SQLException throws {@link SQLException} on JDBC driver exceptions
     * @throws AndesException throws {@link SQLException} on JDBC driver exceptions
     */
    private void restoreRolledBackAcknowledgedMessages(List<AndesPreparedMessageMetadata> messagesToRestore,
                                                       Connection connection) throws SQLException, AndesException {

        PreparedStatement storeMetadataPS =  null;
        PreparedStatement restoreContentPS = null;
        String taskDescription = "Restore acknowledged messages on dtx.rollback";

        try {
            storeMetadataPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_METADATA);
            restoreContentPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_RESTORE_ACKED_MESSAGE_PART);

            for (AndesPreparedMessageMetadata metadata : messagesToRestore) {
                storeMetadataPS.setLong(1, metadata.getMessageID());
                storeMetadataPS.setLong(2,
                                        rdbmsMessageStore.getCachedQueueID( metadata.getStorageQueueName()));
                storeMetadataPS.setBinaryStream(3, new ByteArrayInputStream(metadata.getMetadata()));
                storeMetadataPS.addBatch();

                restoreContentPS.setLong(1, metadata.getMessageID());
                restoreContentPS.setLong(2, metadata.getOldMessageId());
                restoreContentPS.addBatch();
            }

            storeMetadataPS.executeBatch();
            restoreContentPS.executeBatch();

        } finally {
            rdbmsMessageStore.close(storeMetadataPS, taskDescription);
            rdbmsMessageStore.close(restoreContentPS, taskDescription);
        }
    }

    /**
     * Remove the prepared dtx records from the database using the provided {@link Connection}. Executed statements
     * are not committed within the method. It is the responsibility of the caller to commit the changes
     *
     * @param internalXid internal {@link Xid}
     * @param connection {@link Connection}
     * @throws SQLException Throws when there is an JDBC driver level exception
     */
    private void removePreparedRecords(long internalXid, Connection connection) throws SQLException {

        PreparedStatement statement = null;
        try {
            String nodeId = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
            statement = connection.prepareStatement(RDBMSConstants.PS_DELETE_DTX_ENTRY);
            statement.setLong(1, internalXid);
            statement.setString(2, nodeId);
            statement.execute();
        } finally {
            rdbmsMessageStore.close(statement, RDBMSConstants.TASK_DELETING_DTX_PREPARED_XID);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        return rdbmsMessageStore.isOperational(testString, testTime);
    }
}
