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

import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DtxStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
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
    public long storeDtxRecords(Xid xid, List<AndesMessage> enqueueRecords, List<AndesAckData> dequeueRecords)
            throws AndesException {
        Connection connection = null;
        PreparedStatement storeXidPS = null;
        PreparedStatement storeMetadataPS = null;
        PreparedStatement storeContentPS = null;
        PreparedStatement storeDequeueRecordPS = null;

        String taskDescription = "Storing dtx enqueue record";
        ;
        try {

            connection = rdbmsMessageStore.getConnection();

            storeXidPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_ENTRY);
            long internalXid = uniqueIdGenerator.generateUniqueId();
            storeXidPS.setLong(1, internalXid);
            storeXidPS.setInt(2, xid.getFormatId());
            storeXidPS.setBytes(3, xid.getGlobalTransactionId());
            storeXidPS.setBytes(4, xid.getBranchQualifier());
            storeXidPS.executeUpdate();

            storeMetadataPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_ENQUEUE_METADATA_RECORD);
            storeContentPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_MESSAGE_PART);

            for (AndesMessage message : enqueueRecords) {

                long temporaryMessageId = uniqueIdGenerator.generateUniqueId();

                AndesMessageMetadata metadata = message.getMetadata();
                storeMetadataPS.setLong(1, internalXid);
                storeMetadataPS.setLong(2, temporaryMessageId);
                storeMetadataPS.setString(3, metadata.getDestination());
                storeMetadataPS.setBytes(4, metadata.getMetadata());
                storeMetadataPS.addBatch();

                for (AndesMessagePart messagePart : message.getContentChunkList()) {
                    storeContentPS.setLong(1, temporaryMessageId);
                    storeContentPS.setInt(2, messagePart.getOffset());
                    storeContentPS.setBytes(3, messagePart.getData());
                    storeContentPS.addBatch();
                }
            }

            storeMetadataPS.executeBatch();
            storeContentPS.executeBatch();

            storeDequeueRecordPS = connection.prepareStatement(RDBMSConstants.PS_INSERT_DTX_DEQUEUE_RECORD);

            for (AndesAckData dequeueRecord : dequeueRecords) {
                DeliverableAndesMetadata acknowledgedMessage = dequeueRecord.getAcknowledgedMessage();

                storeDequeueRecordPS.setLong(1, internalXid);
                storeDequeueRecordPS.setLong(2, acknowledgedMessage.getMessageID());
                storeDequeueRecordPS.setLong(
                        3,
                        rdbmsMessageStore.getCachedQueueID(acknowledgedMessage.getStorageQueueName()));
                storeDequeueRecordPS.addBatch();
            }

            storeDequeueRecordPS.executeBatch();

            connection.commit();
            return internalXid;
        } catch (AndesException e) {
            rdbmsMessageStore.rollback(connection, taskDescription);
            throw e;
        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, taskDescription);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while inserting dtx enqueue record", e);
        } finally {
            rdbmsMessageStore.close(storeXidPS, taskDescription);
            rdbmsMessageStore.close(storeMetadataPS, taskDescription);
            rdbmsMessageStore.close(storeContentPS, taskDescription);
            rdbmsMessageStore.close(storeDequeueRecordPS, taskDescription);
            rdbmsMessageStore.close(connection, taskDescription);
        }
    }

    @Override
    public void updateOnCommit(long internalXid, List<AndesMessage> enqueueRecords,
                               List<DeliverableAndesMetadata> dequeueRecords) throws AndesException {
        Connection connection = null;

        String task = "Updating records on dtx.commit ";

        try {
            connection = rdbmsMessageStore.getConnection();
            if (!enqueueRecords.isEmpty()) {
                rdbmsMessageStore.prepareToStoreMessages(connection, enqueueRecords);
            }

            if (!dequeueRecords.isEmpty()) {
                rdbmsMessageStore.prepareToDeleteMessages(connection, dequeueRecords);
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

    @Override
    public void removeDtxRecords(long internalXid) throws AndesException {
        Connection connection = null;
        String task = "Deleting prepared records on dtx.commit ";
        try {
            connection = rdbmsMessageStore.getConnection();
            removePreparedRecords(internalXid, connection);
        } catch (SQLException e) {
            rdbmsMessageStore.rollback(connection, task);
            throw rdbmsStoreUtils.convertSQLException("Error occurred while executing dtx commit event", e);
        } finally {
            rdbmsMessageStore.close(connection, RDBMSConstants.TASK_DELETING_DTX_PREPARED_XID);
        }

    }

    private void removePreparedRecords(long internalXid, Connection connection) throws SQLException {

        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(RDBMSConstants.PS_REMOVE_DTX_PREPARED_XID);
            statement.setLong(1, internalXid);

            statement.execute();
        } finally {
            rdbmsMessageStore.close(statement, RDBMSConstants.TASK_DELETING_DTX_PREPARED_XID);
        }
    }

    @Override
    public boolean isOperational(String testString, long testTime) {
        return rdbmsMessageStore.isOperational(testString, testTime);
    }
}
