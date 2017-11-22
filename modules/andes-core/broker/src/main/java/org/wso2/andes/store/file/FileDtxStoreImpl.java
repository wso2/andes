/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/

package org.wso2.andes.store.file;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.transaction.xa.Xid;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

import static org.wso2.andes.store.file.FileStoreConstants.DELIMITER;
import static org.wso2.andes.store.file.FileStoreConstants.DESTINATION_NAME;
import static org.wso2.andes.store.file.FileStoreConstants.DLC;
import static org.wso2.andes.store.file.FileStoreConstants.DLC_STATUS;
import static org.wso2.andes.store.file.FileStoreConstants.DTX_ENTRY;
import static org.wso2.andes.store.file.FileStoreConstants.DTX_ENQUEUE;
import static org.wso2.andes.store.file.FileStoreConstants.DTX_DEQUEUE;
import static org.wso2.andes.store.file.FileStoreConstants.DTX_XID;
import static org.wso2.andes.store.file.FileStoreConstants.FORMAT_CODE;
import static org.wso2.andes.store.file.FileStoreConstants.GLOBAL_ID;
import static org.wso2.andes.store.file.FileStoreConstants.BRANCH_ID;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE_CONTENT;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE_METADATA;
import static org.wso2.andes.store.file.FileStoreConstants.START_OFFSET;
import static org.wso2.andes.store.file.FileStoreConstants.XID_IDENTIFIER;

/**
 * Implementation of file based distributed transaction store. This supports LevelDB.
 */
public class FileDtxStoreImpl implements DtxStore {

    private FileMessageStoreImpl fileMessageStore;

    /**
     * Generate unique IDs : temporary message ids and internal xids
     */
    private Andes uniqueIdGenerator;

    FileDtxStoreImpl(FileMessageStoreImpl fileMessageStore) {
        this.fileMessageStore = fileMessageStore;
        uniqueIdGenerator = Andes.getInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long storeDtxRecords(Xid xid, List<AndesMessage> enqueueRecords,
                                List<? extends AndesMessageMetadata> dequeueRecords) throws AndesException {
        DB brokerStore = fileMessageStore.getDB();
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            long internalXid = prepareToStoreXid(xid, batch);
            prepareToStoreEnqueuedRecords(enqueueRecords, internalXid, batch);

            if (!dequeueRecords.isEmpty()) {
                prepareToBackupDequeueRecords(dequeueRecords, internalXid, batch);
                prepareToDeleteMessages(dequeueRecords, brokerStore, batch);
            }
            brokerStore.write(batch);
            return internalXid;
        } catch (AndesException e) {
            throw e;
        } catch (DBException e) {
            throw new AndesException("Error occurred while storing dtx records.", e);
        } finally {
            fileMessageStore.closeWriteBatch(batch);
        }
    }

    /**
     * Store the provided xid in the batch. Operations are not written to the database within the method. It is the
     * responsibility of the caller to write the changes.
     *
     * @param xid   {@link Xid} object to be stored
     * @param batch {@link DB} LevelDB write batch
     * @return return the internal xid relating to the {@link Xid}
     * @throws DBException throws {@link DBException} when there is an exception in LevelDB
     */
    private long prepareToStoreXid(Xid xid, WriteBatch batch) throws DBException {

        String nodeId = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();

        String internalXid = Long.toString(uniqueIdGenerator.generateUniqueId());

        String dtxFormatCodeIdentifier = fileMessageStore.generateKey(DTX_ENTRY, internalXid, nodeId, FORMAT_CODE);
        String dtxGlobalIDIdentifier = fileMessageStore.generateKey(DTX_ENTRY, internalXid, nodeId, GLOBAL_ID);
        String dtxBranchIDIdentifier = fileMessageStore.generateKey(DTX_ENTRY, internalXid, nodeId, BRANCH_ID);
        String dtxXIDIdentifier = fileMessageStore.generateKey(DTX_XID, nodeId, Integer.toString(xid.getFormatId()),
                asString(xid.getGlobalTransactionId()), asString(xid.getBranchQualifier()), XID_IDENTIFIER);

        batch.put(bytes(dtxFormatCodeIdentifier), bytes(Integer.toString(xid.getFormatId())));
        batch.put(bytes(dtxGlobalIDIdentifier), xid.getGlobalTransactionId());
        batch.put(bytes(dtxBranchIDIdentifier), xid.getBranchQualifier());
        batch.put(bytes(dtxXIDIdentifier), bytes(internalXid));

        return Long.parseLong(internalXid);
    }

    /**
     * Store enqueued messages in the batch. Operations are not written to the database within the method. It is the
     * responsibility of the caller to write the changes.
     *
     * @param enqueueRecords {@link List} of {@link AndesMessage} to store
     * @param internalXid    internal Xid of the transaction
     * @param batch          {@link DB} LevelDB write batch
     * @throws DBException throws {@link DBException} when there is an exception in LevelDB
     */
    private void prepareToStoreEnqueuedRecords(List<AndesMessage> enqueueRecords, long internalXid,
                                               WriteBatch batch) throws DBException {

        for (AndesMessage message : enqueueRecords) {
            long temporaryMessageId = uniqueIdGenerator.generateUniqueId();

            String dtxEnqueueMetadataIdentifier = fileMessageStore.generateKey(DTX_ENQUEUE,
                    Long.toString(internalXid), MESSAGE_METADATA, Long.toString(temporaryMessageId));

            batch.put(bytes(dtxEnqueueMetadataIdentifier), message.getMetadata().getBytes());

            for (AndesMessagePart messagePart : message.getContentChunkList()) {
                String dtxEnqueueContentIdentifier = fileMessageStore.generateKey(DTX_ENQUEUE,
                        Long.toString(internalXid), MESSAGE_CONTENT, Long.toString(temporaryMessageId),
                        Integer.toString(messagePart.getOffset()));

                batch.put(bytes(dtxEnqueueContentIdentifier), messagePart.getData());
            }
        }
    }

    /**
     * Prepare dequeued records in a batch. Operations are not written to the database within the method. It is the
     * responsibility of the caller to write the changes.
     *
     * @param dequeueRecords {@link List} of acknowledged messages
     * @param internalXid    internal {@link Xid}
     * @param batch          {@link WriteBatch} LevelDB write batch
     * @throws AndesException throws {@link AndesException} on database error
     */
    private void prepareToBackupDequeueRecords(List<? extends AndesMessageMetadata> dequeueRecords, long internalXid,
                                               WriteBatch batch) throws AndesException {

        for (AndesMessageMetadata messageMetadata : dequeueRecords) {

            String dtxDequeueDestinationIdentifier = fileMessageStore.generateKey(DTX_DEQUEUE,
                    Long.toString(internalXid), DESTINATION_NAME, Long.toString(messageMetadata.getMessageID()));
            String dtxDequeueMetadataIdentifier = fileMessageStore.generateKey(DTX_DEQUEUE,
                    Long.toString(internalXid), MESSAGE_METADATA, Long.toString(messageMetadata.getMessageID()));

            batch.put(bytes(dtxDequeueDestinationIdentifier), bytes(messageMetadata.getStorageDestination()));
            batch.put(bytes(dtxDequeueMetadataIdentifier), messageMetadata.getBytes());

            DB brokerStore = fileMessageStore.getDB();
            DBIterator keyIterator = brokerStore.iterator();
            String head = fileMessageStore.generateKey(MESSAGE, Long.toString(messageMetadata.getMessageID()),
                    START_OFFSET, MESSAGE_CONTENT);
            keyIterator.seek(bytes(head));

            try {
                while (keyIterator.hasNext()) {
                    //key arrangement : MESSAGE.$message_id.$offset.MESSAGE_CONTENT
                    String key = asString(keyIterator.peekNext().getKey());
                    String[] keySplit = key.split(DELIMITER);

                    String messageID = keySplit[1];
                    String offset = keySplit[2];

                    String dtxDequeueContentIdentifier = fileMessageStore.generateKey(DTX_DEQUEUE,
                            Long.toString(internalXid), MESSAGE_CONTENT, messageID, offset);

                    batch.put(bytes(dtxDequeueContentIdentifier), keyIterator.peekNext().getValue());

                    if (!keySplit[3].equals(MESSAGE_CONTENT)) {
                        break;
                    }

                    keyIterator.next();
                }
            } catch (DBException e) {
                throw new AndesException("Error occurred while retrieving content for dequeue records.", e);
            } finally {
                fileMessageStore.closeKeyIterator(keyIterator);
            }
        }
    }

    /**
     * Delete the messages from message store using the provided batch. Operations are not written to the database
     * within the method. It is the responsibility of the caller to write the changes.
     *
     * @param dequeueRecords {@link List} of dequeue records
     * @param brokerStore    {@link DB} LevelDB store
     * @param batch          {@link WriteBatch} LevelDB write batch
     * @throws AndesException throws {@link AndesException} on database error
     */
    private void prepareToDeleteMessages(List<? extends AndesMessageMetadata> dequeueRecords,
                                         DB brokerStore, WriteBatch batch) {
        for (AndesMessageMetadata metadata : dequeueRecords) {
            long messageID = metadata.getMessageID();
            String storageQueueName = metadata.getStorageDestination();

            fileMessageStore.deleteMessage(messageID, batch);
            fileMessageStore.deleteMessageFromQueue(messageID, storageQueueName, batch);

            // Delete message from DLC if it is in DLC
            String messageDLCStatus = fileMessageStore.generateKey(MESSAGE, Long.toString(messageID),
                    DLC_STATUS);

            if (Long.parseLong(asString(brokerStore.get(bytes(messageDLCStatus)))) == 1) {
                fileMessageStore.deleteMessageFromQueue(messageID, DLC, batch);
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOnCommit(long internalXid, List<AndesMessage> enqueueRecords) throws AndesException {
        DB brokerStore = fileMessageStore.getDB();
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            if (!enqueueRecords.isEmpty()) {
                prepareToStoreMessages(enqueueRecords, batch);
            }
            removePreparedRecords(internalXid, batch);
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while executing dtx commit event", e);
        } finally {
            fileMessageStore.closeWriteBatch(batch);
        }
    }

    /**
     * Store the messages in the message store using the provided batch. Operations are not written to the database
     * within the method. It is the responsibility of the caller to write the changes.
     *
     * @param enqueueRecords {@link List} of enqueue records
     * @param batch          {@link WriteBatch} LevelDB write batch
     * @throws AndesException throws {@link AndesException} on database error
     */
    private void prepareToStoreMessages(List<AndesMessage> enqueueRecords, WriteBatch batch) throws AndesException {
        for (AndesMessage message : enqueueRecords) {
            AndesMessageMetadata metadata = message.getMetadata();

            fileMessageStore.addMetadata(metadata, metadata.getStorageDestination(), batch);

            if (metadata.getExpirationTime() > 0) {
                fileMessageStore.addExpiryData(metadata, batch);
            }

            fileMessageStore.addContent(message.getContentChunkList(), batch);
        }
    }

    /**
     * Remove the prepared dtx records using the provided batch. Operations are not written to the database within the
     * method. It is the responsibility of the caller to write the changes.
     *
     * @param internalXid internal {@link Xid}
     * @param batch       {@link WriteBatch} LevelDB write batch
     * @throws DBException throws {@link DBException} when there is an exception in LevelDB
     */
    private void removePreparedRecords(long internalXid, WriteBatch batch) throws DBException {

        deleteDtxEntry(internalXid, batch);
        deleteDtxEnqueueRecord(internalXid, batch);
        deleteDtxDequeueRecord(internalXid, batch);
    }

    /**
     * Remove the prepared dtx entry using the provided batch.
     *
     * @param internalXid internal {@link Xid}
     * @param batch       {@link WriteBatch} LevelDB write batch
     * @throws DBException throws {@link DBException} when there is an exception in LevelDB
     */
    private void deleteDtxEntry(long internalXid, WriteBatch batch) throws DBException {
        String nodeId = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();

        DBIterator keyIterator = fileMessageStore.getDB().iterator();
        String head = fileMessageStore.generateKey(DTX_ENTRY, Long.toString(internalXid), nodeId);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                String currentXID = keySplit[1];

                if (!currentXID.equals(Long.toString(internalXid))) {
                    break;
                }

                batch.delete(bytes(key));
                keyIterator.next();
            }

            head = fileMessageStore.generateKey(DTX_XID);
            keyIterator.seek(bytes(head));

            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                if (asString(keyIterator.peekNext().getValue()).equals(Long.toString(internalXid))) {
                    batch.delete(bytes(key));
                    break;
                } else if (!keySplit[0].equals(DTX_XID)) {
                    break;
                }
                keyIterator.next();
            }
        } finally {
            fileMessageStore.closeKeyIterator(keyIterator);
        }
    }

    /**
     * Remove the prepared enqueue records using the provided batch.
     *
     * @param internalXid internal {@link Xid}
     * @param batch       {@link WriteBatch} LevelDB write batch
     * @throws DBException throws {@link DBException} when there is an exception in LevelDB
     */
    private void deleteDtxEnqueueRecord(long internalXid, WriteBatch batch) throws DBException {
        DBIterator keyIterator = fileMessageStore.getDB().iterator();
        String head = fileMessageStore.generateKey(DTX_ENQUEUE, Long.toString(internalXid));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                String xid = keySplit[1];

                if (!xid.equals(Long.toString(internalXid))) {
                    break;
                }

                batch.delete(bytes(key));
                keyIterator.next();
            }
        } finally {
            fileMessageStore.closeKeyIterator(keyIterator);
        }
    }

    /**
     * Remove the prepared dequeue records using the provided batch.
     *
     * @param internalXid internal {@link Xid}
     * @param batch       {@link WriteBatch} LevelDB write batch
     * @throws DBException throws {@link DBException} when there is an exception in LevelDB
     */
    private void deleteDtxDequeueRecord(long internalXid, WriteBatch batch) throws DBException {
        DBIterator keyIterator = fileMessageStore.getDB().iterator();
        String head = fileMessageStore.generateKey(DTX_DEQUEUE, Long.toString(internalXid));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                String xid = keySplit[1];

                if (!xid.equals(Long.toString(internalXid))) {
                    break;
                }

                batch.delete(bytes(key));
                keyIterator.next();
            }
        } finally {
            fileMessageStore.closeKeyIterator(keyIterator);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOnOnePhaseCommit(List<AndesMessage> enqueueRecords, List<AndesPreparedMessageMetadata>
            dequeueRecordsMetadata) throws AndesException {
        DB brokerStore = fileMessageStore.getDB();
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            if (!enqueueRecords.isEmpty()) {
                prepareToStoreMessages(enqueueRecords, batch);
            }

            if (!dequeueRecordsMetadata.isEmpty()) {
                prepareToDeleteMessages(dequeueRecordsMetadata, brokerStore, batch);
            }

            brokerStore.write(batch);

        } catch (DBException e) {
            throw new AndesException("Error occurred while executing dtx commit event", e);
        } finally {
            fileMessageStore.closeWriteBatch(batch);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOnRollback(long internalXid, List<AndesPreparedMessageMetadata> messagesToRestore)
            throws AndesException {
        DB brokerStore = fileMessageStore.getDB();
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            restoreRolledBackAcknowledgedMessages(messagesToRestore, internalXid, batch);
            removePreparedRecords(internalXid, batch);
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while executing dtx commit event", e);
        } finally {
            fileMessageStore.closeWriteBatch(batch);
        }
    }

    /**
     * Restore acknowledged but not committed messages back in the message store.
     *
     * @param messagesToRestore {@link List} of {@link AndesPreparedMessageMetadata}
     * @param internalXid       internal Xid of the transaction
     * @param batch             {@link WriteBatch} LevelDB write batch
     * @throws DBException throws {@link DBException} when there is an exception in LevelDB
     */
    private void restoreRolledBackAcknowledgedMessages(List<AndesPreparedMessageMetadata> messagesToRestore,
                                                       long internalXid, WriteBatch batch) throws DBException,
            AndesException {

        for (AndesPreparedMessageMetadata metadata : messagesToRestore) {

            fileMessageStore.addMetadata(metadata, metadata.getStorageDestination(), batch);

            if (metadata.getExpirationTime() > 0) {
                fileMessageStore.addExpiryData(metadata, batch);
            }

            DB brokerStore = fileMessageStore.getDB();
            DBIterator keyIterator = brokerStore.iterator();
            String head = fileMessageStore.generateKey(DTX_DEQUEUE, Long.toString(internalXid),
                    Long.toString(metadata.getOldMessageId()), START_OFFSET, MESSAGE_CONTENT);
            keyIterator.seek(bytes(head));

            try {
                while (keyIterator.hasNext()) {
                    //key arrangement : DTX_DEQUEUE:$xid:$message_id:$offset:MESSAGE_CONTENT
                    String key = asString(keyIterator.peekNext().getKey());
                    String[] keySplit = key.split(DELIMITER);

                    String offset = keySplit[3];

                    String messageContentIdentifier = fileMessageStore.generateKey(MESSAGE,
                            Long.toString(metadata.getMessageID()), offset, MESSAGE_CONTENT);

                    batch.put(bytes(messageContentIdentifier), keyIterator.peekNext().getValue());

                    if (!keySplit[3].equals(MESSAGE_CONTENT)) {
                        break;
                    }

                    keyIterator.next();
                }
            } catch (DBException e) {
                throw new AndesException("Error occurred while retrieving content for dequeue records.", e);
            } finally {
                fileMessageStore.closeKeyIterator(keyIterator);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long recoverBranchData(DtxBranch branch, String nodeId) throws AndesException {
        DB brokerStore = fileMessageStore.getDB();

        try {
            long internalXid = getInternalXid(branch.getXid(), nodeId, brokerStore);
            if (internalXid == DtxBranch.NULL_XID) {
                return DtxBranch.NULL_XID;
            }
            recoverEnqueueMessages(internalXid, branch, brokerStore);
            recoverDequeueMessages(internalXid, branch, brokerStore);

            return internalXid;
        } catch (AndesException e) {
            throw new AndesException("Error occurred while recovering DtxBranch ", e);
        }
    }

    /**
     * Retrieve the internal Xid from dtx store
     *
     * @param xid         {@link Xid} of the transaction
     * @param nodeId      node id of the server
     * @param brokerStore LevelDB object {@link DB}
     * @return internal Xid
     * @throws AndesException throws {@link AndesException} on database error
     */
    private long getInternalXid(Xid xid, String nodeId, DB brokerStore) throws AndesException {

        try {
            String dtxXIDIdentifier = fileMessageStore.generateKey(DTX_XID, nodeId, Integer.toString(xid.getFormatId()),
                    asString(xid.getGlobalTransactionId()), asString(xid.getBranchQualifier()), XID_IDENTIFIER);

            if (brokerStore.get(bytes(dtxXIDIdentifier)) != null) {
                long internalXID = Long.parseLong(asString(brokerStore.get(bytes(dtxXIDIdentifier))));
                return internalXID;
            } else {
                return DtxBranch.NULL_XID;
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while recovering DtxBranch internal Xid ", e);
        }
    }

    /**
     * Retrieve enqueued incoming messages that were not committed but prepared
     *
     * @param internalXid internal Xid of the transaction
     * @param branch      {@link DtxBranch} of the transaction
     * @param brokerStore LevelDB Object {@link DB}
     * @throws AndesException throws {@link AndesException} on database error
     */
    private void recoverEnqueueMessages(long internalXid, DtxBranch branch, DB brokerStore)
            throws AndesException {

        Map<Long, AndesMessage> messageMap = new HashMap<>();

        DBIterator keyIterator = brokerStore.iterator();
        String head = fileMessageStore.generateKey(DTX_ENQUEUE, Long.toString(internalXid), MESSAGE_METADATA);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DTX_ENQUEUE:$xid:MESSAGE_METADATA:$message_id
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                if (!keySplit[2].equals(MESSAGE_METADATA)) {
                    break;
                }
                long messageID = Long.parseLong(keySplit[3]);

                AndesMessageMetadata metadata = new AndesMessageMetadata(keyIterator.peekNext().getValue());
                AndesMessage andesMessage = new AndesMessage(metadata);

                andesMessage.setChunkList(getDtxContent(internalXid, messageID, brokerStore));

                messageMap.put(messageID, andesMessage);

                keyIterator.next();
            }

            branch.setMessagesToStore(messageMap.values());
        } catch (DBException e) {
            throw new AndesException("Error occurred while recovering enqueued messages for " + "internal xid " +
                    internalXid, e);
        } finally {
            fileMessageStore.closeKeyIterator(keyIterator);
        }
    }

    /**
     * Return content chunk list for given xid and messageId from the dtxStore
     *
     * @param internalXid internal Xid of the transaction
     * @param messageID   id of the message
     * @param brokerStore LevelDB Object {@link DB}
     * @throws AndesException throws {@link AndesException} on database error
     */
    private List<AndesMessagePart> getDtxContent(long internalXid, long messageID, DB brokerStore)
            throws AndesException {
        List<AndesMessagePart> contentChunkList = new ArrayList<>();

        DBIterator keyIterator = brokerStore.iterator();
        String head = fileMessageStore.generateKey(DTX_ENQUEUE, Long.toString(internalXid), MESSAGE_CONTENT,
                Long.toString(messageID), START_OFFSET);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DTX_ENQUEUE:$xid:MESSAGE_CONTENT:$message_id:$offset
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);
                long currentMessageID = Long.parseLong(keySplit[3]);

                if (currentMessageID != messageID) {
                    break;
                }
                int offset = Integer.parseInt(keySplit[5]);

                AndesMessagePart messagePart = new AndesMessagePart();
                messagePart.setData(keyIterator.peekNext().getValue());
                messagePart.setMessageID(messageID);
                messagePart.setOffSet(offset);

                contentChunkList.add(messagePart);

                keyIterator.next();
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving content chunk list for message:" + messageID, e);
        } finally {
            fileMessageStore.closeKeyIterator(keyIterator);
        }
        return contentChunkList;
    }

    /**
     * Retrieve the dequeued messages from the dtx store
     *
     * @param internalXid internal Xid of the transaction
     * @param branch      {@link DtxBranch} of the transaction
     * @param brokerStore LevelDB Object {@link DB}
     * @throws AndesException throws {@link AndesException} related to database error
     */
    private void recoverDequeueMessages(long internalXid, DtxBranch branch, DB brokerStore) throws AndesException {
        List<AndesPreparedMessageMetadata> dtxMetadataList = new ArrayList<>();

        DBIterator keyIterator = brokerStore.iterator();
        String head = fileMessageStore.generateKey(DTX_DEQUEUE, Long.toString(internalXid), MESSAGE_METADATA);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DTX_DEQUEUE:$xid:MESSAGE_METADATA:$message_id
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                if (!keySplit[2].equals(MESSAGE_METADATA)) {
                    break;
                }
                String messageID = keySplit[3];
                String dtxDestinationIdentifier = fileMessageStore.generateKey(DTX_DEQUEUE,
                        Long.toString(internalXid), DESTINATION_NAME, messageID);
                String destinationName = asString(brokerStore.get(bytes(dtxDestinationIdentifier)));

                AndesMessageMetadata metadata = new AndesMessageMetadata(keyIterator.peekNext().getValue());
                metadata.setStorageDestination(destinationName);

                AndesPreparedMessageMetadata dtxMetadata = new AndesPreparedMessageMetadata(metadata);

                dtxMetadataList.add(dtxMetadata);

                keyIterator.next();
            }
            branch.setMessagesToRestore(dtxMetadataList);
        } catch (DBException e) {
            throw new AndesException("Error occurred while recovering DtxBranch ", e);
        } finally {
            fileMessageStore.closeKeyIterator(keyIterator);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<XidImpl> getStoredXidSet(String nodeId) throws AndesException {
        Set<XidImpl> xidSet = new HashSet<>();

        DB brokerStore = fileMessageStore.getDB();
        DBIterator keyIterator = brokerStore.iterator();
        String head = fileMessageStore.generateKey(DTX_XID, nodeId);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DTX_XID:$node_id:$format_code:$gloabal_id:$branch_id:XID_IDENTIFIER
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);
                String currentNodeID = keySplit[1];

                if (!currentNodeID.equals(nodeId)) {
                    break;
                }
                String internalXid = asString(keyIterator.peekNext().getValue());

                String dtxFormatCodeIdentifier = fileMessageStore.generateKey(DTX_ENTRY, internalXid, nodeId,
                        FORMAT_CODE);
                String dtxGlobalIDIdentifier = fileMessageStore.generateKey(DTX_ENTRY, internalXid, nodeId, GLOBAL_ID);
                String dtxBranchIDIdentifier = fileMessageStore.generateKey(DTX_ENTRY, internalXid, nodeId, BRANCH_ID);

                int formatCode = Integer.parseInt(asString(brokerStore.get(bytes(dtxFormatCodeIdentifier))));
                byte[] branchID = brokerStore.get(bytes(dtxBranchIDIdentifier));
                byte[] globalID = brokerStore.get(bytes(dtxGlobalIDIdentifier));

                XidImpl xid = new XidImpl(branchID, formatCode, globalID);
                xidSet.add(xid);

                keyIterator.next();
            }
            return xidSet;
        } catch (DBException e) {
            throw new AndesException("Error occurred while recovering DtxBranch ", e);
        } finally {
            fileMessageStore.closeKeyIterator(keyIterator);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        return fileMessageStore.isOperational(testString, testTime);
    }
}
