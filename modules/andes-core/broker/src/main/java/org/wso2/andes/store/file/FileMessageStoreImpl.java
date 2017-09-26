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

import com.gs.collections.api.iterator.MutableLongIterator;
import com.gs.collections.api.list.primitive.MutableLongList;
import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;

import org.apache.log4j.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.dtx.XidImpl;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DtxStore;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.dtx.AndesPreparedMessageMetadata;
import org.wso2.andes.kernel.dtx.DtxBranch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.transaction.xa.Xid;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

import static org.wso2.andes.store.file.FileStoreConstants.CONNECTOR;

import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE_METADATA;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE_CONTENT;
import static org.wso2.andes.store.file.FileStoreConstants.EXPIRATION_TIME;

import static org.wso2.andes.store.file.FileStoreConstants.DESTINATION;
import static org.wso2.andes.store.file.FileStoreConstants.DESTINATION_ID;
import static org.wso2.andes.store.file.FileStoreConstants.DESTINATION_NAME;
import static org.wso2.andes.store.file.FileStoreConstants.LAST_DESTINATION_ID;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE_COUNT;

import static org.wso2.andes.store.file.FileStoreConstants.DLC;
import static org.wso2.andes.store.file.FileStoreConstants.DLC_QUEUE_ID;
import static org.wso2.andes.store.file.FileStoreConstants.DEFAULT_DLC_QUEUE_ID;


/**
 * Implementation of LevelDB broker store. Message persistence related methods are implemented
 * in this class.
 */
public class FileMessageStoreImpl implements MessageStore {

    private static final Logger log = Logger.getLogger(FileMessageStoreImpl.class);

    /**
     * File store connection source object
     */
    private FileStoreConnection fileStoreConnection;

    /**
     * All the database operations are preformed on this object. Refers to the LevelDB store.
     */
    private DB brokerStore;

    /**
     * Used to handle the last queue id.
     */
    private int lastQueueID;

    /**
     * {@inheritDoc} Check if data can be inserted, read and finally deleted
     * from the database.
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        return false;
    }

    /**
     * {@inheritDoc} Block restart interval, block size, cache size, max open files, write buffer size and path of the
     * leveldb store can be configured.
     */
    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore, ConfigurationProperties
            connectionProperties) throws AndesException {
        this.fileStoreConnection = new FileStoreConnection();
        this.fileStoreConnection.initialize(connectionProperties);
        this.brokerStore = (DB) fileStoreConnection.getConnection();
        return fileStoreConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessages(List<AndesMessage> messageList) throws AndesException {
        Transaction tx = new Transaction(brokerStore);

        try {
            for (AndesMessage message : messageList) {
                storeMessage(message, tx);
            }
            tx.commit(brokerStore);
        } catch (DBException e) {
            log.error("Messages storing failed", e);
        } finally {
            tx.close();
        }
    }

    /**
     * Add metadata, expire data and message content to the transaction
     *
     * @param message     message which needed to be stored
     * @param transaction transaction which accumulates operations to commit
     * @throws AndesException
     */
    private void storeMessage(AndesMessage message, Transaction transaction) throws AndesException {
        AndesMessageMetadata metadata = message.getMetadata();

        addMetadata(metadata, metadata.getStorageQueueName(), transaction);

        if (metadata.isExpirationDefined()) {
            addExpiryData(metadata, transaction);
        }

        for (AndesMessagePart messagePart : message.getContentChunkList()) {
            addContent(messagePart, transaction);
        }
    }

    /**
     * Add metadata details to the transaction
     *
     * @param metadata    message metadata which needed to be stored
     * @param queueName   queue/topic name where the message is needed to be stored
     * @param transaction transaction which accumulates operations to commit
     *                    <p>
     *                    Following keys will be created
     *                    <p>
     *                    MESSAGE.$message_id.DESTINATION_ID
     *                    MESSAGE.$message_id.DLC_QUEUE_ID
     *                    MESSAGE.$message_id.MESSAGE_METADATA
     */
    private void addMetadata(AndesMessageMetadata metadata, final String queueName, Transaction transaction)
            throws AndesException {
        String queueIDIdentifier = generateKey(DESTINATION_ID, MESSAGE, Long.toString(metadata.getMessageID()));
        String dlcQueueIDIdentifier = generateKey(DLC_QUEUE_ID, MESSAGE, Long.toString(metadata.getMessageID()));
        String messageMetaDataIdentifier = generateKey(MESSAGE_METADATA, MESSAGE, Long.toString(metadata.getMessageID()));

        transaction.put(bytes(queueIDIdentifier), bytes(getDestinationID(queueName)));
        transaction.put(bytes(dlcQueueIDIdentifier), bytes(DEFAULT_DLC_QUEUE_ID));
        transaction.put(bytes(messageMetaDataIdentifier), metadata.getMetadata());

        createQueueMetaDataMapping(metadata, queueName, transaction);
    }

    /**
     * Add message expire details to the transaction
     *
     * @param metadata    message which is needed to be stored
     * @param transaction transaction which accumulates operations to commit
     *                    <p>
     *                    Following keys will be created
     *                    <p>
     *                    MESSAGE.$message_id.EXPIRATION_TIME
     *                    DESTINATION.$destination_name.$message_id.EXPIRATION_TIME
     */
    private void addExpiryData(AndesMessageMetadata metadata, Transaction transaction) {
        String messageExpirationTimeIdentifier = generateKey(EXPIRATION_TIME, MESSAGE,
                Long.toString(metadata.getMessageID()));
        String queueMessageExpirationTimeIdentifier = generateKey(EXPIRATION_TIME, DESTINATION,
                metadata.getStorageQueueName(), Long.toString(metadata.getMessageID()));

        transaction.put(bytes(messageExpirationTimeIdentifier),
                bytes(Long.toString(metadata.getExpirationTime())));
        transaction.put(bytes(queueMessageExpirationTimeIdentifier),
                bytes(Long.toString(metadata.getExpirationTime())));
    }

    /**
     * Add message content details to the transaction
     *
     * @param messagePart message content chunk which needed to be stored
     * @param transaction transaction which accumulates operations to commit
     *                    <p>
     *                    Following keys will be created
     *                    <p>
     *                    MESSAGE.$message_id.$offset.MESSAGE_CONTENT
     */
    private void addContent(AndesMessagePart messagePart, Transaction transaction) {
        String messageContentIdentifier = generateKey(MESSAGE_CONTENT, MESSAGE,
                Long.toString(messagePart.getMessageID()), Long.toString(messagePart.getOffset()));

        transaction.put(bytes(messageContentIdentifier), messagePart.getData());
    }

    /**
     * Create mapping between the destination and the message metadata
     *
     * @param metadata    message metadata which is needed to be mapped
     * @param transaction transaction which accumulates operations to commit
     *                    <p>
     *                    Following keys will be created
     *                    <p>
     *                    DESTINATION.$destination_name.$message_id.MESSAGE_METADATA
     *                    DESTINATION.$destination_name.MESSAGE_COUNT
     */
    private void createQueueMetaDataMapping(AndesMessageMetadata metadata, String queueName, Transaction transaction) {
        String queueMessageMetaDataIdentifier = generateKey(MESSAGE_METADATA, DESTINATION, queueName,
                Long.toString(metadata.getMessageID()));
        String queueMessageCountIdentifier = generateKey(MESSAGE_COUNT, DESTINATION, queueName);

        Long queueMessageCount;

        if (asString(transaction.getKey(queueMessageCountIdentifier)) == null) {
            queueMessageCount = Long.parseLong(asString(brokerStore.get(bytes(queueMessageCountIdentifier))));
        } else {
            queueMessageCount = Long.parseLong(asString(transaction.getKey(queueMessageCountIdentifier)));
        }
        queueMessageCount++;
        transaction.setKey(queueMessageCountIdentifier, bytes(Long.toString(queueMessageCount)));

        transaction.put(bytes(queueMessageMetaDataIdentifier), metadata.getMetadata());
        transaction.put(bytes(queueMessageCountIdentifier), bytes(Long.toString(queueMessageCount)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        Transaction tx = new Transaction(brokerStore);

        try {
            for (AndesMessagePart messagePart : partList) {
                addContent(messagePart, tx);
            }
            tx.commit(brokerStore);
        } catch (DBException e) {
            log.error("Message parts storing failed", e);
        } finally {
            tx.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        String messageContentIdentifier = generateKey(MESSAGE_CONTENT, MESSAGE, Long.toString(messageId),
                Integer.toString(offsetValue));

        byte[] messageContent = brokerStore.get(bytes(messageContentIdentifier));
        AndesMessagePart messagePart = new AndesMessagePart();
        messagePart.setData(messageContent);
        messagePart.setMessageID(messageId);
        messagePart.setOffSet(offsetValue);

        return messagePart;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongObjectHashMap<List<AndesMessagePart>> getContent(LongArrayList messageIDList) throws AndesException {
        MutableLongIterator mutableLongIterator = messageIDList.longIterator();
        LongObjectHashMap<List<AndesMessagePart>> messages = new LongObjectHashMap<>();

        while (mutableLongIterator.hasNext()) {
            long messageID = mutableLongIterator.next();

            List<AndesMessagePart> messageContentList = new ArrayList<>();

            DBIterator keyIterator = brokerStore.iterator();
            String head = generateKey(MESSAGE_CONTENT, MESSAGE, Long.toString(messageID), "0");
            keyIterator.seek(bytes(head));

            try {
                while (keyIterator.hasNext()) {
                    //key arrangement : MESSAGE.$message_id.$offset.MESSAGE_CONTENT
                    String key = asString(keyIterator.peekNext().getKey());
                    String[] keySplit = key.split(CONNECTOR);
                    Long currentID = Long.parseLong(keySplit[1]);
                    String identifier = keySplit[keySplit.length - 1];

                    if (currentID != messageID || !identifier.equals(MESSAGE_CONTENT)) {
                        break;
                    }
                    int offset = Integer.parseInt(keySplit[2]);

                    AndesMessagePart messagePart = getContent(messageID, offset);
                    messageContentList.add(messagePart);

                    keyIterator.next();
                }
            } finally {
                try {
                    keyIterator.close();
                } catch (IOException e) {
                    log.error("Key iterator closing failed.", e);
                }
            }
            messages.put(messageID, messageContentList);
        }
        return messages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getMetadata(long messageId) throws AndesException {
        String messageMetaDataIdentifier = generateKey(MESSAGE_METADATA, MESSAGE, Long.toString(messageId));
        byte[] byteMetaData = brokerStore.get(bytes(messageMetaDataIdentifier));
        return new AndesMessageMetadata(messageId, byteMetaData, true);
    }

    /**
     * Read  a metadata list from store specifying a starting message id and a limit
     *
     * @param storageQueueName name of the queue messages are stored
     * @param firstMsgId       first id of the range
     * @param limit            amount of messages which should be returned in the list
     * @return list of metadata
     * @throws AndesException
     */
    @Override
    public List<DeliverableAndesMetadata> getMetadataList(String storageQueueName, long firstMsgId, long limit)
            throws AndesException {
        List<DeliverableAndesMetadata> metadataList = new ArrayList<>();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(MESSAGE_METADATA, DESTINATION, storageQueueName, Long.toString(firstMsgId));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DESTINATION.$destination_name.$message_id.MESSAGE_METADATA
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(FileStoreConstants.CONNECTOR);

                if (keySplit[keySplit.length - 1].equals(EXPIRATION_TIME)) {
                    keyIterator.next();
                    continue;
                }

                if (!keySplit[keySplit.length - 1].equals(MESSAGE_METADATA)) {
                    break;
                }
                Long messageID = Long.parseLong(keySplit[2]);

                byte[] byteMetadata = brokerStore.get(bytes(key));
                DeliverableAndesMetadata metadata = new DeliverableAndesMetadata(messageID, byteMetadata, true);
                metadata.setStorageQueueName(storageQueueName);
                metadataList.add(metadata);

                if (metadataList.size() > limit) {
                    break;
                }
                keyIterator.next();
            }
        } finally {
            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Key iterator closing failed.", e);
            }
        }
        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getMetadataList(String storageQueueName, long firstMsgId, int count)
            throws AndesException {
        List<AndesMessageMetadata> metadataList = new ArrayList<>();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(MESSAGE_METADATA, DESTINATION, storageQueueName, Long.toString(firstMsgId));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DESTINATION.$destination_name.$message_id.MESSAGE_METADATA
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(FileStoreConstants.CONNECTOR);

                if (keySplit[keySplit.length - 1].equals(EXPIRATION_TIME)) {
                    keyIterator.next();
                    continue;
                }

                if (!keySplit[keySplit.length - 1].equals(MESSAGE_METADATA)) {
                    break;
                }
                Long messageID = Long.parseLong(keySplit[2]);

                AndesMessageMetadata metadata = getMetadata(messageID);
                metadata.setStorageQueueName(storageQueueName);
                metadataList.add(metadata);

                if (metadataList.size() > count) {
                    break;
                }
                keyIterator.next();
            }
        } finally {
            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Key iterator closing failed.", e);
            }
        }
        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataForQueueFromDLC(
            String storageQueueName, String dlcQueueName, long firstMsgId, int count) throws AndesException {
        List<AndesMessageMetadata> metadataList = new ArrayList<>(count);

        String head = generateKey(MESSAGE_METADATA, DESTINATION, dlcQueueName, Long.toString(firstMsgId));
        DBIterator keyIterator = brokerStore.iterator();
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                // key arrangement : DESTINATION.$dlc_queue_name.$message_id.MESSAGE_METADATA
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(CONNECTOR);

                if (keySplit[keySplit.length - 1].equals(EXPIRATION_TIME)) {
                    keyIterator.next();
                    continue;
                }

                if (!keySplit[keySplit.length - 1].equals(MESSAGE_METADATA)) {
                    break;
                }

                String messageID = keySplit[2];

                String messageQueueIDIdentifier = generateKey(DESTINATION_ID, MESSAGE, messageID);
                String storageQueueID = asString(brokerStore.get(bytes(messageQueueIDIdentifier)));

                // Only if the message is stored in the given queue, it will be added to the metadataList
                if (storageQueueID.equals(getDestinationID(storageQueueName))) {
                    byte[] byteMetadata = brokerStore.get(bytes(key));
                    DeliverableAndesMetadata metadata = new DeliverableAndesMetadata(Long.parseLong(messageID),
                            byteMetadata, true);
                    metadata.setStorageQueueName(storageQueueName);
                    metadataList.add(metadata);
                }

                if (metadataList.size() > count) {
                    break;
                }
                keyIterator.next();
            }
        } finally {
            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Key iterator closing failed.", e);
            }
        }
        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromDLC(String dlcQueueName, long firstMsgId, int count)
            throws AndesException {
        return getMetadataList(dlcQueueName, firstMsgId, count);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(String storageQueueName) throws AndesException {
        getLastDestinationID();

        String queueIDIdentifier = generateKey(DESTINATION_ID, DESTINATION, storageQueueName);
        String queueNameIdentifier = generateKey(DESTINATION_NAME, DESTINATION, Integer.toString(this.lastQueueID));
        String queueMessageCountIdentifier = generateKey(MESSAGE_COUNT, DESTINATION, storageQueueName);

        Transaction tx = new Transaction(brokerStore);

        try {
            tx.put(bytes(queueIDIdentifier), bytes(Integer.toString(this.lastQueueID)));
            tx.put(bytes(queueNameIdentifier), bytes(storageQueueName));
            tx.put(bytes(queueMessageCountIdentifier), bytes("0"));

            this.lastQueueID++;

            tx.put(bytes(LAST_DESTINATION_ID), bytes(Integer.toString(this.lastQueueID)));

            tx.commit(brokerStore);
        } catch (DBException e) {
            log.error("Queue : " + storageQueueName + " adding failed", e);
        } finally {
            tx.close();
        }
    }


    /**
     * Update lastDestinationID by assigning the value from the db
     */
    private void getLastDestinationID() {
        String lastDestinationID = asString(brokerStore.get(bytes(LAST_DESTINATION_ID)));

        if (lastDestinationID == null) {
            lastDestinationID = "0";
        }
        this.lastQueueID = Integer.parseInt(lastDestinationID);
    }

    /**
     * Return destination id if the destination exists. else create a new destination.
     *
     * @param queueName name of the destination
     * @return queueID (id of the destination)
     */
    private String getDestinationID(String queueName) throws AndesException {
        String queueIDIdentifier = generateKey(DESTINATION_ID, DESTINATION, queueName);
        String queueID = asString(brokerStore.get(bytes(queueIDIdentifier)));

        if (queueID == null) {
            addQueue(queueName);
            queueID = asString(brokerStore.get(bytes(queueIDIdentifier)));
        }
        return queueID;
    }

    /**
     * Return destination name given the destination id
     *
     * @param queueID id of the destination
     * @return destinationName
     */
    private String getDestinationName(long queueID) {
        String queueNameIdentifier = generateKey(DESTINATION_NAME, DESTINATION, Long.toString(queueID));
        return asString(brokerStore.get(bytes(queueNameIdentifier)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToQueue(long messageId, String currentQueueName, String targetQueueName)
            throws AndesException {
        Transaction tx = new Transaction(brokerStore);

        try {
            // Update queue_id in message metadata
            String messageQueueIDIdentifier = generateKey(DESTINATION_ID, MESSAGE, Long.toString(messageId));
            tx.put(bytes(messageQueueIDIdentifier), bytes(getDestinationID(targetQueueName)));

            // Delete message from current queue
            deleteMessageFromQueue(messageId, currentQueueName, tx);

            // Add message to the target queue
            String messageMetaDataIdentifier = generateKey(MESSAGE_METADATA, MESSAGE, Long.toString(messageId));
            String messageExpirationTimeIdentifier = generateKey(EXPIRATION_TIME, MESSAGE, Long.toString(messageId));

            byte[] byteMetaData = brokerStore.get(bytes(messageMetaDataIdentifier));
            AndesMessageMetadata metadata = new AndesMessageMetadata();
            metadata.setMessageID(messageId);
            metadata.setMetadata(byteMetaData);
            metadata.setStorageQueueName(targetQueueName);

            createQueueMetaDataMapping(metadata, targetQueueName, tx);

            if (brokerStore.get(bytes(messageExpirationTimeIdentifier)) != null) {
                metadata.setExpirationTime(Long.parseLong(asString(brokerStore.
                        get(bytes(messageExpirationTimeIdentifier)))));
                addExpiryData(metadata, tx);
            }
            tx.commit(brokerStore);
        } catch (DBException e) {
            log.error("Message :" + messageId + " moving from " + currentQueueName + " to " + targetQueueName +
                    " failed.", e);
        } finally {
            tx.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueueInRange(String storageQueueName, long firstMessageId, long lastMessageId)
            throws AndesException {
        String head = generateKey(MESSAGE_METADATA, DESTINATION, storageQueueName, Long.toString(firstMessageId));
        DBIterator keyIterator = brokerStore.iterator();
        keyIterator.seek(bytes(head));

        long count = 0;

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DESTINATION.$destination_name.$message_id.MESSAGE_METADATA
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(CONNECTOR);

                if (keySplit[keySplit.length - 1].equals(EXPIRATION_TIME)) {
                    keyIterator.next();
                    continue;
                }
                count++;
                long messageId = Long.parseLong(keySplit[2]);

                if (messageId >= lastMessageId) {
                    break;
                }
                keyIterator.next();
            }
        } finally {
            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Key iterator closing failed.", e);
            }
        }
        return count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongArrayList getMessageIDsAddressedToQueue(String storageQueueName, Long startMessageID)
            throws AndesException {
        LongArrayList messageIDs = new LongArrayList();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(MESSAGE_METADATA, DESTINATION, storageQueueName, Long.toString(startMessageID));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(CONNECTOR);

                if (keySplit[keySplit.length - 1].equals(EXPIRATION_TIME)) {
                    keyIterator.next();
                    continue;
                }

                if (!keySplit[keySplit.length - 1].equals(MESSAGE_METADATA)) {
                    break;
                }
                Long messageID = Long.parseLong(keySplit[2]);
                messageIDs.add(messageID);

                keyIterator.next();
            }
        } finally {
            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Key iterator closing failed.", e);
            }
        }
        return messageIDs;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Integer> getMessageCountForAllQueues(List<String> queueNames) throws AndesException {
        Map<String, Integer> queueMessageCount = new HashMap<>();

        for (String queueName : queueNames) {
            String queueMessageCountIdentifier = generateKey(MESSAGE_COUNT, DESTINATION, queueName);
            int messageCount = Integer.parseInt(asString(brokerStore.get(bytes(queueMessageCountIdentifier))));
            queueMessageCount.put(queueName, messageCount);
        }
        return queueMessageCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String storageQueueName) throws AndesException {
        String queueMessageCountIdentifier = generateKey(MESSAGE_COUNT, DESTINATION, storageQueueName);
        return Long.parseLong(asString(brokerStore.get(bytes(queueMessageCountIdentifier))));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deleteAllMessageMetadata(String storageQueueName) throws AndesException {
        int count = 0;

        Transaction tx = new Transaction(brokerStore);

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(storageQueueName, DESTINATION);
        keyIterator.seek(bytes(head));

        try {

            try {
                while (keyIterator.hasNext()) {
                    String key = asString(keyIterator.peekNext().getKey());
                    String[] keySplit = key.split(CONNECTOR);

                    if (keySplit[keySplit.length - 1].equals(EXPIRATION_TIME)) {
                        keyIterator.next();
                        continue;
                    }

                    if (!keySplit[keySplit.length - 1].equals(MESSAGE_METADATA)) {
                        break;
                    }
                    long messageID = Long.parseLong(keySplit[2]);

                    deleteMessage(messageID, tx);
                    deleteMessageFromQueue(messageID, storageQueueName, tx);

                    count++;

                    keyIterator.next();
                }
            } finally {
                try {
                    keyIterator.close();
                } catch (IOException e) {
                    log.error("Key iterator closing failed.", e);
                }
            }

            tx.commit(brokerStore);
        } catch (DBException e) {
            log.error("Messages deletion from " + storageQueueName + " failed", e);
        } finally {
            tx.close();
        }
        return count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeQueue(String storageQueueName) throws AndesException {
        Transaction tx = new Transaction(brokerStore);

        try {
            deleteAllMessagesFromQueue(storageQueueName, tx);
            deleteQueueData(storageQueueName, tx);

            tx.commit(brokerStore);
        } catch (DBException e) {
            log.error("Removing Queue : " + storageQueueName + " failed", e);
        } finally {
            tx.close();
        }
    }

    /**
     * Delete all messages stored in the queue
     *
     * @param storageQueueName name of the destination
     * @param transaction      transaction which accumulates operations to commit
     * @return queueID (id of the destination)
     */
    private void deleteAllMessagesFromQueue(String storageQueueName, Transaction transaction) {
        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(storageQueueName, DESTINATION);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(CONNECTOR);

                if (!(keySplit[keySplit.length - 1].equals(MESSAGE_METADATA) ||
                        keySplit[keySplit.length - 1].equals(EXPIRATION_TIME))) {
                    break;
                }
                long messageID = Long.parseLong(keySplit[2]);

                deleteMessage(messageID, transaction);
                deleteMessageFromQueue(messageID, storageQueueName, transaction);

                keyIterator.next();
            }
        } finally {
            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Error occurred while closing ", e);
            }
        }
    }

    /**
     * Delete queue from the db
     *
     * @param storageQueueName name of the destination
     * @param transaction      transaction which accumulates operations to commit
     * @return queueID (id of the destination)
     */
    private void deleteQueueData(String storageQueueName, Transaction transaction) throws AndesException {
        String queueNameIdentifier = generateKey(DESTINATION_NAME, DESTINATION, getDestinationID(storageQueueName));
        String queueIDIdentifier = generateKey(DESTINATION_ID, DESTINATION, storageQueueName);
        String queueMessageCountIdentifier = generateKey(MESSAGE_COUNT, DESTINATION, storageQueueName);

        transaction.delete(bytes(queueNameIdentifier));
        transaction.delete(bytes(queueIDIdentifier));
        transaction.delete(bytes(queueMessageCountIdentifier));

        this.lastQueueID--;
        transaction.put(bytes(LAST_DESTINATION_ID), bytes(Long.toString(this.lastQueueID)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException {
        Transaction tx = new Transaction(brokerStore);

        try {
            // Create dlc if not already exists else get the dlc id
            String dlcQueueID = getDestinationID(dlcQueueName);

            // Add message metadata to dlc
            String messageMetaDataIdentifier = generateKey(MESSAGE_METADATA, MESSAGE, Long.toString(messageId));
            String messageExpireTimeIdentifier = generateKey(EXPIRATION_TIME, MESSAGE, Long.toString(messageId));

            AndesMessageMetadata metadata = new AndesMessageMetadata();
            metadata.setMessageID(messageId);
            metadata.setMetadata(brokerStore.get(bytes(messageMetaDataIdentifier)));

            if (brokerStore.get(bytes(messageExpireTimeIdentifier)) != null) {
                metadata.setExpirationTime(Long.parseLong(asString(brokerStore.get(bytes(messageExpireTimeIdentifier)))));
            }
            metadata.setStorageQueueName(DLC);

            createQueueMetaDataMapping(metadata, dlcQueueName, tx);

            if (brokerStore.get(bytes(messageExpireTimeIdentifier)) != null) {
                addExpiryData(metadata, tx);
            }
            // Update message dlc queue id
            String messageDLCQueueIDIdentifier = generateKey(DLC_QUEUE_ID, MESSAGE, Long.toString(messageId));
            tx.put(bytes(messageDLCQueueIDIdentifier), bytes(dlcQueueID));

            // Delete metadata from current queue
            String messageQueueIdIdentifier = generateKey(DESTINATION_ID, MESSAGE, Long.toString(messageId));
            long queueID = Long.parseLong(asString(brokerStore.get(bytes(messageQueueIdIdentifier))));
            deleteMessageFromQueue(messageId, getDestinationName(queueID), tx);

            tx.commit(brokerStore);
        } catch (DBException e) {
            log.error("Message :" + messageId + " moving to " + dlcQueueName + " failed.", e);
        } finally {
            tx.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(List<AndesMessageMetadata> messages, String dlcQueueName) throws AndesException {
        for (AndesMessageMetadata message : messages) {
            moveMetadataToDLC(message.getMessageID(), dlcQueueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getMessageIdsInDLC(String dlcQueueName, long startMessageId, int messageLimit) throws
            AndesException {
        List<Long> messageIDList = new ArrayList<>();
        MutableLongList messageIDs = getMessageIDsAddressedToQueue(dlcQueueName, startMessageId);

        for (int i = 0; i < messageIDs.size(); i++) {
            messageIDList.add(messageIDs.get(i));
        }

        return messageIDList.subList(0, messageLimit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForDLCQueue(String dlcQueueName) throws AndesException {
        return getMessageCountForQueue(dlcQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getMessageIdsInDLCForQueue(String sourceQueueName, String dlcQueueName, long startMessageId,
                                                 int messageLimit) throws AndesException {
        List<Long> messageIds = new ArrayList<>();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(MESSAGE_METADATA, DESTINATION, dlcQueueName, Long.toString(startMessageId));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(CONNECTOR);

                if (keySplit[keySplit.length - 1].equals(EXPIRATION_TIME)) {
                    keyIterator.next();
                    continue;
                }

                if (!keySplit[keySplit.length - 1].equals(MESSAGE_METADATA)) {
                    break;
                }
                String messageID = keySplit[2];
                String messageQueueIDIdentifier = generateKey(DESTINATION_ID, MESSAGE, messageID);
                String queueID = asString(brokerStore.get(bytes(messageQueueIDIdentifier)));

                if (getDestinationID(sourceQueueName).equals(queueID)) {
                    messageIds.add(Long.parseLong(messageID));
                }

                if (messageIds.size() >= messageLimit) {
                    break;
                }
                keyIterator.next();
            }
        } finally {
            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Key iterator closing failed.", e);
            }
        }
        return messageIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueueInDLC(String storageQueueName, String dlcQueueName) throws AndesException {
        return getMessageIdsInDLCForQueue(storageQueueName, dlcQueueName, 0, Integer.MAX_VALUE).size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int clearDLCQueue(String dlcQueueName) throws AndesException {
        return deleteAllMessageMetadata(dlcQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMetadataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {
        Transaction tx = new Transaction(brokerStore);

        try {
            for (AndesMessageMetadata metadata : metadataList) {
                long messageID = metadata.getMessageID();
                byte[] byteMetaData = metadata.getMetadata();

                // Add new metadata
                String messageMetaDataIdentifier = generateKey(MESSAGE_METADATA, MESSAGE, Long.toString(messageID));
                tx.put(bytes(messageMetaDataIdentifier), byteMetaData);

                // Change the destination
                updateMetadataInfoInDestination(metadata, currentQueueName, tx);
            }

            tx.commit(brokerStore);
        } catch (DBException e) {
            log.error("Updating metadata of message list failed", e);
        } finally {
            tx.close();
        }
    }

    /**
     * Update the destination of the message
     *
     * @param metadata               updated message metadata
     * @param currentDestinationName destination where message is currently stored
     * @param transaction            transaction which accumulates operations to commit
     */
    private void updateMetadataInfoInDestination(AndesMessageMetadata metadata, String currentDestinationName,
                                                 Transaction transaction) throws AndesException {
        Long messageId = metadata.getMessageID();
        String targetQueueName = metadata.getStorageQueueName();

        // Update destination_id in message metadata
        String messageQueueIDIdentifier = generateKey(DESTINATION_ID, MESSAGE, Long.toString(messageId));
        transaction.put(bytes(messageQueueIDIdentifier), bytes(getDestinationID(targetQueueName)));

        // Delete message from current destination
        deleteMessageFromQueue(messageId, currentDestinationName, transaction);

        // Add message to the target destination
        createQueueMetaDataMapping(metadata, targetQueueName, transaction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(String storageQueueName, List<AndesMessageMetadata> messagesToRemove)
            throws AndesException {
        Transaction tx = new Transaction(brokerStore);

        for (AndesMessageMetadata metadata : messagesToRemove) {
            long messageID = metadata.getMessageID();

            try {
                deleteMessage(messageID, tx);
                deleteMessageFromQueue(messageID, storageQueueName, tx);

                tx.commit(brokerStore);
            } catch (DBException e) {
                log.error("Message metadata deletion from " + storageQueueName + " failed", e);
            } finally {
                tx.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(Collection<? extends AndesMessageMetadata> messagesToRemove) throws AndesException {

        for (AndesMessageMetadata metadata : messagesToRemove) {
            long messageID = metadata.getMessageID();
            String storageQueueName = metadata.getStorageQueueName();

            Transaction tx = new Transaction(brokerStore);

            try {
                deleteMessage(messageID, tx);
                deleteMessageFromQueue(messageID, storageQueueName, tx);

                // Delete message from DLC if it is in DLC
                String messageDLCQueueIdIdentifier = generateKey(DLC_QUEUE_ID, MESSAGE, Long.toString(messageID));

                if (Long.parseLong(asString(brokerStore.get(bytes(messageDLCQueueIdIdentifier)))) != -1) {
                    deleteMessageFromQueue(messageID, DLC, tx);
                }
                tx.commit(brokerStore);
            } catch (DBException e) {
                log.error("Messages deletion failed", e);
            } finally {
                tx.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(List<Long> messagesToRemove) throws AndesException {

        for (Long messageID : messagesToRemove) {
            Transaction tx = new Transaction(brokerStore);

            try {
                //Delete message metadata and content
                deleteMessage(messageID, tx);

                // Delete message from destination
                String messageQueueIdIdentifier = generateKey(DESTINATION_ID, MESSAGE, Long.toString(messageID));
                String storageQueueName = getDestinationName(Long.parseLong(asString(brokerStore.get(
                        bytes(messageQueueIdIdentifier)))));
                deleteMessageFromQueue(messageID, storageQueueName, tx);

                // Delete message from DLC if it is in DLC
                String messageDLCQueueIdIdentifier = generateKey(DLC_QUEUE_ID, MESSAGE, Long.toString(messageID));

                if (Long.parseLong(asString(brokerStore.get(bytes(messageDLCQueueIdIdentifier)))) != -1) {
                    deleteMessageFromQueue(messageID, FileStoreConstants.DLC, tx);
                }
                tx.commit(brokerStore);
            } catch (Exception e) {
                log.error("Messages deletion failed", e);
            } finally {
                tx.close();
            }
        }
    }

    /**
     * Add message deletion from destination related operations to the transaction
     *
     * @param messageID   id of the message which needed to be deleted
     * @param transaction transaction which accumulates operations to commit
     *                    <p>
     *                    Following keys will be removed
     *                    <p>
     *                    MESSAGE.$message_id.$offset.MESSAGE_CONTENT
     *                    MESSAGE.$message_id.DESTINATION_ID
     *                    MESSAGE.$message_id.DLC_QUEUE_ID
     *                    MESSAGE.$message_id.MESSAGE_METADATA
     */
    private void deleteMessage(long messageID, Transaction transaction) {
        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(Long.toString(messageID), MESSAGE);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                Long currentID = Long.parseLong(key.split(CONNECTOR)[1]);

                if (currentID != messageID) {
                    break;
                }
                transaction.delete(bytes(key));

                keyIterator.next();
            }
        } finally {
            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Key iterator closing failed.", e);
            }
        }
    }

    /**
     * Add message deletion from destination related operations to the transaction
     *
     * @param messageID   id of the message which needed to be deleted
     * @param transaction transaction which accumulates operations to commit
     *                    <p>
     *                    Following keys will be deleted
     *                    <p>
     *                    DESTINATION.$destination_name.$message_id.MESSAGE_METADATA
     *                    DESTINATION.$destination_name.$message_id.EXPIRATION_TIME
     */
    private void deleteMessageFromQueue(long messageID, String queueName, Transaction transaction) {
        String queueMessageMetaDataIdentifier = generateKey(MESSAGE_METADATA, DESTINATION, queueName,
                Long.toString(messageID));
        String queueMessageCountIdentifier = generateKey(MESSAGE_COUNT, DESTINATION, queueName);
        String queueMessageExpireTimeIdentifier = generateKey(EXPIRATION_TIME, DESTINATION, queueName,
                Long.toString(messageID));

        Long queueMessageCount;

        if (asString(transaction.getKey(queueMessageCountIdentifier)) == null) {
            queueMessageCount = Long.parseLong(asString(brokerStore.get(bytes(queueMessageCountIdentifier))));
        } else {
            queueMessageCount = Long.parseLong(asString(transaction.getKey(queueMessageCountIdentifier)));
        }
        queueMessageCount--;

        transaction.setKey(queueMessageCountIdentifier, bytes(Long.toString(queueMessageCount)));
        transaction.delete(bytes(queueMessageMetaDataIdentifier));

        if (brokerStore.get(bytes(queueMessageExpireTimeIdentifier)) != null) {
            transaction.delete(bytes(queueMessageExpireTimeIdentifier));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDLCMessages(List<AndesMessageMetadata> messagesToRemove) throws AndesException {

        for (AndesMessageMetadata metadata : messagesToRemove) {
            long messageID = metadata.getMessageID();

            Transaction tx = new Transaction(brokerStore);

            try {
                deleteMessage(messageID, tx);
                deleteMessageFromQueue(messageID, DLC, tx);

                tx.commit(brokerStore);
            } catch (DBException e) {
                log.error("Messages deletion from DLC failed", e);
            } finally {
                tx.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getExpiredMessages(long lowerBoundMessageID, String queueName) throws AndesException {
        List<Long> expiredMessageIDList = new ArrayList<>();
        Long currentTime = System.currentTimeMillis();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(EXPIRATION_TIME, DESTINATION, queueName, Long.toString(lowerBoundMessageID));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(CONNECTOR);

                if (keySplit[keySplit.length - 1].equals(MESSAGE_METADATA)) {
                    keyIterator.next();
                    continue;
                }

                if (!keySplit[keySplit.length - 1].equals(EXPIRATION_TIME)) {
                    break;
                }
                Long messageID = Long.parseLong(keySplit[2]);
                Long messageExpiredTime = Long.parseLong(asString(keyIterator.peekNext().getValue()));

                if (messageExpiredTime < currentTime) {
                    expiredMessageIDList.add(messageID);
                }
                keyIterator.next();
            }
        } finally {
            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Key iterator closing failed.", e);
            }
        }
        return expiredMessageIDList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getExpiredMessagesFromDLC(long messageCount) throws AndesException {
        return getExpiredMessages(0, DLC).subList(0, (int) messageCount);
    }

    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String
            destination) throws AndesException {
        // Not implemented in RDBMS store
    }

    @Override
    public long getApproximateQueueMessageCount(String storageQueueName) throws AndesException {
        return 0;
    }

    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        // Not implemented in RDBMS store
    }

    @Override
    public void removeLocalQueueData(String storageQueueName) {
        // No cache is used
    }

    @Override
    public void incrementMessageCountForQueue(String storageQueueName, long incrementBy) throws AndesException {
        // Not implemented in RDBMS store
    }

    @Override
    public void decrementMessageCountForQueue(String storageQueueName, long decrementBy) throws AndesException {
        // Not implemented in RDBMS store
    }

    @Override
    public void storeRetainedMessages(Map<String, AndesMessage> retainMap) throws AndesException {

    }

    @Override
    public List<String> getAllRetainedTopics() throws AndesException {
        return null;
    }

    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {
        return null;
    }

    @Override
    public DeliverableAndesMetadata getRetainedMetadata(String destination) throws AndesException {
        return null;
    }

    @Override
    public void close() {
        fileStoreConnection.close();
    }

    @Override
    public DtxStore getDtxStore() {
        return null;
    }

    /**
     * Create key name when prefix, suffix and identifiers are provided
     *
     * @param suffix      suffix for the key name
     * @param prefix      prefix for the key name
     * @param identifiers identifiers which are included in the key name
     */
    private static String generateKey(String suffix, String prefix, String... identifiers) {

        for (String identifier : identifiers) {
            prefix = prefix + CONNECTOR + identifier;
        }
        return prefix + CONNECTOR + suffix;
    }
}
