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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DtxStore;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.tools.utils.MessageTracer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

import static org.wso2.andes.store.file.FileStoreConstants.DELIMITER;

import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE_METADATA;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE_CONTENT;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE_EXPIRATION_TIME;
import static org.wso2.andes.store.file.FileStoreConstants.INITIAL_MESSAGE_COUNT;
import static org.wso2.andes.store.file.FileStoreConstants.START_OFFSET;

import static org.wso2.andes.store.file.FileStoreConstants.DESTINATION;
import static org.wso2.andes.store.file.FileStoreConstants.DESTINATION_NAME;
import static org.wso2.andes.store.file.FileStoreConstants.MESSAGE_COUNT;

import static org.wso2.andes.store.file.FileStoreConstants.DLC;
import static org.wso2.andes.store.file.FileStoreConstants.DLC_STATUS;
import static org.wso2.andes.store.file.FileStoreConstants.DEFAULT_DLC_STATUS;
import static org.wso2.andes.store.file.FileStoreConstants.IN_DLC_STATUS;


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
     * Contains utils methods related to connection tests
     */
    private FileStoreUtils fileStoreUtils;

    /**
     * All the database operations are preformed on this object. Refers to the LevelDB store.
     */
    private DB brokerStore;

    /**
     * Stores message counts of each destination as AtomicLong values.
     */
    private Map<String, AtomicLong> destinationMessageCount = new HashMap<>();

    /**
     * Store implementation to handle distributed transactions' store operations
     */
    private DtxStore dtxStore;

    /**
     * Initialize file store connection
     *
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore, ConfigurationProperties
            connectionProperties) throws AndesException {
        this.fileStoreUtils = new FileStoreUtils();
        this.fileStoreConnection = new FileStoreConnection();
        this.fileStoreConnection.initialize(connectionProperties);
        this.brokerStore = (DB) fileStoreConnection.getConnection();
        getInitialMessageCountsForAllDestinations();
        dtxStore = new FileDtxStoreImpl(this);
        return fileStoreConnection;
    }

    /**
     * Load message counts for all destinations which are stored in the database and store them in
     * destinationMessageCount map
     */
    private void getInitialMessageCountsForAllDestinations() throws AndesException {
        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(DESTINATION, MESSAGE_COUNT);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DESTINATION.MESSAGE_COUNT.$destination_name
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);
                String identifier = keySplit[1];

                if (!identifier.equals(MESSAGE_COUNT)) {
                    break;
                }
                String destinationName = keySplit[2];
                long messageCount = Long.parseLong(asString(keyIterator.peekNext().getValue()));
                this.destinationMessageCount.put(destinationName, new AtomicLong(messageCount));

                keyIterator.next();
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while loading message counts for destinations.", e);
        } finally {
            closeKeyIterator(keyIterator);
        }
    }

    /**
     * Check if data can be inserted, read and finally deleted from the database
     *
     * {@inheritDoc}
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        try {
            return fileStoreUtils.testInsert(this.fileStoreConnection, testString, testTime) &&
                    fileStoreUtils.testRead(this.fileStoreConnection, testString, testTime) &&
                    fileStoreUtils.testDelete(this.fileStoreConnection, testString);
        } catch (DBException e) {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessages(List<AndesMessage> messageList) throws AndesException {
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            for (AndesMessage message : messageList) {
                storeMessage(message, batch);
            }
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while storing message list.", e);
        } finally {
            closeWriteBatch(batch);
        }
    }

    /**
     * Add metadata, expire data and message content to the write batch.
     *
     * @param message message which needed to be stored
     * @param batch   accumulates operations to apply atomically
     * @throws AndesException throws {@link AndesException} on database error
     */
    private void storeMessage(AndesMessage message, WriteBatch batch) throws AndesException {
        AndesMessageMetadata metadata = message.getMetadata();

        addMetadata(metadata, metadata.getStorageDestination(), batch);

        if (message.getMetadata().getExpirationTime() > 0) {
            addExpiryData(metadata, batch);
        }

        addContent(message.getContentChunkList(), batch);
    }

    /**
     * Add metadata to the write batch. Operations are not written to the database within the method. It is the
     * responsibility of the caller to write the changes.
     *
     * Following keys will be created.
     *
     * MESSAGE.$message_id.DESTINATION_ID
     * MESSAGE.$message_id.DLC_QUEUE_ID
     * MESSAGE.$message_id.MESSAGE_METADATA
     *
     * @param metadata  message metadata which needed to be stored
     * @param queueName queue/topic name where the message is needed to be stored
     * @param batch     accumulates operations to apply atomically
     * @throws AndesException throws {@link AndesException} on database error
     */
    public void addMetadata(AndesMessageMetadata metadata, final String queueName, WriteBatch batch)
            throws AndesException {
        String messageID = Long.toString(metadata.getMessageID());

        String dlcStatusIdentifier = generateKey(MESSAGE, messageID, DLC_STATUS);
        String messageDestinationIdentifier = generateKey(MESSAGE, messageID, DESTINATION_NAME);

        batch.put(bytes(dlcStatusIdentifier), bytes(DEFAULT_DLC_STATUS));
        batch.put(bytes(messageDestinationIdentifier), bytes(queueName));

        createQueueMessageMapping(metadata, queueName, batch);
    }

    /**
     * Add message expire data to the batch. Operations are not written to the database within the method. It is the
     * responsibility of the caller to write the changes.
     *
     * Following keys will be created.
     *
     * DESTINATION.$destination_name.MESSAGE_EXPIRATION_TIME.$message_id
     *
     * @param metadata message metadata which contains required message expiration data
     * @param batch    accumulates operations to apply atomically
     */
    public void addExpiryData(AndesMessageMetadata metadata, WriteBatch batch) {
        String messageID = Long.toString(metadata.getMessageID());
        String storageQueueName = metadata.getStorageDestination();
        String expirationTime = Long.toString(metadata.getExpirationTime());

        String queueMessageExpirationTimeIdentifier = generateKey(DESTINATION, storageQueueName,
                MESSAGE_EXPIRATION_TIME, messageID);

        batch.put(bytes(queueMessageExpirationTimeIdentifier), bytes(expirationTime));
    }

    /**
     * Add message content to the batch. Operations are not written to the database within the method. It is the
     * responsibility of the caller to write the changes.
     *
     * Following keys will be created.
     *
     * MESSAGE.$message_id.$offset.MESSAGE_CONTENT
     *
     * @param contentChunkList list which contains message content chunks which need to be stored
     * @param batch            accumulates operations to apply atomically
     */
    public void addContent(List<AndesMessagePart> contentChunkList, WriteBatch batch) {

        for (AndesMessagePart messagePart : contentChunkList) {
            String messageContentIdentifier = generateKey(MESSAGE, Long.toString(messagePart.getMessageID()),
                    Long.toString(messagePart.getOffset()), MESSAGE_CONTENT);

            batch.put(bytes(messageContentIdentifier), messagePart.getData());
        }
    }

    /**
     * Create mapping between the destination and the message. Operations are not written to the database within the
     * method. It is the responsibility of the caller to write the changes.
     *
     * Following keys will be created.
     *
     * DESTINATION.$destination_name.MESSAGE_METADATA.$message_id
     * DESTINATION.MESSAGE_COUNT.$destination_name
     *
     * @param metadata message metadata which needs to be mapped
     * @param batch    accumulates operations to apply atomically
     * @throws AndesException throws {@link AndesException} on database error
     */
    public void createQueueMessageMapping(AndesMessageMetadata metadata, String queueName, WriteBatch batch)
            throws AndesException {
        addQueue(queueName);
        String queueMessageMetaDataIdentifier = generateKey(DESTINATION, queueName, MESSAGE_METADATA,
                Long.toString(metadata.getMessageID()));

        batch.put(bytes(queueMessageMetaDataIdentifier), metadata.getBytes());
        increaseMessageCount(queueName, batch);
    }

    /**
     * Increment the message count value of destinationMessageCount map for a given destination and
     * apply that value to the batch.
     *
     * @param storageQueueName message metadata which needs to be mapped
     * @param batch            accumulates operations to apply atomically
     */
    private void increaseMessageCount(String storageQueueName, WriteBatch batch) {
        String destinationMessageCountIdentifier = generateKey(DESTINATION, MESSAGE_COUNT, storageQueueName);

        AtomicLong queueMessageCount = this.destinationMessageCount.get(storageQueueName);

        batch.put(bytes(destinationMessageCountIdentifier),
                bytes(Long.toString(queueMessageCount.incrementAndGet())));
    }

    /**
     * Increment the message count value of destinationMessageCount map for a given destination and
     * apply that value to the batch.
     *
     * @param storageQueueName message metadata which needs to be mapped
     * @param batch            accumulates operations to apply atomically
     */
    private void decreaseMessageCount(String storageQueueName, WriteBatch batch) {
        String destinationMessageCountIdentifier = generateKey(DESTINATION, MESSAGE_COUNT, storageQueueName);

        AtomicLong queueMessageCount = this.destinationMessageCount.get(storageQueueName);

        batch.put(bytes(destinationMessageCountIdentifier),
                bytes(Long.toString(queueMessageCount.decrementAndGet())));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            addContent(partList, batch);
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while storing message content list.", e);
        } finally {
            closeWriteBatch(batch);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        try {
            String messageContentIdentifier = generateKey(MESSAGE, Long.toString(messageId),
                    Integer.toString(offsetValue), MESSAGE_CONTENT);

            byte[] messageContent = brokerStore.get(bytes(messageContentIdentifier));

            AndesMessagePart messagePart = new AndesMessagePart();
            messagePart.setData(messageContent);
            messagePart.setMessageID(messageId);
            messagePart.setOffSet(offsetValue);

            return messagePart;
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving content chunk for message: " + messageId, e);
        }
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
            messages.put(messageID, getMessageContentChunkList(messageID));
        }
        return messages;
    }

    /**
     * Get message content chunk list for a given message id.
     *
     * @param messageID id of the message which content chunk list should be retrieved
     * @throws AndesException throws {@link AndesException} on database error
     */
    private List<AndesMessagePart> getMessageContentChunkList(long messageID) throws AndesException {
        List<AndesMessagePart> messageContentList = new ArrayList<>();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(MESSAGE, Long.toString(messageID), START_OFFSET, MESSAGE_CONTENT);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : MESSAGE.$message_id.$offset.MESSAGE_CONTENT
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);
                String identifier = keySplit[keySplit.length - 1];

                if (!identifier.equals(MESSAGE_CONTENT)) {
                    break;
                }
                int offset = Integer.parseInt(keySplit[2]);

                AndesMessagePart messagePart = new AndesMessagePart();
                messagePart.setData(keyIterator.peekNext().getValue());
                messagePart.setMessageID(messageID);
                messagePart.setOffSet(offset);

                messageContentList.add(messagePart);

                keyIterator.next();
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving content chunk list for message:" + messageID, e);
        } finally {
            closeKeyIterator(keyIterator);
        }
        return messageContentList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getMetadata(long messageId) throws AndesException {
        try {
            String messageID = Long.toString(messageId);
            String messageDestinationIdentifier = generateKey(MESSAGE, messageID, DESTINATION_NAME);

            String storageQueueName = asString(brokerStore.get(messageDestinationIdentifier.getBytes()));

            String messageMetaDataIdentifier = generateKey(DESTINATION, storageQueueName, messageID, MESSAGE_METADATA);

            byte[] byteMetaData = brokerStore.get(bytes(messageMetaDataIdentifier));
            return new AndesMessageMetadata(byteMetaData);
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving metadata for message: " + messageId, e);
        }
    }

    /**
     * Read  a metadata list from store specifying a starting message id and a limit.
     *
     * @param storageQueueName name of the queue messages are stored
     * @param firstMsgId       first id of the range
     * @param limit            amount of messages which should be returned in the list
     * @return list of metadata
     * @throws AndesException throws {@link AndesException} on database error
     */
    @Override
    public List<DeliverableAndesMetadata> getMetadataList(String storageQueueName, long firstMsgId, long limit)
            throws AndesException {
        List<DeliverableAndesMetadata> metadataList = new ArrayList<>();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(DESTINATION, storageQueueName, MESSAGE_METADATA, Long.toString(firstMsgId));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DESTINATION.$destination_name.MESSAGE_METADATA.$message_id
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(FileStoreConstants.DELIMITER);

                if (!keySplit[1].equals(storageQueueName)) {
                    break;
                }

                byte[] byteMetadata = keyIterator.peekNext().getValue();
                DeliverableAndesMetadata metadata = new DeliverableAndesMetadata(byteMetadata);
                metadataList.add(metadata);

                //Tracing message
                MessageTracer.trace(metadata, MessageTracer.METADATA_READ_FROM_DB);

                if (metadataList.size() >= limit) {
                    break;
                }
                keyIterator.next();
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving metadata from " + storageQueueName, e);
        } finally {
            closeKeyIterator(keyIterator);
        }
        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getMetadataList(String storageQueueName, long firstMsgId, int count)
            throws AndesException {
        return getMetadataList(storageQueueName, firstMsgId, count);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataForQueueFromDLC(
            String storageQueueName, String dlcQueueName, long firstMsgId, int count) throws AndesException {
        List<AndesMessageMetadata> metadataList = new ArrayList<>(count);

        String head = generateKey(DESTINATION, dlcQueueName, MESSAGE_METADATA, Long.toString(firstMsgId));
        DBIterator keyIterator = brokerStore.iterator();
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                // key arrangement : DESTINATION.$dlc_queue_name.MESSAGE_METADATA.$message_id
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                if (!keySplit[2].equals(MESSAGE_METADATA) || !keySplit[1].equals(dlcQueueName)) {
                    break;
                }
                String messageID = keySplit[3];

                String messageDestinationIdentifier = generateKey(MESSAGE, messageID, DESTINATION_NAME);
                String destination = asString(brokerStore.get(bytes(messageDestinationIdentifier)));

                // Only if the message is stored in the given queue, it will be added to the metadataList
                if (destination.equals(storageQueueName)) {
                    byte[] byteMetadata = keyIterator.peekNext().getValue();
                    AndesMessageMetadata metadata = new AndesMessageMetadata(byteMetadata);
                    metadataList.add(metadata);
                }

                if (metadataList.size() >= count) {
                    break;
                }
                keyIterator.next();
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving metadata from " + dlcQueueName, e);
        } finally {
            closeKeyIterator(keyIterator);
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
        String destinationMessageCountIdentifier = generateKey(DESTINATION, MESSAGE_COUNT, storageQueueName);

        if (brokerStore.get(bytes(destinationMessageCountIdentifier)) == null) {
            try {
                brokerStore.put(bytes(destinationMessageCountIdentifier), bytes(INITIAL_MESSAGE_COUNT));
                getInitialMessageCountsForAllDestinations();
            } catch (DBException e) {
                throw new AndesException("Error occurred while adding destination: " + storageQueueName, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToQueue(long messageId, String currentQueueName, String targetQueueName)
            throws AndesException {
        String messageID = Long.toString(messageId);
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            // Update destination of the message
            String messageDestinationIdentifier = generateKey(MESSAGE, Long.toString(messageId), DESTINATION_NAME);
            batch.put(bytes(messageDestinationIdentifier), bytes(targetQueueName));

            // Add message to the target queue
            String messageMetaDataIdentifier = generateKey(DESTINATION, currentQueueName, MESSAGE_METADATA, messageID);
            String messageExpirationTimeIdentifier = generateKey(DESTINATION, currentQueueName, MESSAGE_EXPIRATION_TIME,
                    messageID);

            byte[] byteMetaData = brokerStore.get(bytes(messageMetaDataIdentifier));
            AndesMessageMetadata metadata = new AndesMessageMetadata(byteMetaData);
            metadata.setMessageID(messageId);
            metadata.setStorageDestination(targetQueueName);

            createQueueMessageMapping(metadata, targetQueueName, batch);

            if (brokerStore.get(bytes(messageExpirationTimeIdentifier)) != null) {
                metadata.setExpirationTime(Long.parseLong(asString(brokerStore.
                        get(bytes(messageExpirationTimeIdentifier)))));
                addExpiryData(metadata, batch);
            }

            // Delete message from current queue
            deleteMessageFromQueue(messageId, currentQueueName, batch);

            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException(" Error occurred while moving message: " + messageId + " from " + currentQueueName
                    + " to " + targetQueueName, e);
        } finally {
            closeWriteBatch(batch);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueueInRange(String storageQueueName, long firstMessageId, long lastMessageId)
            throws AndesException {
        String head = generateKey(DESTINATION, storageQueueName, MESSAGE_METADATA, Long.toString(firstMessageId));
        DBIterator keyIterator = brokerStore.iterator();
        keyIterator.seek(bytes(head));

        long count = 0;

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DESTINATION.$destination_name.MESSAGE_METADATA.$message_id
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);
                count++;
                long messageId = Long.parseLong(keySplit[3]);

                if (messageId >= lastMessageId) {
                    break;
                }
                keyIterator.next();
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while getting message count for " + storageQueueName);
        } finally {
            closeKeyIterator(keyIterator);
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
        String head = generateKey(DESTINATION, storageQueueName, MESSAGE_METADATA, Long.toString(startMessageID));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DESTINATION.$destination_name.MESSAGE_METADATA.$message_id
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                if (!keySplit[1].equals(storageQueueName)) {
                    break;
                }
                Long messageID = Long.parseLong(keySplit[3]);
                messageIDs.add(messageID);

                keyIterator.next();
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving message ids for " + storageQueueName);
        } finally {
            closeKeyIterator(keyIterator);
        }
        return messageIDs;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Integer> getMessageCountForAllQueues(List<String> queueNames) throws AndesException {
        Map<String, Integer> queueMessageCount = new HashMap<>();

        try {
            for (String queueName : queueNames) {
                String destinationMessageCountIdentifier = generateKey(DESTINATION, MESSAGE_COUNT, queueName);
                queueMessageCount.put(queueName,
                        Integer.parseInt(asString(brokerStore.get(bytes(destinationMessageCountIdentifier)))));
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while getting message counts for all queues.", e);
        }

        return queueMessageCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String storageQueueName) throws AndesException {
        try {
            String destinationMessageCountIdentifier = generateKey(DESTINATION, MESSAGE_COUNT, storageQueueName);
            return Long.parseLong(asString(brokerStore.get(bytes(destinationMessageCountIdentifier))));
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving message count for " + storageQueueName, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deleteAllMessageMetadata(String storageQueueName) throws AndesException {
        int count = 0;

        WriteBatch batch = brokerStore.createWriteBatch();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(DESTINATION, storageQueueName, MESSAGE_METADATA);
        keyIterator.seek(bytes(head));

        try {
            try {
                while (keyIterator.hasNext()) {
                    //key arrangement : DESTINATION.$destination_name.MESSAGE_METADATA.$message_id
                    String key = asString(keyIterator.peekNext().getKey());
                    String[] keySplit = key.split(DELIMITER);

                    if (!keySplit[1].equals(storageQueueName)) {
                        break;
                    }
                    long messageID = Long.parseLong(keySplit[3]);

                    deleteMessage(messageID, batch);
                    deleteMessageFromQueue(messageID, storageQueueName, batch);

                    count++;

                    keyIterator.next();
                }
            } finally {
                closeKeyIterator(keyIterator);
            }
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while deleting message metadata from " + storageQueueName, e);
        } finally {
            closeWriteBatch(batch);
        }
        return count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeQueue(String storageQueueName) throws AndesException {
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            deleteAllMessagesFromQueue(storageQueueName, batch);
            deleteDestinationData(storageQueueName, batch);

            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while removing destination: " + storageQueueName, e);
        } finally {
            closeWriteBatch(batch);
        }
    }

    private void deleteDestinationData(String storageQueueName, WriteBatch batch) {
        String destinationMessageCountIdentifier = generateKey(DESTINATION, MESSAGE_COUNT, storageQueueName);
        batch.delete(bytes(destinationMessageCountIdentifier));
    }

    /**
     * Delete all messages stored in the destination. Operations are not written to the database within the method.
     * It is the responsibility of the caller to write the changes.
     *
     * @param storageQueueName name of the destination
     * @param batch            accumulates operations to apply atomically
     */
    private void deleteAllMessagesFromQueue(String storageQueueName, WriteBatch batch) {
        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(DESTINATION, storageQueueName, MESSAGE_EXPIRATION_TIME);
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                if (!keySplit[1].equals(storageQueueName)) {
                    break;
                }
                long messageID = Long.parseLong(keySplit[3]);

                deleteMessage(messageID, batch);
                deleteMessageFromQueue(messageID, storageQueueName, batch);

                keyIterator.next();
            }
        } finally {
            closeKeyIterator(keyIterator);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException {
        WriteBatch batch = brokerStore.createWriteBatch();

        try {

            // Add message metadata to dlc
            String messageDestinationIdentifier = generateKey(MESSAGE, Long.toString(messageId), DESTINATION_NAME);
            String storageQueueName = asString(brokerStore.get(bytes(messageDestinationIdentifier)));

            String messageMetaDataIdentifier = generateKey(DESTINATION, storageQueueName, MESSAGE_METADATA,
                    Long.toString(messageId));
            String messageExpireTimeIdentifier = generateKey(DESTINATION, storageQueueName, MESSAGE_EXPIRATION_TIME,
                    Long.toString(messageId));

            AndesMessageMetadata metadata = new AndesMessageMetadata(brokerStore.get(bytes(messageMetaDataIdentifier)));
            metadata.setMessageID(messageId);

            if (brokerStore.get(bytes(messageExpireTimeIdentifier)) != null) {
                metadata.setExpirationTime(Long.parseLong(asString(
                        brokerStore.get(bytes(messageExpireTimeIdentifier)))));
            }
            metadata.setStorageDestination(DLC);

            createQueueMessageMapping(metadata, dlcQueueName, batch);

            if (brokerStore.get(bytes(messageExpireTimeIdentifier)) != null) {
                addExpiryData(metadata, batch);
            }

            // Update message dlc queue id
            String messageDLCQueueIDIdentifier = generateKey(MESSAGE, Long.toString(messageId), DLC_STATUS);
            batch.put(bytes(messageDLCQueueIDIdentifier), bytes(IN_DLC_STATUS));

            // Delete metadata from current queue
            deleteMessageFromQueue(messageId, storageQueueName, batch);

            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while moving message:" + messageId + "to DLC.", e);
        } finally {
            closeWriteBatch(batch);
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
        String head = generateKey(DESTINATION, dlcQueueName, MESSAGE_METADATA, Long.toString(startMessageId));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DESTINATION.$destination_name.MESSAGE_METADATA.$message_id
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                if (!keySplit[1].equals(dlcQueueName)) {
                    break;
                }
                String messageID = keySplit[3];
                String messageDestinationIdentifier = generateKey(MESSAGE, messageID, DESTINATION_NAME);
                String storageQueueName = asString(brokerStore.get(bytes(messageDestinationIdentifier)));

                if (storageQueueName.equals(sourceQueueName)) {
                    messageIds.add(Long.parseLong(messageID));
                }

                if (messageIds.size() >= messageLimit) {
                    break;
                }
                keyIterator.next();
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving message ids in" + sourceQueueName +
                    "from" + dlcQueueName, e);
        } finally {
            closeKeyIterator(keyIterator);
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
        WriteBatch batch = brokerStore.createWriteBatch();

        try {

            for (AndesMessageMetadata metadata : metadataList) {

                // Update metadata in the destination
                updateMetadataInfoInDestination(metadata, currentQueueName, batch);
            }
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while updating metadata in " + currentQueueName, e);
        } finally {
            closeWriteBatch(batch);
        }
    }

    /**
     * Update the destination of the message. Operations are not written to the database within the method. It is the
     * responsibility of the caller to write the changes.
     *
     * @param metadata               updated message metadata
     * @param currentDestinationName destination where the message is currently stored
     * @param batch                  accumulates operations to apply atomically
     * @throws AndesException throws {@link AndesException} on database error
     */
    private void updateMetadataInfoInDestination(AndesMessageMetadata metadata, String currentDestinationName,
                                                 WriteBatch batch) throws AndesException {
        Long messageId = metadata.getMessageID();
        String targetQueueName = metadata.getStorageDestination();

        // Delete message from current destination
        deleteMessageFromQueue(messageId, currentDestinationName, batch);

        // Add message to the target destination
        createQueueMessageMapping(metadata, targetQueueName, batch);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(String storageQueueName, List<AndesMessageMetadata> messagesToRemove)
            throws AndesException {
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            for (AndesMessageMetadata metadata : messagesToRemove) {
                long messageID = metadata.getMessageID();
                deleteMessage(messageID, batch);
                deleteMessageFromQueue(messageID, storageQueueName, batch);
            }
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while deleting messages from " + storageQueueName, e);
        } finally {
            closeWriteBatch(batch);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(Collection<? extends AndesMessageMetadata> messagesToRemove) throws AndesException {
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            for (AndesMessageMetadata metadata : messagesToRemove) {
                long messageID = metadata.getMessageID();
                String storageQueueName = metadata.getStorageDestination();

                deleteMessage(messageID, batch);
                deleteMessageFromQueue(messageID, storageQueueName, batch);

                // Delete message from DLC if it is in DLC
                String messageDLCStatus = generateKey(MESSAGE, Long.toString(messageID), DLC_STATUS);

                if (Long.parseLong(asString(brokerStore.get(bytes(messageDLCStatus)))) == 1) {
                    deleteMessageFromQueue(messageID, DLC, batch);
                }
            }
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while deleting message list.", e);
        } finally {
            closeWriteBatch(batch);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(List<Long> messagesToRemove) throws AndesException {
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            for (Long messageID : messagesToRemove) {
                //Delete message metadata and content
                deleteMessage(messageID, batch);

                // Delete message from destination
                String messageQueueDestinationIdentifier = generateKey(MESSAGE, Long.toString(messageID),
                        DESTINATION_NAME);
                String storageQueueName = asString(brokerStore.get(bytes(messageQueueDestinationIdentifier)));
                deleteMessageFromQueue(messageID, storageQueueName, batch);

                // Delete message from DLC if it is in DLC
                String messageDLCQueueIdIdentifier = generateKey(MESSAGE, Long.toString(messageID), DLC_STATUS);

                if (Long.parseLong(asString(brokerStore.get(bytes(messageDLCQueueIdIdentifier)))) == 1) {
                    deleteMessageFromQueue(messageID, DLC, batch);
                }
            }
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while deleting message list.", e);
        } finally {
            closeWriteBatch(batch);
        }
    }

    /**
     * Add message delete operations to the batch. Operations are not written to the database within the method. It is
     * the responsibility of the caller to write the changes.
     *
     * Following keys will be removed.
     *
     * MESSAGE.$message_id.$offset.MESSAGE_CONTENT
     * MESSAGE.$message_id.DESTINATION_NAME
     * MESSAGE.$message_id.DLC_STATUS
     *
     * @param messageID id of the message which needed to be deleted
     * @param batch     accumulates operations to apply atomically
     */
    public void deleteMessage(long messageID, WriteBatch batch) {
        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(MESSAGE, Long.toString(messageID));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                String key = asString(keyIterator.peekNext().getKey());
                Long currentID = Long.parseLong(key.split(DELIMITER)[1]);

                if (currentID != messageID) {
                    break;
                }
                batch.delete(bytes(key));

                keyIterator.next();
            }
        } finally {
            closeKeyIterator(keyIterator);
        }
    }

    /**
     * Add message delete from destination operations to the batch. Operations are not written to the database within
     * the method. It is the responsibility of the caller to write the changes.
     *
     * Following keys will be deleted.
     *
     * DESTINATION.$destination_name.MESSAGE_METADATA.$message_id
     * DESTINATION.$destination_name.EXPIRATION_TIME.$message_id
     *
     * @param messageID id of the message which needed to be deleted
     * @param batch     accumulates operations to apply atomically
     */
    public void deleteMessageFromQueue(long messageID, String queueName, WriteBatch batch) {
        String queueMessageMetaDataIdentifier = generateKey(DESTINATION, queueName, MESSAGE_METADATA,
                Long.toString(messageID));
        String queueMessageExpireTimeIdentifier = generateKey(DESTINATION, queueName, MESSAGE_EXPIRATION_TIME,
                Long.toString(messageID));

        batch.delete(bytes(queueMessageMetaDataIdentifier));

        if (brokerStore.get(bytes(queueMessageExpireTimeIdentifier)) != null) {
            batch.delete(bytes(queueMessageExpireTimeIdentifier));
        }
        decreaseMessageCount(queueName, batch);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDLCMessages(List<AndesMessageMetadata> messagesToRemove) throws AndesException {
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            for (AndesMessageMetadata metadata : messagesToRemove) {
                long messageID = metadata.getMessageID();
                deleteMessage(messageID, batch);
                deleteMessageFromQueue(messageID, DLC, batch);
            }
            brokerStore.write(batch);
        } catch (DBException e) {
            throw new AndesException("Error occurred while deleting message list from DLC.", e);
        } finally {
            closeWriteBatch(batch);
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
        String head = generateKey(DESTINATION, queueName, MESSAGE_EXPIRATION_TIME, Long.toString(lowerBoundMessageID));
        keyIterator.seek(bytes(head));

        try {
            while (keyIterator.hasNext()) {
                //key arrangement : DESTINATION.$destination_name.MESSAGE_EXPIRATION_TIME.$message_id
                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(DELIMITER);

                if (!keySplit[2].equals(MESSAGE_EXPIRATION_TIME)) {
                    break;
                }
                Long messageID = Long.parseLong(keySplit[3]);
                Long messageExpiredTime = Long.parseLong(asString(keyIterator.peekNext().getValue()));

                if (messageExpiredTime < currentTime) {
                    expiredMessageIDList.add(messageID);
                }
                keyIterator.next();
            }
        } catch (DBException e) {
            throw new AndesException("Error occurred while retrieving expired messages from " + queueName, e);
        } finally {
            closeKeyIterator(keyIterator);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String
            destination) throws AndesException {
        // Not implemented in RDBMS store
        throw new AndesException(new NotImplementedException("Method is not implemented."));
    }

    @Override
    public long getApproximateQueueMessageCount(String storageQueueName) throws AndesException {
        return getMessageCountForQueue(storageQueueName);
    }

    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        // Not implemented in RDBMS store
        throw new AndesException(new NotImplementedException("Method is not implemented."));
    }

    @Override
    public void removeLocalQueueData(String storageQueueName) {
        // No cache is used
        throw new NotImplementedException("Method is not implemented.");
    }

    @Override
    public void incrementMessageCountForQueue(String storageQueueName, long incrementBy) throws AndesException {
        // Not implemented in RDBMS store
        throw new AndesException(new NotImplementedException("Method is not implemented."));
    }

    @Override
    public void decrementMessageCountForQueue(String storageQueueName, long decrementBy) throws AndesException {
        // Not implemented in RDBMS store
        throw new AndesException(new NotImplementedException("Method is not implemented."));
    }

    // Retained store is not implemented as MB not supports MQTT
    @Override
    public void storeRetainedMessages(Map<String, AndesMessage> retainMap) throws AndesException {
        throw new AndesException(new NotImplementedException("Method is not implemented."));
    }

    @Override
    public List<String> getAllRetainedTopics() throws AndesException {
        throw new AndesException(new NotImplementedException("Method is not implemented."));
    }

    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {
        throw new AndesException(new NotImplementedException("Method is not implemented."));
    }

    @Override
    public DeliverableAndesMetadata getRetainedMetadata(String destination) throws AndesException {
        throw new AndesException(new NotImplementedException("Method is not implemented."));
    }

    /**
     * Close file based store connection.
     *
     * {@inheritDoc}
     */
    @Override
    public void close() {
        fileStoreConnection.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DtxStore getDtxStore() {
        return this.dtxStore;
    }

    /**
     * Close the given key iterator
     *
     * @param keyIterator key iterator which needs to be closed
     */
    public void closeKeyIterator(DBIterator keyIterator) {
        try {
            keyIterator.close();
        } catch (IOException e) {
            log.error("Error occurred while closing the key iterator.", e);
        }
    }

    /**
     * Close the given write batch
     *
     * @param batch write batch which needs to be closed
     */
    public void closeWriteBatch(WriteBatch batch) {
        try {
            batch.close();
        } catch (IOException e) {
            log.error("Error occurred while closing the write batch.", e);
        }
    }

    /**
     * Return LevelDB store object
     *
     * @return DB LevelDB store object
     */
    protected DB getDB() {
        return this.brokerStore;
    }

    /**
     * Create key name when identifiers are provided
     *
     * @param identifiers identifiers which are included in the key name
     * @return key
     */
    public String generateKey(String... identifiers) {
        String key = identifiers[0];

        for (int i = 1; i <= identifiers.length - 2; i++) {
            key = key + DELIMITER + identifiers[i];
        }
        return key + DELIMITER + identifiers[identifiers.length - 1];
    }
}
