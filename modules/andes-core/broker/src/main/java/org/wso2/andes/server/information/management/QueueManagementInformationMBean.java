/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/

package org.wso2.andes.server.information.management;

import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.common.ByteBuffer;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.BasicContentHeaderProperties;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.kernel.DisablePubAckImpl;
import org.wso2.andes.kernel.FlowControlListener;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.disruptor.compression.LZ4CompressionHelper;
import org.wso2.andes.kernel.disruptor.inbound.InboundQueueEvent;
import org.wso2.andes.kernel.registry.StorageQueueRegistry;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.management.common.mbeans.QueueManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostImpl;
import org.wso2.andes.transport.codec.BBDecoder;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

/**
 * This class contains all operations such as addition, deletion, purging, browsing, etc. that are invoked by the UI
 * console with relation to queues.
 */
public class QueueManagementInformationMBean extends AMQManagedObject implements QueueManagementInformation {

    public static final String MIME_TYPE_TEXT_PLAIN = "text/plain";
    public static final String MIMI_TYPE_TEXT_XML = "text/xml";
    public static final String MIME_TYPE_APPLICATION_JAVA_OBJECT_STREAM = "application/java-object-stream";
    public static final String MIME_TYPE_AMQP_MAP = "amqp/map";
    public static final String MIME_TYPE_JMS_MAP_MESSAGE = "jms/map-message";
    public static final String MIME_TYPE_JMS_STREAM_MESSAGE = "jms/stream-message";
    public static final String MIME_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";
    private static Log log = LogFactory.getLog(QueueManagementInformationMBean.class);

    private final QueueRegistry queueRegistry;

    private final String PURGE_QUEUE_ERROR = "Error in purging queue : ";

    private final String MESSAGE_COUNT_RETRIEVE_ERROR = "Error while retrieving message count queue : ";

    // OpenMBean data types for viewMessageContent method
    private static CompositeType _msgContentType = null;
    private static OpenType[] _msgContentAttributeTypes = new OpenType[8];

    /**
     * Publisher Acknowledgements are disabled for this MBean
     * hence using DisablePubAckImpl to drop any pub ack request by Andes
     */
    private final DisablePubAckImpl disablePubAck;

    /**
     * The message restore flowcontrol blocking state.
     * If true message restore will be interrupted from dead letter channel.
     */
    boolean restoreBlockedByFlowControl = false;

    private static final int CHARACTERS_TO_SHOW = 15;

    /**
     * Maximum size a message will be displayed on UI
     */
    public static final Integer MESSAGE_DISPLAY_LENGTH_MAX = AndesConfigurationManager
            .readValue(AndesConfiguration.MANAGEMENT_CONSOLE_MAX_DISPLAY_LENGTH_FOR_MESSAGE_CONTENT);

    /**
     * Shown to user has a indication that the particular message has more content than shown in UI
     */
    public static final String DISPLAY_CONTINUATION = "...";

    /**
     * Message shown in UI if message content exceed the limit - Further enhancement,
     * these needs to read from a resource bundle
     */
    public static final String DISPLAY_LENGTH_EXCEEDED = "Message Content is too large to display.";

    protected static final byte BOOLEAN_TYPE = (byte) 1;

    protected static final byte BYTE_TYPE = (byte) 2;

    protected static final byte BYTEARRAY_TYPE = (byte) 3;

    protected static final byte SHORT_TYPE = (byte) 4;

    protected static final byte CHAR_TYPE = (byte) 5;

    protected static final byte INT_TYPE = (byte) 6;

    protected static final byte LONG_TYPE = (byte) 7;

    protected static final byte FLOAT_TYPE = (byte) 8;

    protected static final byte DOUBLE_TYPE = (byte) 9;

    protected static final byte STRING_TYPE = (byte) 10;

    protected static final byte NULL_STRING_TYPE = (byte) 11;

    /**
     * This is set when reading a byte array. The readBytes(byte[]) method supports multiple calls to read
     * a byte array in multiple chunks, hence this is used to track how much is left to be read
     */
    private int byteArrayRemaining = -1;

    /**
     * Used to get configuration values related to compression and used to decompress message content
     */
    LZ4CompressionHelper lz4CompressionHelper;

    /**
     * AndesChannel for this dead letter channel restore which implements flow control.
     */
    AndesChannel andesChannel;

    /***
     * Virtual host information are needed in the constructor to evaluate user permissions for
     * queue management actions.(e.g. purge)
     *
     * @param vHostMBean Used to access the virtual host information
     * @throws NotCompliantMBeanException
     */
    public QueueManagementInformationMBean(VirtualHostImpl.VirtualHostMBean vHostMBean)
            throws NotCompliantMBeanException, OpenDataException {
        super(QueueManagementInformation.class, QueueManagementInformation.TYPE);

        andesChannel = Andes.getInstance().createChannel(new FlowControlListener() {
            @Override
            public void block() {
                restoreBlockedByFlowControl = true;
            }

            @Override
            public void unblock() {
                restoreBlockedByFlowControl = false;
            }

            @Override
            public void disconnect() {
                // Do nothing. since its not applicable.
            }
        });

        VirtualHost virtualHost = vHostMBean.getVirtualHost();

        queueRegistry = virtualHost.getQueueRegistry();
        disablePubAck = new DisablePubAckImpl();

        _msgContentAttributeTypes[0] = SimpleType.STRING; // For message properties
        _msgContentAttributeTypes[1] = SimpleType.STRING; // For content type
        _msgContentAttributeTypes[2] = new ArrayType(1, SimpleType.STRING); // For message content
        _msgContentAttributeTypes[3] = SimpleType.STRING; // For JMS message id
        _msgContentAttributeTypes[4] = SimpleType.BOOLEAN; // For redelivered
        _msgContentAttributeTypes[5] = SimpleType.LONG; // For JMS timeStamp
        _msgContentAttributeTypes[6] = SimpleType.STRING; // For dlc message destination
        _msgContentAttributeTypes[7] = SimpleType.LONG; // For andes message metadata id
        _msgContentType = new CompositeType("Message Content", "Message content for queue browse",
                VIEW_MSG_CONTENT_COMPOSITE_ITEM_NAMES_DESC
                        .toArray(new String[VIEW_MSG_CONTENT_COMPOSITE_ITEM_NAMES_DESC.size()]),
                VIEW_MSG_CONTENT_COMPOSITE_ITEM_NAMES_DESC
                        .toArray(new String[VIEW_MSG_CONTENT_COMPOSITE_ITEM_NAMES_DESC.size()]),
                _msgContentAttributeTypes);
        lz4CompressionHelper = new LZ4CompressionHelper();
    }

    public String getObjectInstanceName() {
        return QueueManagementInformation.TYPE;
    }

    /***
     * {@inheritDoc}
     *
     * @return
     */
    public synchronized String[] getAllQueueNames() {

        try {
            AndesMessageRouter queueMessageRouter = AndesContext.getInstance().
                    getMessageRouterRegistry().getMessageRouter(AMQPUtils.DIRECT_EXCHANGE_NAME);
            List<String> queueNameList = queueMessageRouter.getNamesOfAllQueuesBound();
            String[] queues = new String[queueNameList.size()];
            queueNameList.toArray(queues);
            return queues;

        } catch (Exception e) {
            throw new RuntimeException("Error in accessing destination queues", e);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Integer> getAllQueueCounts() {
        try {
            AndesMessageRouter queueMessageRouter = AndesContext.getInstance().
                    getMessageRouterRegistry().getMessageRouter(AMQPUtils.DIRECT_EXCHANGE_NAME);
            List<String> queueNames = queueMessageRouter.getNamesOfAllQueuesBound();
            return Andes.getInstance().getMessageCountForAllQueues(queueNames);
        } catch (AndesException exception) {
            throw new RuntimeException("Error retrieving message count for all queues", exception);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData getMessageCountOfQueuesAsCompositeData() {
        CompositeDataSupport support;
        try {
            Map<String, java.lang.Integer> messageCounts = getAllQueueCounts();

            OpenType[] messageCountAttributeTypes = new OpenType[messageCounts.size()];
            String[] itemNames = messageCounts.keySet().toArray(new String[0]);
            String[] itemDescriptions = messageCounts.keySet().toArray(new String[0]);
            for (int count = 0; count < messageCounts.size(); count++) {
                messageCountAttributeTypes[count] = SimpleType.INTEGER;
            }
            CompositeType messageCountCompositeType = new CompositeType("Message Count of Queues",
                    "Message count of queues", itemNames, itemDescriptions, messageCountAttributeTypes);
            support = new CompositeDataSupport(messageCountCompositeType, messageCounts);
        } catch (OpenDataException e) {
            log.error("Error in accessing retrieving message count information", e);
            throw new RuntimeException("Error in accessing retrieving message count information", e);
        }
        return support;
    }

    /**
     * Get names of all durable queues created and registered in broker.
     * @return set of unique names of queues
     */
    @Override
    public Set<String> getNamesOfAllDurableQueues() {
        Set<String> namesOfDurableQueues = new HashSet<>();
        StorageQueueRegistry queueRegistry = AndesContext.getInstance().getStorageQueueRegistry();
        List<StorageQueue> queues = queueRegistry.getAllStorageQueues();
        for (StorageQueue queue : queues) {
            if(queue.isDurable()) {
                namesOfDurableQueues.add(queue.getName());
            }
        }
        return namesOfDurableQueues;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isQueueExists(String queueName) {
        try {
            List<String> queuesList = AndesContext.getInstance().
                    getStorageQueueRegistry().getAllStorageQueueNames();
            return queuesList.contains(queueName);
        } catch (Exception e) {
            throw new RuntimeException("Error in accessing destination queues", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllMessagesInQueue(@MBeanOperationParameter(name = "queueName",
                                                                  description = "Name of the queue to delete messages from") String queueName,
            @MBeanOperationParameter(name = "ownerName",
                                     description = "Username of user that calls for " + "purge") String ownerName)
            throws MBeanException {

        AMQQueue queue = queueRegistry.getQueue(new AMQShortString(queueName));

        try {
            if (queue == null) {
                throw new JMException("The Queue " + queueName + " is not a registered queue.");
            }

            queue.purge(0l); //This is to trigger the AMQChannel purge event so that the queue
            // state of qpid is updated. This method also validates the request owner and throws
            // an exception if permission is denied.

            InboundQueueEvent storageQueue = AMQPUtils.createInboundQueueEvent(queue);
            int purgedMessageCount = Andes.getInstance().purgeQueue(storageQueue);
            log.info("Total message count purged for queue (from store) : " + queueName + " : " +
                    purgedMessageCount + ". All in memory messages received before the purge call" +
                    " are abandoned from delivery phase. ");

        } catch (JMException jme) {
            if (jme.toString().contains("not a registered queue")) {
                throw new MBeanException(jme, "The Queue " + queueName + " is not a registered " +
                        "queue.");
            } else {
                throw new MBeanException(jme, PURGE_QUEUE_ERROR + queueName);
            }
        } catch (AMQException | AndesException amqex) {
            throw new MBeanException(amqex, PURGE_QUEUE_ERROR + queueName);
        }
    }

    /**
     * Delete a selected message list from a given Dead Letter Queue of a tenant.
     *
     * @param andesMetadataIDs     The browser message Ids
     * @param destinationQueueName The Dead Letter Queue Name for the tenant
     */
    @Override
    public void deleteMessagesFromDeadLetterQueue(
            @MBeanOperationParameter(name = "andesMetadataIDs",
                                     description = "ID of the Messages to Be DELETED") long[] andesMetadataIDs,
            @MBeanOperationParameter(name = "destinationQueueName",
                                     description = "The Dead Letter Queue Name for the selected tenant")
                                     String destinationQueueName) {

        List<AndesMessageMetadata> messageMetadataList = new ArrayList<>(andesMetadataIDs.length);

        for (long andesMetadataID : andesMetadataIDs) {
            AndesMessageMetadata messageToRemove =
                    new AndesMessageMetadata(andesMetadataID, destinationQueueName, ProtocolType.AMQP);
            messageToRemove.setStorageDestination(destinationQueueName);
            messageToRemove.setDestination(destinationQueueName);
            messageMetadataList.add(messageToRemove);
        }

        // Deleting messages which are in the list.
        try {
            Andes.getInstance().deleteMessagesFromDLC(messageMetadataList);
        } catch (AndesException e) {
            throw new RuntimeException("Error deleting messages from Dead Letter Channel", e);
        }
    }

    /***
    * {@inheritDoc}
    */
    @Override
    public void restoreSelectedMessagesFromDeadLetterChannel(
            @MBeanOperationParameter(name = "andesMessageIds",
                    description = "IDs of the Messages to Be restored") long[] andesMessageIds,
            @MBeanOperationParameter(name = "destinationQueueName",
                    description = "Original destination queue of the messages") String destinationQueueName)
            throws MBeanException {

        if (null != andesMessageIds) {
            int movedMessageCount = -1;
            List<Long> andesMessageIdList = new ArrayList<>(andesMessageIds.length);
            Collections.addAll(andesMessageIdList, ArrayUtils.toObject(andesMessageIds));
            try {
                movedMessageCount = moveMessagesFromDLCToNewDestination(andesMessageIdList, destinationQueueName,
                        destinationQueueName, true);
            } catch (AndesException ex) {
                throw new MBeanException(ex, "Error occurred when restoring messages from DLC to original qeueue : " +
                        destinationQueueName + " movedMessageCount : " + movedMessageCount);
            }
        }
    }

    /***
     * {@inheritDoc}
     */
    @Override
    public void rerouteSelectedMessagesFromDeadLetterChannel(
            @MBeanOperationParameter(name = "andesMessageIds",
                    description = "IDs of the Messages to Be Restored") long[] andesMessageIds,
            @MBeanOperationParameter(name = "sourceQueue",
                    description = "The  original queue name of the messages") String sourceQueue,
            @MBeanOperationParameter(name = "targetQueue",
                    description = "New destination queue for the messages") String targetQueue)
            throws MBeanException {

        if (null != andesMessageIds) {
            int movedMessageCount = -1;
            List<Long> andesMessageIdList = new ArrayList<>(andesMessageIds.length);
            Collections.addAll(andesMessageIdList, ArrayUtils.toObject(andesMessageIds));
            try {
                movedMessageCount = moveMessagesFromDLCToNewDestination(andesMessageIdList, sourceQueue, targetQueue,
                        false);
            } catch (AndesException ex) {
                throw new MBeanException(ex, "Error occurred when moving messages destined to sourceQueue : " + sourceQueue
                        + " from DLC to targetQueue : " + targetQueue + ". movedMessageCount : "
                        + movedMessageCount);
            }
        }
    }

    /**
     * Create new andes message and update the chunk details.
     *
     * @param messageMetadata message metadata
     * @param messageParts message parts list
     * @return andesMessage
     */
    private AndesMessage createMessage(AndesMessageMetadata messageMetadata, List<AndesMessagePart> messageParts) {
        AndesMessageMetadata clonedMetadata = messageMetadata.shallowCopy(messageMetadata.getMessageID());
        AndesMessage andesMessage = new AndesMessage(clonedMetadata);
        // Update Andes message with all the chunk details
        for (AndesMessagePart messagePart : messageParts) {
            andesMessage.addMessagePart(messagePart);
        }
        return  andesMessage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData[] browseQueue(@MBeanOperationParameter(name = "queueName",
                                                                description = "Name of queue to browse "
                                                                        + "messages") String queueName,
            @MBeanOperationParameter(name = "lastMsgId",
                                     description = "Browse message this message id " + "onwards") long nextMsgId,
            @MBeanOperationParameter(name = "maxMsgCount",
                                     description = "Maximum message count per " + "request") int maxMsgCount)
            throws MBeanException {
        List<CompositeData> compositeDataList = new ArrayList<>();
        try {

            List<AndesMessageMetadata> nextNMessageMetadataFromQueue;
            if (!DLCQueueUtils.isDeadLetterQueue(queueName)) {
                nextNMessageMetadataFromQueue = Andes.getInstance()
                        .getNextNMessageMetadataFromQueue(queueName, nextMsgId, maxMsgCount);
            } else {
                nextNMessageMetadataFromQueue = Andes.getInstance()
                        .getNextNMessageMetadataFromDLC(queueName, nextMsgId, maxMsgCount);
            }

            return getDisplayableMetaData(nextNMessageMetadataFromQueue, true);

        } catch (AndesException e) {
            throw new MBeanException(e, "Error occurred in browse queue.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNumberOfMessagesInDLCForQueue(String queueName) throws MBeanException {
        try {
            return Andes.getInstance().getMessageCountInDLCForQueue(queueName,
                    DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(queueName));
        } catch (AndesException e) {
            throw new MBeanException(e, "Error restoring messages from dead letter channel for:" + queueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositeData[] getMessagesInDLCForQueue(@MBeanOperationParameter(name = "queueName",
                                                                            description = "Name of queue to browse "
                                                                                    + "messages") String queueName,
            @MBeanOperationParameter(name = "lastMsgId",
                                     description = "Browse message this " + "onwards") long nextMsgId,
            @MBeanOperationParameter(name = "maxMsgCount",
                                     description = "Maximum message count " + "per request") int maxMessageCount)
            throws MBeanException {

        try {

            List<AndesMessageMetadata> nextNMessageMetadataFromQueue;
            if (!DLCQueueUtils.isDeadLetterQueue(queueName)) {
                nextNMessageMetadataFromQueue = Andes.getInstance().getNextNMessageMetadataInDLCForQueue(queueName,
                        DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(queueName), nextMsgId,
                        maxMessageCount);
            } else {
                nextNMessageMetadataFromQueue = Andes.getInstance().getNextNMessageMetadataFromDLC(
                        DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(queueName), nextMsgId,
                        maxMessageCount);
            }
            return getDisplayableMetaData(nextNMessageMetadataFromQueue, true);
        } catch (AndesException e) {
            throw new MBeanException(e, "Error occurred in browse queue.");
        }

    }

    /**
     * Extract MapMessage content from ByteBuffer
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @return extracted content as text
     */
    private String extractMapMessageContent(ByteBuffer wrapMsgContent) {
        wrapMsgContent.rewind();
        BBDecoder decoder = new BBDecoder();
        decoder.init(wrapMsgContent.buf());
        Map<String, Object> mapMassage = decoder.readMap();
        String wholeMsg = "";
        for (Map.Entry<String, Object> entry : mapMassage.entrySet()) {
            String mapName = entry.getKey();
            String mapVal = entry.getValue().toString();
            StringBuilder messageContentBuilder = new StringBuilder();
            wholeMsg = StringEscapeUtils.escapeHtml(
                    messageContentBuilder.append(mapName).append(": ").append(mapVal).append(", ").toString()).trim();
        }
        return wholeMsg;
    }

    /**
     * Extract StreamMessage from ByteBuffer
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param encoding       message encoding
     * @return extracted content as text
     * @throws CharacterCodingException
     */
    private String extractStreamMessageContent(ByteBuffer wrapMsgContent, String encoding)
            throws CharacterCodingException, AndesException {
        String wholeMsg;
        boolean eofReached = false;
        StringBuilder messageContentBuilder = new StringBuilder();

        while (!eofReached) {
            Object obj = readObject(wrapMsgContent, encoding);
            // obj could be null if the wire type is AbstractBytesTypedMessage.NULL_STRING_TYPE
            if (null != obj) {
                messageContentBuilder.append(obj.toString()).append(", ");
            }

        }

        wholeMsg = StringEscapeUtils.escapeHtml(messageContentBuilder.toString());
        return wholeMsg;
    }

    /**
     * Extract ByteMessage from ByteBuffer
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param byteMsgContent byte[] of message content
     * @return extracted content as text
     */
    private String extractByteMessageContent(ByteBuffer wrapMsgContent, byte[] byteMsgContent) {
        String wholeMsg;
        if (byteMsgContent == null) {
            throw new IllegalArgumentException("byte array must not be null");
        }
        int count = (wrapMsgContent.remaining() >= byteMsgContent.length ?
                byteMsgContent.length :
                wrapMsgContent.remaining());
        if (count == 0) {
            wholeMsg = String.valueOf(-1);
        } else {
            wrapMsgContent.get(byteMsgContent, 0, count);
            wholeMsg = String.valueOf(count);
        }
        return wholeMsg;
    }

    /**
     * Extract TextMessage from ByteBuffer
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param encoding       message encoding
     * @return extracted content as text
     * @throws CharacterCodingException
     */
    private String extractTextMessageContent(ByteBuffer wrapMsgContent, String encoding)
            throws CharacterCodingException {
        String wholeMsg;
        wholeMsg = wrapMsgContent.getString(Charset.forName(encoding).newDecoder());
        return wholeMsg;
    }

    /**
     * Read object from StreamMessage ByteBuffer content
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param encoding       message encoding
     * @return Object extracted from ByteBuffer
     * @throws CharacterCodingException
     */
    private Object readObject(ByteBuffer wrapMsgContent, String encoding)
            throws AndesException, CharacterCodingException {
        int position = wrapMsgContent.position();
        checkAvailable(1, wrapMsgContent);
        byte wireType = wrapMsgContent.get();
        Object result = null;
        try {
            switch (wireType) {
            case BOOLEAN_TYPE:
                checkAvailable(1, wrapMsgContent);
                result = wrapMsgContent.get() != 0;
                break;
            case BYTE_TYPE:
                checkAvailable(1, wrapMsgContent);
                result = wrapMsgContent.get();
                break;
            case BYTEARRAY_TYPE:
                checkAvailable(4, wrapMsgContent);
                int size = wrapMsgContent.getInt();
                if (size == -1) {
                    result = null;
                } else {
                    byteArrayRemaining = size;
                    byte[] bytesResult = new byte[size];
                    readBytesImpl(wrapMsgContent, bytesResult);
                    result = bytesResult;
                }
                break;
            case SHORT_TYPE:
                checkAvailable(2, wrapMsgContent);
                result = wrapMsgContent.getShort();
                break;
            case CHAR_TYPE:
                checkAvailable(2, wrapMsgContent);
                result = wrapMsgContent.getChar();
                break;
            case INT_TYPE:
                checkAvailable(4, wrapMsgContent);
                result = wrapMsgContent.getInt();
                break;
            case LONG_TYPE:
                checkAvailable(8, wrapMsgContent);
                result = wrapMsgContent.getLong();
                break;
            case FLOAT_TYPE:
                checkAvailable(4, wrapMsgContent);
                result = wrapMsgContent.getFloat();
                break;
            case DOUBLE_TYPE:
                checkAvailable(8, wrapMsgContent);
                result = wrapMsgContent.getDouble();
                break;
            case NULL_STRING_TYPE:
                result = null;
                break;
            case STRING_TYPE:
                checkAvailable(1, wrapMsgContent);
                result = wrapMsgContent.getString(Charset.forName(encoding).newDecoder());
                break;
            }
            return result;
        } catch (RuntimeException e) {
            wrapMsgContent.position(position);
            throw e;
        }
    }

    /**
     * Read byte[] array object and return length
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param bytes          byte[] object
     * @return length of byte[] array
     */
    private int readBytesImpl(ByteBuffer wrapMsgContent, byte[] bytes) {
        int count = (byteArrayRemaining >= bytes.length ? bytes.length : byteArrayRemaining);
        byteArrayRemaining -= count;
        if (count == 0) {
            return 0;
        } else {
            wrapMsgContent.get(bytes, 0, count);
            return count;
        }
    }

    /**
     * Check that there is at least a certain number of bytes available to read
     *
     * @param length the number of bytes
     * @throws  if there are less than len bytes available to read
     */
    private void checkAvailable(int length, ByteBuffer byteBuffer) throws AndesException {
        if (byteBuffer.remaining() < length) {
            throw new AndesException("Unable to read " + length + " bytes");
        }
    }

    /**
     * Return readable name for ContentType of message
     *
     * @param contentType content type of message
     * @return readable name
     */
    private String getReadableNameForMessageContentType(String contentType) {
        if (StringUtils.isNotBlank(contentType)) {
            if (contentType.equals(MIME_TYPE_TEXT_PLAIN) || contentType.equals(MIMI_TYPE_TEXT_XML)) {
                contentType = "Text";
            } else if (contentType.equals(MIME_TYPE_APPLICATION_JAVA_OBJECT_STREAM)) {
                contentType = "Object";
            } else if (contentType.equals(MIME_TYPE_AMQP_MAP) || contentType.equals(MIME_TYPE_JMS_MAP_MESSAGE)) {
                contentType = "Map";
            } else if (contentType.equals(MIME_TYPE_JMS_STREAM_MESSAGE)) {
                contentType = "Stream";
            } else if (contentType.equals(MIME_TYPE_APPLICATION_OCTET_STREAM)) {
                contentType = "Byte";
            }
        }
        return contentType;
    }

    /**
     * Retrieve a valid andes messageId list from a given browser message Id list.
     *
     * @param browserMessageIdList List of browser messageIds.
     * @return Valid Andes MessageId list
     */
    private LongArrayList getValidAndesMessageIdList(String[] browserMessageIdList) {
        LongArrayList andesMessageIdList = new LongArrayList(browserMessageIdList.length);

        for (String browserMessageId : browserMessageIdList) {
            Long andesMessageId = AndesUtils.getAndesMessageId(browserMessageId);

            if (andesMessageId > 0) {
                andesMessageIdList.add(andesMessageId);
            } else {
                log.warn("A valid message could not be found for the message Id : " + browserMessageId);
            }
        }

        return andesMessageIdList;
    }

    /**
     * We are returning message count to the UI from this method.
     * When it has received Acks from the clients more than the message actual
     * message in the  queue,( This can happen when a copy of a message get
     * delivered to the consumer while the ACK for the previouse message was
     * on the way back to server), Message count is becoming minus.
     * <p>
     * So from now on , we ll not provide minus values to the front end since
     * it is not acceptable
     */
    public long getMessageCount(String queueName, String msgPattern) throws MBeanException {

        if (log.isDebugEnabled()) {
            log.debug("Counting at queue : " + queueName);
        }

        long messageCount = 0;
        try {
            if (!DLCQueueUtils.isDeadLetterQueue(queueName)) {
                if ("queue".equals(msgPattern)) {
                    messageCount = Andes.getInstance().getMessageCountOfQueue(queueName);
                }
            } else {
                messageCount = Andes.getInstance().getMessageCountInDLC(queueName);
            }

        } catch (AndesException e) {
            log.error(MESSAGE_COUNT_RETRIEVE_ERROR + queueName, e);
            throw new MBeanException(e, MESSAGE_COUNT_RETRIEVE_ERROR + queueName);
        }

        return messageCount;
    }

    /**
     * Get DLC queue by name specified if already created
     *
     * @param queueName name of the DLc queue (including tenant information)
     * @return a Map with <DLCQueueName,MessageCount> entry
     */
    public Map<String, Long> getDLCQueueInformation(String queueName) throws MBeanException {

        Map<String, Long> DLCQueueInformation;

        try {
            DLCQueueInformation = new HashMap<>(1);

            AndesMessageRouter DLCMessageRouter = AndesContext.getInstance().
                    getMessageRouterRegistry().getMessageRouter(AMQPUtils.DLC_EXCHANGE_NAME);

            List<StorageQueue> DLCQueues = DLCMessageRouter.getAllBoundQueues();

            for (StorageQueue dlcQueue : DLCQueues) {
                if(queueName.equals(dlcQueue.getName())) {
                    DLCQueueInformation.put(dlcQueue.getName(), dlcQueue.getMessageCount());
                    break;
                }
            }
        } catch (AndesException e) {
            throw new MBeanException(e, "Error while receiving DLC queue Information from MBeans");
        }

        return DLCQueueInformation;
    }

    /***
     * {@inheritDoc}
     */
    public int getSubscriptionCount(String queueName) {
        try {
            return AndesContext.getInstance().getAndesSubscriptionManager().
                    numberOfSubscriptionsInCluster(queueName, ProtocolType.AMQP);
        } catch (Exception e) {
            throw new RuntimeException("Error in getting subscriber count", e);
        }
    }

    /**
     * Method to display a list of messages when browsed.
     *
     * @param metadataList the list of message metadata
     * @return Composite data array of properties of all messages
     * @throws MBeanException if an OpenDataException occurs while mapping JMS headers for the Message.
     */
    private CompositeData[] getDisplayableMetaData(List<AndesMessageMetadata> metadataList, boolean includeContent) throws MBeanException {
        List<CompositeData> compositeDataList = new ArrayList<>();
        try {
            for (AndesMessageMetadata andesMessageMetadata : metadataList) {
                Object[] itemValues = getItemValues(andesMessageMetadata, includeContent);
                if (null != itemValues) {
                    CompositeDataSupport support = new CompositeDataSupport(_msgContentType,
                            VIEW_MSG_CONTENT_COMPOSITE_ITEM_NAMES_DESC
                                    .toArray(new String[VIEW_MSG_CONTENT_COMPOSITE_ITEM_NAMES_DESC.size()]),
                            itemValues);
                    compositeDataList.add(support);
                }
            }
        } catch (OpenDataException exception) {
            throw new MBeanException(exception, "Error occurred when formatting message in queue.");
        }
        return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
    }

    /**
     * Method to get an array of properties of a single message.
     *
     * @param andesMessageMetadata andes message metadata to be parsed
     * @return an array of properties of the message
     * @throws MBeanException if an AMQException occurs while reading Message Content headers.
     */
    private Object[] getItemValues(AndesMessageMetadata andesMessageMetadata, boolean includeContent) throws MBeanException {
        try {

            Object[] itemValues = null;
            //get AMQMessage from AndesMessageMetadata
            AMQMessage amqMessage = AMQPUtils.getAMQMessageFromAndesMetaData(andesMessageMetadata);
            //header properties from AMQMessage
            BasicContentHeaderProperties properties = (BasicContentHeaderProperties) amqMessage.getContentHeaderBody()
                    .getProperties();
            //get custom header properties of AMQMessage
            StringBuilder stringBuilder = new StringBuilder();
            for (String headerKey : properties.getHeaders().keys()) {
                stringBuilder.append(headerKey).append(" = ").append(properties.getHeaders().get(headerKey));
                stringBuilder.append(", ");
            }
            if (null != properties.getCorrelationId()) {
                stringBuilder.append("JMSCorrelationID").append(" = ").append(properties.getCorrelationId()).append
                        (", ");
            }
            if (null != properties.getReplyTo()) {
                stringBuilder.append("JMSReplyTo").append(" = ").append(properties.getReplyTo()).append(", ");
            }
            if (null != properties.getType()) {
                stringBuilder.append("JMSType").append(" = ").append(properties.getType()).append(", ");
            }
            String msgProperties = stringBuilder.toString();
            //get content type
            String contentType = properties.getContentTypeAsString();
            //get message id
            String messageId = properties.getMessageIdAsString();
            //get redelivered
            boolean redelivered = false;
            //get timestamp
            long timeStamp = properties.getTimestamp();
            //get destination
            String destination = andesMessageMetadata.getDestination();
            //get AndesMessageMetadata id
            long andesMessageMetadataId = andesMessageMetadata.getMessageID();

            //content is constructing
            final int bodySize = (int) amqMessage.getSize();

            if (includeContent) {
                AndesMessagePart constructedContent = constructContent(bodySize, amqMessage);
                byte[] messageContent = constructedContent.getData();
                int position = constructedContent.getOffset();
                //if position did not proceed, there is an error receiving content. If not, decode content
                if (!((bodySize != 0) && (position == 0))) {
                    String[] content = decodeContent(amqMessage, messageContent);
                    //set content type of message to readable name
                    contentType = getReadableNameForMessageContentType(contentType);
                    //set CompositeData of message
                    itemValues = new Object[]{msgProperties, contentType, content, messageId, redelivered,
                            timeStamp, destination, andesMessageMetadataId};
                } else if (bodySize == 0) { //empty message
                    itemValues = new Object[]{msgProperties, contentType, "", messageId, redelivered,
                            timeStamp, destination, andesMessageMetadataId};
                }
            } else {
                itemValues = new Object[]{msgProperties, contentType, new String[]{}, messageId, redelivered,
                        timeStamp, destination, andesMessageMetadataId};
            }
            return itemValues;
        } catch (AMQException exception) {
            throw new MBeanException(exception, "Error occurred when formatting message with Id : " +
                    andesMessageMetadata.getMessageID() + " assigned to queue : " + andesMessageMetadata.getDestination());
        }
    }

    /**
     * Method to construct message body of a single message.
     *
     * @param bodySize   Original content size of the message
     * @param amqMessage AMQMessage
     * @return Message content and last position of written data as an AndesMessagePart
     * @throws MBeanException
     */
    private AndesMessagePart constructContent(int bodySize, AMQMessage amqMessage) throws MBeanException {

        AndesMessagePart andesMessagePart;

        if (amqMessage.getMessageMetaData().isCompressed()) {
            /* If the current message was compressed by the server, decompress the message content and, get it as an
             * AndesMessagePart
             */
            long messageID = amqMessage.getMessageId();
            LongArrayList messList = new LongArrayList();
            messList.add(messageID);

            try {
                LongObjectHashMap<List<AndesMessagePart>> contentListMap = MessagingEngine.getInstance()
                        .getContent(messList);
                List<AndesMessagePart> contentList = contentListMap.get(messageID);

                andesMessagePart = lz4CompressionHelper.getDecompressedMessage(contentList, bodySize);

            } catch (AndesException e) {
                throw new MBeanException(e,
                        "Error occurred while construct the message content. Message ID:" + amqMessage.getMessageId());
            }
        } else {
            byte[] messageContent = new byte[bodySize];

            //Getting a buffer, to write data into the byte array and to the buffer at the same time
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(messageContent);

            int position = 0;

            while (position < bodySize) {
                position = position + amqMessage.getContent(buffer, position);

                //If position did not proceed, there is an error receiving content
                if ((0 != bodySize) && (0 == position)) {
                    break;
                }

                //The limit is setting to the current position and then the position of the buffer is setting to zero
                buffer.flip();

                //The position of the buffer is setting to zero, the limit is setting to the capacity
                buffer.clear();
            }

            andesMessagePart = new AndesMessagePart();
            andesMessagePart.setData(messageContent);
            andesMessagePart.setOffSet(position);
        }

        return andesMessagePart;
    }

    /**
     * Method to decode content of a single message into text
     *
     * @param amqMessage     the message of which content need to be decoded
     * @param messageContent the byte array of message content to be decoded
     * @return A string array representing the decoded message content
     * @throws MBeanException
     */
    private String[] decodeContent(AMQMessage amqMessage, byte[] messageContent) throws MBeanException {

        try {
            //get encoding
            String encoding = amqMessage.getMessageHeader().getEncoding();
            if (encoding == null) {
                encoding = "UTF-8";
            }
            //get mime type of message
            String mimeType = amqMessage.getMessageHeader().getMimeType();
            // setting default mime type
            if (StringUtils.isBlank(mimeType)) {
                mimeType = MIME_TYPE_TEXT_PLAIN;
            }
            //create message content to readable text from ByteBuffer
            ByteBuffer wrapMsgContent = ByteBuffer.wrap(messageContent);
            String content[] = new String[2];
            String summaryMsg = "";
            String wholeMsg = "";

            //get TextMessage content to display
            if (mimeType.equals(MIME_TYPE_TEXT_PLAIN) || mimeType.equals(MIMI_TYPE_TEXT_XML)) {
                wholeMsg = extractTextMessageContent(wrapMsgContent, encoding);
                //get ByteMessage content to display
            } else if (mimeType.equals(MIME_TYPE_APPLICATION_OCTET_STREAM)) {
                wholeMsg = extractByteMessageContent(wrapMsgContent, messageContent);
                //get ObjectMessage content to display
            } else if (mimeType.equals(MIME_TYPE_APPLICATION_JAVA_OBJECT_STREAM)) {
                wholeMsg = "This Operation is Not Supported!";
                //get StreamMessage content to display
            } else if (mimeType.equals(MIME_TYPE_JMS_STREAM_MESSAGE)) {
                wholeMsg = extractStreamMessageContent(wrapMsgContent, encoding);
                //get MapMessage content to display
            } else if (mimeType.equals(MIME_TYPE_AMQP_MAP) || mimeType.equals(MIME_TYPE_JMS_MAP_MESSAGE)) {
                wholeMsg = extractMapMessageContent(wrapMsgContent);
            }
            //trim content to summary and whole message
            if (wholeMsg.length() >= CHARACTERS_TO_SHOW) {
                summaryMsg = wholeMsg.substring(0, CHARACTERS_TO_SHOW);
            } else {
                summaryMsg = wholeMsg;
            }
            if (wholeMsg.length() > MESSAGE_DISPLAY_LENGTH_MAX) {
                wholeMsg = wholeMsg.substring(0, MESSAGE_DISPLAY_LENGTH_MAX - 3) +
                        DISPLAY_CONTINUATION + DISPLAY_LENGTH_EXCEEDED;
            }
            content[0] = summaryMsg;
            content[1] = wholeMsg;
            return content;
        } catch (CharacterCodingException | AndesException exception) {
            throw new MBeanException(exception, "Error occurred in browse queue.");
        }
    }

    /**
     * Common method to restore a list of messages based on Id to its original queue or a different queue.
     *
     * @param messageIds             list of messages to be restored
     * @param sourceQueue            original destination queue of the messages.
     * @param targetQueue            new target destination of the messages.
     * @param restoreToOriginalQueue true if the messages need to be restored to their original
     *                               queues instead of a single target queue.
     * @return int Number of messages that were successfully restored.
     * @throws AndesException if the database calls to read/delete the messages/content fails.
     */
    private int moveMessagesFromDLCToNewDestination(List<Long> messageIds, String sourceQueue, String targetQueue,
                                                    boolean restoreToOriginalQueue)
            throws AndesException {

        List<AndesMessageMetadata> messagesToRemove = new ArrayList<>(messageIds.size());

        LongArrayList messageIdCollection = new LongArrayList();
        for (Long messageId : messageIds) {
            messageIdCollection.add(messageId);
        }

        int movedMessageCount = 0;
        LongObjectHashMap<List<AndesMessagePart>> messageContent = Andes.getInstance().getContent(messageIdCollection);
        boolean interruptedByFlowControl = false;

        for (Long messageId : messageIds) {
            if (restoreBlockedByFlowControl) {
                interruptedByFlowControl = true;
                break;
            }
            AndesMessageMetadata metadata = Andes.getInstance().getMessageMetaData(messageId);
            if (!restoreToOriginalQueue) {
                StorageQueue newStorageQueue = AndesContext.getInstance().getStorageQueueRegistry()
                        .getStorageQueue(targetQueue);

                // Set the new destination queue
                metadata.setDestination(targetQueue);
                metadata.setStorageDestination(targetQueue);
//                metadata.setMessageRouterName(newStorageQueue.getMessageRouter().getName());
//                metadata.updateMetadata(targetQueue, newStorageQueue.getMessageRouter().getName());
            }

            AndesMessageMetadata clonedMetadata = metadata.shallowCopy(metadata.getMessageID());
            AndesMessage andesMessage = new AndesMessage(clonedMetadata);

            messagesToRemove.add(metadata);
            // Update Andes message with all the chunk details
            if(!messageContent.isEmpty()) {
                List<AndesMessagePart> messageParts = messageContent.get(messageId);
                for (AndesMessagePart messagePart : messageParts) {
                    andesMessage.addMessagePart(messagePart);
                }
            }

            // Handover message to Andes. This will generate a new message ID and store it
            Andes.getInstance().messageReceived(andesMessage, andesChannel, disablePubAck);

            movedMessageCount++;
        }

        if (interruptedByFlowControl) {
            // Throw this out so UI will show this to the user as an error message.
            // Messages can be duplicated
            throw new AndesException("Message restore from dead letter queue has been interrupted by flow "
                    + "control. Messages in the DLC for sourceQueue : " + sourceQueue + " may be duplicated due to "
                    + "this situation. Please try again later. movedMessageCount : " + movedMessageCount);
        }

        // Delete old messages
        Andes.getInstance().deleteMessagesFromDLC(messagesToRemove);

        return movedMessageCount;
    }


    /***
     * {@inheritDoc}
     */
    @Override
    public CompositeData[] getMessageMetadataInDeadLetterChannel(
            @MBeanOperationParameter(name = "targetQueue", description = "Name of destination queue ") String targetQueue,
            @MBeanOperationParameter(name = "startMessageId",
                    description = "Message Id to start the resultset with.") long startMessageId,
            @MBeanOperationParameter(name = "pageLimit",
                    description = "Maximum message count required in a single response") int pageLimit)
            throws MBeanException {

        try {
            List<AndesMessageMetadata> nextNMessageMetadataFromQueue;

            if (!DLCQueueUtils.isDeadLetterQueue(targetQueue)) {
                nextNMessageMetadataFromQueue = Andes.getInstance().getNextNMessageMetadataInDLCForQueue(targetQueue,
                        DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(targetQueue), startMessageId,
                        pageLimit);
            } else {
                nextNMessageMetadataFromQueue = Andes.getInstance().getNextNMessageMetadataFromDLC(
                        DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(targetQueue), startMessageId,
                        pageLimit);
            }
            return getDisplayableMetaData(nextNMessageMetadataFromQueue, false);

        } catch (AndesException e) {
            throw new MBeanException(e,
                    "Error occurred when listing metadata in DLC for queue : " + targetQueue + " from message Id : "
                            + startMessageId + " onwards.");
        }
    }

    /***
     * {@inheritDoc}
     */
    @Override
    public int rerouteAllMessagesInDeadLetterChannelForQueue(
            @MBeanOperationParameter(name = "sourceQueue", description = "Name of the source queue") String sourceQueue,
            @MBeanOperationParameter(name = "targetQueue", description = "Name of the target queue") String targetQueue,
            @MBeanOperationParameter(name = "internalBatchSize", description = "Number of messages processed in a "
                    + "single database call.") int internalBatchSize)
            throws MBeanException {

        List<Long> currentMessageIdList;
        Long lastMessageId = 0L;
        int movedMessageCount = 0;

        // Get full name of Dead Letter Channel
        String dlcQueueName = DLCQueueUtils.identifyTenantInformationAndGenerateDLCString(sourceQueue);

        try {
            if (DLCQueueUtils.isDeadLetterQueue(sourceQueue)) {
                currentMessageIdList = Andes.getInstance()
                        .getNextNMessageIdsInDLC(dlcQueueName, lastMessageId, internalBatchSize);

                while (currentMessageIdList.size() > 0) {
                    int movedMessageCountInThisBatch = moveMessagesFromDLCToNewDestination(currentMessageIdList,
                            sourceQueue, targetQueue, false);

                    if (log.isDebugEnabled()) {
                        log.debug("Successfully restored messages from DLC to targetQueue: " + targetQueue
                                + " movedMessageCountInThisBatch : " + movedMessageCountInThisBatch);
                    }

                    movedMessageCount = movedMessageCount + movedMessageCountInThisBatch;
                    lastMessageId = currentMessageIdList.get(currentMessageIdList.size() - 1);

                    currentMessageIdList = Andes.getInstance().getNextNMessageIdsInDLC(dlcQueueName, lastMessageId,
                            internalBatchSize);
                }
            } else {
                currentMessageIdList = Andes.getInstance()
                        .getNextNMessageIdsInDLCForQueue(sourceQueue, dlcQueueName, lastMessageId,
                                internalBatchSize);

                while (currentMessageIdList.size() > 0) {
                    int movedMessageCountInThisBatch = moveMessagesFromDLCToNewDestination(currentMessageIdList,
                            sourceQueue, targetQueue, false);

                    if (log.isDebugEnabled()) {
                        log.debug("Successfully restored messages from sourceQueue : " + sourceQueue
                                + " to targetQueue : " + targetQueue + " movedMessageCountInThisBatch : "
                                + movedMessageCountInThisBatch);
                    }

                    movedMessageCount = movedMessageCount + movedMessageCountInThisBatch;
                    lastMessageId = currentMessageIdList.get(currentMessageIdList.size() - 1);

                    currentMessageIdList = Andes.getInstance()
                            .getNextNMessageIdsInDLCForQueue(sourceQueue, dlcQueueName,
                                    lastMessageId, internalBatchSize);
                }
            }
        } catch (AndesException ex) {
            throw new MBeanException(ex, "Error occurred when moving metadata destined to sourceQueue : " + sourceQueue
                    + " from DLC to targetQueue : " + targetQueue + ". movedMessageCount : " + movedMessageCount);
        }

        return movedMessageCount;
    }

}