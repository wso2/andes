/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.server.util;

import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.mina.common.ByteBuffer;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.framing.BasicContentHeaderProperties;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.disruptor.compression.LZ4CompressionHelper;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.transport.codec.BBDecoder;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.management.MBeanException;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

/**
 * Composite data helper to convert objects into {@link CompositeType} format for MBeans.
 */
public class CompositeDataHelper {

	/**
	 * Composite data helper inner class for destinations.
	 */
	public class DestinationCompositeDataHelper {
        public static final String DESTINATION_NAME = "DESTINATION_NAME";
        public static final String DESTINATION_OWNER = "DESTINATION_OWNER";
        public static final String IS_DURABLE = "IS_DURABLE";
        public static final String SUBSCRIPTION_COUNT = "SUBSCRIPTION_COUNT";
        public static final String MESSAGE_COUNT = "MESSAGE_COUNT";
        public static final String PROTOCOL_TYPE = "PROTOCOL_TYPE";
        public static final String DESTINATION_TYPE = "DESTINATION_TYPE";
        private List<String> DESTINATION_COMPOSITE_ITEM_NAMES = Collections.unmodifiableList(Arrays.asList
            (DESTINATION_NAME, DESTINATION_OWNER, IS_DURABLE, SUBSCRIPTION_COUNT, MESSAGE_COUNT, PROTOCOL_TYPE,
                                                                                                    DESTINATION_TYPE));

        private OpenType[] destinationAttributeTypes = new OpenType[7];
        private CompositeType destinationCompositeType = null;

		/**
		 * Converts an {@link AndesQueue} and the message count to a {@link CompositeDataSupport} object.
		 *
		 * @param destination  The {@link AndesQueue} object representing the destination.
		 * @param messageCount The message count for the represented destination.
		 * @return A {@link CompositeDataSupport} object. This can be casted to a {@link
		 * javax.management.openmbean.CompositeData}.
		 * @throws OpenDataException
		 */
		public CompositeDataSupport getDestinationAsCompositeData(AndesQueue destination, long messageCount) throws
				OpenDataException {
			setDestinationCompositeType();

			return new CompositeDataSupport(destinationCompositeType, DESTINATION_COMPOSITE_ITEM_NAMES.toArray(new
					String[DESTINATION_COMPOSITE_ITEM_NAMES.size()]), getDestinationItemValue(destination,
					messageCount));
		}

		/**
		 * Gets an object array that includes the properties of the destination along with the message count.
		 *
		 * @param destination  The {@link AndesQueue} object representing the destination.
		 * @param messageCount The message count for the represented destination.
		 * @return An object array with the properties of the destination.
		 */
		private Object[] getDestinationItemValue(AndesQueue destination, long messageCount) {
			return new Object[]{destination.queueName, destination.queueOwner, destination.isDurable,
			                    destination.subscriptionCount, messageCount, destination.getProtocolType().name(),
			                    destination.getDestinationType().name()};
		}

		/**
		 * Initializes the composite type for destinations to support Composite Data
		 *
		 * @throws OpenDataException
		 */
		private void setDestinationCompositeType() throws OpenDataException {
            destinationAttributeTypes[0] = SimpleType.STRING; // Destination Name
            destinationAttributeTypes[1] = SimpleType.STRING; // Destination Owner
            destinationAttributeTypes[2] = SimpleType.BOOLEAN; // Is Durable
            destinationAttributeTypes[3] = SimpleType.INTEGER; // Subscription Count
            destinationAttributeTypes[4] = SimpleType.LONG; // Message Count
            destinationAttributeTypes[5] = SimpleType.STRING; // Protocol Type
            destinationAttributeTypes[6] = SimpleType.STRING; // Destination Type
            destinationCompositeType = new CompositeType("Destination", "Destination details",
                    DESTINATION_COMPOSITE_ITEM_NAMES.toArray(new String[DESTINATION_COMPOSITE_ITEM_NAMES.size()]),
		            DESTINATION_COMPOSITE_ITEM_NAMES.toArray(new String[DESTINATION_COMPOSITE_ITEM_NAMES.size()]),
		            destinationAttributeTypes);
        }
    }

	/**
	 * Composite data helper inner class for subscriptions.
	 */
    public class SubscriptionCompositeDataHelper {
        public static final String SUBSCRIPTION_ID = "SUBSCRIPTION_ID";
        public static final String DESTINATION_NAME = "DESTINATION_NAME";
        public static final String TARGET_QUEUE_BOUND_EXCHANGE_NAME = "TARGET_QUEUE_BOUND_EXCHANGE_NAME";
        public static final String TARGET_QUEUE = "TARGET_QUEUE";
        public static final String IS_DURABLE = "IS_DURABLE";
        public static final String HAS_EXTERNAL_SUBSCRIPTIONS = "HAS_EXTERNAL_SUBSCRIPTIONS";
        public static final String PENDING_MESSAGE_COUNT = "PENDING_MESSAGE_COUNT";
        public static final String SUBSCRIBED_NODE = "SUBSCRIBED_NODE";
        public static final String PROTOCOL_TYPE = "PROTOCOL_TYPE";
        public static final String DESTINATION_TYPE = "DESTINATION_TYPE";
        private List<String> SUBSCRIPTION_COMPOSITE_ITEM_NAMES = Collections.unmodifiableList(Arrays.asList
                (SUBSCRIPTION_ID, DESTINATION_NAME, TARGET_QUEUE_BOUND_EXCHANGE_NAME, TARGET_QUEUE, IS_DURABLE,
                HAS_EXTERNAL_SUBSCRIPTIONS, PENDING_MESSAGE_COUNT, SUBSCRIBED_NODE, PROTOCOL_TYPE, DESTINATION_TYPE));

        private OpenType[] subscriptionAttributeTypes = new OpenType[10];
        private CompositeType subscriptionCompositeType = null;

		/**
		 * Converts an {@link AndesSubscription} and its pending message count to a {@link CompositeDataSupport}
		 * object.
		 *
		 * @param subscription        The {@link AndesSubscription} object.
		 * @param pendingMessageCount The pending messages for the specific subscription.
		 * @return A {@link CompositeDataSupport} object. This can be casted to a {@link
		 * javax.management.openmbean.CompositeData}.
		 * @throws OpenDataException
		 */
		public CompositeDataSupport getSubscriptionAsCompositeData(AndesSubscription subscription,
                                                                   long pendingMessageCount) throws OpenDataException {
            setSubscriptionCompositeType();

            return new CompositeDataSupport(subscriptionCompositeType,
		            SUBSCRIPTION_COMPOSITE_ITEM_NAMES.toArray(new String[SUBSCRIPTION_COMPOSITE_ITEM_NAMES.size()]),
                    getSubscriptionItemValue(subscription, pendingMessageCount));
        }

		/**
		 * Gets an object array that includes the properties of the subscription along with the pending message count.
		 *
		 * @param subscription        The {@link AndesSubscription} object representing the subscription.
		 * @param pendingMessageCount The pending message count for the represented subscription.
		 * @return An object array with the properties of the subscription.
		 */
		private Object[] getSubscriptionItemValue(AndesSubscription subscription, long pendingMessageCount) {
            return new Object[]{subscription.getSubscriptionID(),
                                subscription.getSubscribedDestination(),
                                subscription.getTargetQueueBoundExchangeName()            ,
                                subscription.getTargetQueue(),
                                subscription.isDurable(),
                                subscription.hasExternalSubscriptions(),
                                pendingMessageCount,
                                subscription.getSubscribedNode(),
                                subscription.getProtocolType().name(),
                                subscription.getDestinationType().name()};
        }

		/**
		 * Initializes the composite type for subscription to support Composite Data.
		 *
		 * @throws OpenDataException
		 */
        private void setSubscriptionCompositeType() throws OpenDataException {
            subscriptionAttributeTypes[0] = SimpleType.STRING; // SubscriptionID
            subscriptionAttributeTypes[1] = SimpleType.STRING; // SubscribedDestination
            subscriptionAttributeTypes[2] = SimpleType.STRING; // TargetQueueBoundExchangeName
            subscriptionAttributeTypes[3] = SimpleType.STRING; // TargetQueue
            subscriptionAttributeTypes[4] = SimpleType.BOOLEAN; // Durable
            subscriptionAttributeTypes[5] = SimpleType.BOOLEAN; // ExternalSubscriptions
            subscriptionAttributeTypes[6] = SimpleType.LONG; // Pending Message Count
            subscriptionAttributeTypes[7] = SimpleType.STRING; // SubscribedNode
            subscriptionAttributeTypes[8] = SimpleType.STRING; // ProtocolType
            subscriptionAttributeTypes[9] = SimpleType.STRING; // DestinationType
            subscriptionCompositeType = new CompositeType("Subscription", "Subscription details",
                    SUBSCRIPTION_COMPOSITE_ITEM_NAMES.toArray(new String[SUBSCRIPTION_COMPOSITE_ITEM_NAMES
                            .size()]), SUBSCRIPTION_COMPOSITE_ITEM_NAMES.toArray(new
                    String[SUBSCRIPTION_COMPOSITE_ITEM_NAMES.size()]), subscriptionAttributeTypes);
        }
    }

	/**
	 * Composite data helper inner class for messages.
	 */
    public class MessagesCompositeDataHelper {
        private static final String MIME_TYPE_TEXT_PLAIN = "text/plain";
        private static final String MIMI_TYPE_TEXT_XML = "text/xml";
        private static final String MIME_TYPE_APPLICATION_JAVA_OBJECT_STREAM = "application/java-object-stream";
        private static final String MIME_TYPE_AMQP_MAP = "amqp/map";
        private static final String MIME_TYPE_JMS_MAP_MESSAGE = "jms/map-message";
        private static final String MIME_TYPE_JMS_STREAM_MESSAGE = "jms/stream-message";
        private static final String MIME_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";

        /**
         * Used to get configuration values related to compression and used to decompress message content
         */
        LZ4CompressionHelper lz4CompressionHelper;

        private static final int CHARACTERS_TO_SHOW = 15;

        /**
         * Maximum size a message will be displayed on UI
         */
        private final Integer MESSAGE_DISPLAY_LENGTH_MAX = AndesConfigurationManager.readValue(
                                        AndesConfiguration.MANAGEMENT_CONSOLE_MAX_DISPLAY_LENGTH_FOR_MESSAGE_CONTENT);

        /**
         * Shown to user has a indication that the particular message has more content than shown in UI
         */
        private static final String DISPLAY_CONTINUATION = "...";

        /**
         * This is set when reading a byte array. The readBytes(byte[]) method supports multiple calls to read
         * a byte array in multiple chunks, hence this is used to track how much is left to be read
         */
        private int byteArrayRemaining = -1;

        /**
         * Message shown in UI if message content exceed the limit - Further enhancement,
         * these needs to read from a resource bundle
         */
        private static final String DISPLAY_LENGTH_EXCEEDED = "Message Content is too large to display.";

        private static final byte BOOLEAN_TYPE = (byte) 1;

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

        public static final String ANDES_METADATA_MESSAGE_ID = "ANDES_METADATA_MESSAGE_ID";
        public static final String DESTINATION_NAME = "DESTINATION_NAME";
        public static final String PROTOCOL = "PROTOCOL";
        public static final String MESSAGE_PROPERTIES = "MESSAGE_PROPERTIES";
        public static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";
        private List<String> MESSAGE_COMPOSITE_ITEM_NAMES = Collections.unmodifiableList(Arrays.asList
                (ANDES_METADATA_MESSAGE_ID, DESTINATION_NAME, MESSAGE_PROPERTIES, MESSAGE_CONTENT));

        private OpenType[] messageAttributeTypes = new OpenType[5];
        private CompositeType messageCompositeType = null;

        public MessagesCompositeDataHelper() {
            lz4CompressionHelper = new LZ4CompressionHelper();
        }

		/**
		 * Converts an {@link AndesMessageMetadata} and its pending message count to a {@link CompositeDataSupport}
		 * object.
		 *
		 * @param protocolType         The protocol for the message.
		 * @param andesMessageMetadata The {@link AndesMessageMetadata} object representing the message.
		 * @param getMessageContent    Whether to return content or not.
		 * @return A {@link CompositeDataSupport} object. This can be casted to a {@link
		 * javax.management.openmbean.CompositeData}.
		 * @throws OpenDataException
		 */
        public CompositeDataSupport getMessageAsCompositeData(ProtocolType protocolType,
                                                              AndesMessageMetadata andesMessageMetadata,
                                                              boolean getMessageContent)
                                                                            throws OpenDataException, MBeanException {
            setMessageCompositeType();

            return new CompositeDataSupport(messageCompositeType, MESSAGE_COMPOSITE_ITEM_NAMES.toArray(
                    new String[MESSAGE_COMPOSITE_ITEM_NAMES.size()]),
                    getMessageItemValue(protocolType, andesMessageMetadata, getMessageContent));
        }

		/**
		 * Gets an object array that includes the properties of the message along with the pending message count.
		 *
		 * @param protocolType         The protocol for the message.
		 * @param andesMessageMetadata The {@link AndesMessageMetadata} object representing the message.
		 * @param getMessageContent    Whether to return content or not.
		 * @return An object array with the properties of the message.
		 */
		private Object[] getMessageItemValue(ProtocolType protocolType,
                                             AndesMessageMetadata andesMessageMetadata,
                                             boolean getMessageContent) throws MBeanException {
            if (ProtocolType.AMQP == protocolType) {
                getJMSMessageItemValues(protocolType, andesMessageMetadata, getMessageContent);
            } else if (ProtocolType.MQTT == protocolType){
                throw new NotImplementedException();
            }
            return new Object[0];
        }

		/**
		 * Initializes the composite type for a message to support Composite Data.
		 *
		 * @throws OpenDataException
		 */
        private void setMessageCompositeType() throws OpenDataException {
            messageAttributeTypes[0] = SimpleType.LONG; // Andes Metadata Message ID
            messageAttributeTypes[1] = SimpleType.STRING; // Destination
	        messageAttributeTypes[2] = SimpleType.STRING; // Destination
            messageAttributeTypes[3] = new ArrayType(2, SimpleType.STRING); // Message Properties as 2-D array
            messageAttributeTypes[4] = new ArrayType(1, SimpleType.STRING); // Message Content
            messageCompositeType = new CompositeType("Message", "Message details",
                    MESSAGE_COMPOSITE_ITEM_NAMES.toArray(new String[MESSAGE_COMPOSITE_ITEM_NAMES
                            .size()]), MESSAGE_COMPOSITE_ITEM_NAMES.toArray(new
                    String[MESSAGE_COMPOSITE_ITEM_NAMES.size()]), messageAttributeTypes);
        }

		/**
		 * Gets an object array that includes the properties of the <strong>JMS</strong> message along with the pending
		 * message count.
		 *
		 * @param protocol             The protocol for the message.
		 * @param andesMessageMetadata The {@link AndesMessageMetadata} object representing the JMS message.
		 * @param getMessageContent    Whether to return content or not.
		 * @return An object array with the properties of the JMS message.
		 */
		private Object[] getJMSMessageItemValues(ProtocolType protocol, AndesMessageMetadata andesMessageMetadata,
                                                 boolean getMessageContent) throws MBeanException {
            try {

                Object[] itemValues = null;

                //get AndesMessageMetadata id
                long andesMessageMetadataId = andesMessageMetadata.getMessageID();
                //get destination
                String destination = andesMessageMetadata.getDestination();
	            //get protocol type
	            String protocolAsString = protocol.name();

                Map<String, String> properties = new HashMap<>();
                //get AMQMessage from AndesMessageMetadata
                AMQMessage amqMessage = AMQPUtils.getAMQMessageFromAndesMetaData(andesMessageMetadata);
                //header amqMessageProperties from AMQMessage
                BasicContentHeaderProperties amqMessageProperties =
                                    (BasicContentHeaderProperties) amqMessage.getContentHeaderBody().getProperties();
                //get custom header amqMessageProperties of AMQMessage
                for (String headerKey : amqMessageProperties.getHeaders().keys()) {
                    properties.put(headerKey, amqMessageProperties.getHeaders().get(headerKey).toString());
                }
                properties.put("ContentType", amqMessageProperties.getContentTypeAsString());
                properties.put("MessageID", amqMessageProperties.getMessageIdAsString());
                properties.put("Redelivered", Boolean.FALSE.toString());
                properties.put("Timestamp", Long.toString(amqMessageProperties.getTimestamp()));

                // Converting map to array
                String[][] propertiesArray = new String[properties.size()][2];
                int count = 0;
                for(Map.Entry<String,String> entry : properties.entrySet()){
                    propertiesArray[count][0] = entry.getKey();
                    propertiesArray[count][1] = entry.getValue();
                    count++;
                }

                // Getting message content
                if (!getMessageContent) {
                    itemValues = new Object[]{andesMessageMetadataId, destination, propertiesArray, ""};
                } else {
                    //content is constructing
                    final int bodySize = (int) amqMessage.getSize();

                    AndesMessagePart constructedContent = constructContent(bodySize, amqMessage);
                    byte[] messageContent = constructedContent.getData();
                    int position = constructedContent.getOffset();

                    //if position did not proceed, there is an error receiving content. If not, decode content
                    if (!((bodySize != 0) && (position == 0))) {

                        String[] content = decodeContent(amqMessage, messageContent);

                        //set content type of message to readable name
                        properties.put("ContentType",
                                getReadableNameForMessageContentType(amqMessageProperties.getContentTypeAsString()));

                        //set CompositeData of message
                        itemValues = new Object[]{andesMessageMetadataId, destination, protocolAsString,
                                                  propertiesArray, content};

                    } else if (bodySize == 0) { //empty message
                        itemValues = new Object[]{andesMessageMetadataId, destination, protocolAsString,
                                                  propertiesArray, ""};

                    }
                }

                return itemValues;
            } catch (AMQException exception) {
                throw new MBeanException(exception, "Error occurred in browse queue.");
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
                LongArrayList messageToFetch = new LongArrayList();
                Long messageID = amqMessage.getMessageId();
                messageToFetch.add(messageID);

                try {
                    LongObjectHashMap<List<AndesMessagePart>> contentListMap = MessagingEngine.getInstance()
                            .getContent(messageToFetch);
                    List<AndesMessagePart> contentList = contentListMap.get(messageID);

                    andesMessagePart = lz4CompressionHelper.getDecompressedMessage(contentList, bodySize);

                } catch (AndesException e) {
                    throw new MBeanException(e, "Error occurred while construct the message content. Message ID:"
                                                + amqMessage.getMessageId());
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

                    //The limit is setting to the current position and then the position of the buffer is setting to
                    // zero
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
                String summaryMsg;
                String wholeMsg = "";

                //get TextMessage content to display
                switch (mimeType) {
                    case MIME_TYPE_TEXT_PLAIN:
                    case MIMI_TYPE_TEXT_XML:
                        wholeMsg = extractTextMessageContent(wrapMsgContent, encoding);
                        //get ByteMessage content to display
                        break;
                    case MIME_TYPE_APPLICATION_OCTET_STREAM:
                        wholeMsg = extractByteMessageContent(wrapMsgContent, messageContent);
                        //get ObjectMessage content to display
                        break;
                    case MIME_TYPE_APPLICATION_JAVA_OBJECT_STREAM:
                        wholeMsg = "This Operation is Not Supported!";
                        //get StreamMessage content to display
                        break;
                    case MIME_TYPE_JMS_STREAM_MESSAGE:
                        wholeMsg = extractStreamMessageContent(wrapMsgContent, encoding);
                        //get MapMessage content to display
                        break;
                    case MIME_TYPE_AMQP_MAP:
                    case MIME_TYPE_JMS_MAP_MESSAGE:
                        wholeMsg = extractMapMessageContent(wrapMsgContent);
                        break;
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
            } catch (CharacterCodingException exception) {
                throw new MBeanException(exception, "Error occurred in browse queue.");
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
                switch (contentType) {
                    case MIME_TYPE_TEXT_PLAIN:
                    case MIMI_TYPE_TEXT_XML:
                        contentType = "Text";
                        break;
                    case MIME_TYPE_APPLICATION_JAVA_OBJECT_STREAM:
                        contentType = "Object";
                        break;
                    case MIME_TYPE_AMQP_MAP:
                    case MIME_TYPE_JMS_MAP_MESSAGE:
                        contentType = "Map";
                        break;
                    case MIME_TYPE_JMS_STREAM_MESSAGE:
                        contentType = "Stream";
                        break;
                    case MIME_TYPE_APPLICATION_OCTET_STREAM:
                        contentType = "Byte";
                        break;
                }
            }
            return contentType;
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
                wholeMsg = StringEscapeUtils.escapeHtml(messageContentBuilder.append(mapName).append(": ")
                        .append(mapVal).append(", ").toString()).trim();
            }
            return wholeMsg;
        }

        /**
         * Extract StreamMessage from ByteBuffer
         *
         * @param wrapMsgContent ByteBuffer which contains data
         * @param encoding message encoding
         * @return extracted content as text
         * @throws CharacterCodingException
         */
        private String extractStreamMessageContent(ByteBuffer wrapMsgContent, String encoding)
                                                                                    throws CharacterCodingException {
            String wholeMsg;
            boolean eofReached = false;
            StringBuilder messageContentBuilder = new StringBuilder();

            while (!eofReached) {

                try {
                    Object obj = readObject(wrapMsgContent, encoding);
                    // obj could be null if the wire type is AbstractBytesTypedMessage.NULL_STRING_TYPE
                    if (null != obj) {
                        messageContentBuilder.append(obj.toString()).append(", ");
                    }
                } catch (JMSException e) {
                    eofReached = true;
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
                         byteMsgContent.length : wrapMsgContent.remaining());
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
         * @param encoding message encoding
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
         * @param encoding message encoding
         * @return Object extracted from ByteBuffer
         * @throws JMSException
         * @throws CharacterCodingException
         */
        private Object readObject(ByteBuffer wrapMsgContent, String encoding) throws JMSException,
                                                                                            CharacterCodingException {
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
         * Check that there is at least a certain number of bytes available to read
         *
         * @param length the number of bytes
         * @throws javax.jms.MessageEOFException if there are less than len bytes available to read
         */
        private void checkAvailable(int length, ByteBuffer byteBuffer) throws MessageEOFException {
            if (byteBuffer.remaining() < length) {
                throw new MessageEOFException("Unable to read " + length + " bytes");
            }
        }

        /**
         * Read byte[] array object and return length
         *
         * @param wrapMsgContent ByteBuffer which contains data
         * @param bytes byte[] object
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
    }
}
