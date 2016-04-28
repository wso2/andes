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

package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;

import javax.management.MBeanException;
import javax.management.MBeanOperationInfo;
import javax.management.openmbean.CompositeData;

/**
 * Interface for managing messages.
 */
public interface MessageManagementInformation {
    String TYPE = "MessageManagementInformation";

	/**
	 * Browse message of a destination using message ID.
	 * <p>
	 * To browse messages without message ID,
	 * use {@link MessageManagementInformation#browseDestinationWithOffset(String, String, String, boolean, int, int)}.
	 *
	 *
	 * @param protocolTypeAsString    The protocol type matching for the message. Example : amqp, mqtt.
	 * @param destinationTypeAsString The destination type matching for the message. Example : queue, topic,
	 *                                durable_topic.
	 * @param destinationName         The name of the destination
	 * @param content                 Whether to return message content or not.
	 * @param nextMessageID           The starting message ID to return from.
	 * @param limit                   The number of messages to return.
	 * @return An array of {@link CompositeData} representing a collection messages.
	 * @throws MBeanException
	 * @see
	 */
	@MBeanAttribute(name = "Browse Queue", description = "Browse messages of given queue using message IDs")
    CompositeData[] browseDestinationWithMessageID(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination to browse") String destinationName,
    @MBeanOperationParameter(name = "content", description = "Get content of message") boolean content,
    @MBeanOperationParameter(name = "nextMessageID", description = "Starting message ID") long nextMessageID,
    @MBeanOperationParameter(name = "limit", description = "Maximum message count per request") int limit)
            throws MBeanException;

	/**
	 * Browse message of a destination. Please note this is time costly.
	 * <p>
	 * To browse messages with message ID, use {@link MessageManagementInformation#browseDestinationWithMessageID
	 * (String,
	 * String, String, boolean, long, int)}.
	 *
	 * @param protocolTypeAsString    The protocol type matching for the message. Example : amqp, mqtt.
	 * @param destinationTypeAsString The destination type matching for the message. Example : queue, topic,
	 *                                durable_topic.
	 * @param destinationName         The name of the destination
	 * @param content                 Whether to return message content or not.
	 * @param offset                  Starting index of the messages to return.
	 * @param limit                   The number of messages to return.
	 * @return An array of {@link CompositeData} representing a collection messages.
	 * @throws MBeanException
	 * @see
	 */
	@MBeanAttribute(name = "Browse Queue", description = "Browse messages of given queue")
    CompositeData[] browseDestinationWithOffset(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination to browse") String destinationName,
    @MBeanOperationParameter(name = "content", description = "Get content of message") boolean content,
    @MBeanOperationParameter(name = "offset", description = "Offset for messages") int offset,
    @MBeanOperationParameter(name = "limit", description = "Maximum message count per request") int limit)
            throws MBeanException;

	/**
	 * Gets a message by message ID belonging to a particular protocol, destination type and destination name.
	 *
	 * @param protocolTypeAsString    The protocol type matching for the message. Example : amqp, mqtt.
	 * @param destinationTypeAsString The destination type matching for the message. Example : queue, topic,
	 *                                durable_topic.
	 * @param destinationName         The name of the destination to which the message belongs to.
	 * @param andesMessageID          The message ID. This message is the andes metadata message ID.
	 * @param content                 Whether to return content or not.
	 * @return A {@link CompositeData} representing a message.
	 * @throws MBeanException
	 */
	@MBeanAttribute(name = "Message", description = "Gets a message for a specific protocol, destination type and " +
                                                    "destination name")
    CompositeData getMessage(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName,
    @MBeanOperationParameter(name = "andesMessageID", description = "Andes Metadata Message ID") long andesMessageID,
    @MBeanOperationParameter(name = "content", description = "Get content of message") boolean content)
		    throws MBeanException;

	/**
	 * Purge all messages belonging to a destination.
	 *
	 * @param protocolTypeAsString    The protocol type matching for the message. Example : amqp, mqtt.
	 * @param destinationTypeAsString The destination type matching for the message. Example : queue, topic,
	 *                                durable_topic.
	 * @param destinationName         The name of the destination to purge messages.
	 * @throws MBeanException
	 */
	@MBeanOperation(name = "deleteMessages", description = "Deletes messages for a specific protocol and destination " +
                                                           "type", impact = MBeanOperationInfo.ACTION)
    void deleteMessages(
    @MBeanOperationParameter(name = "protocol", description = "Protocol") String protocolTypeAsString,
    @MBeanOperationParameter(name = "destinationType", description = "Destination Type") String destinationTypeAsString,
    @MBeanOperationParameter(name = "destinationName", description = "Destination Name") String destinationName)
		    throws MBeanException;
}
