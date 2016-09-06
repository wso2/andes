/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */

package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import javax.management.MBeanException;
import javax.management.openmbean.CompositeData;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This interface contains all operations invoked by the UI console with relation to queues. (addition, deletion, purging, etc.)
 */
public interface QueueManagementInformation {

    String TYPE = "QueueManagementInformation";
    //CompositeType key/description information for message content
    //For compatibility reasons, DON'T MODIFY the existing key values if expanding the set.
    String JMS_PROPERTIES = "JMSProperties";
    String CONTENT_TYPE = "ContentType";
    String CONTENT = "Content";
    String JMS_MESSAGE_ID = "JMSMessageId";
    String JMS_CORRELATION_ID = "JMSCorrelationId";
    String JMS_TYPE = "JMSType";
    String JMS_REDELIVERED = "JMSRedelivered";
    String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    String JMS_PRIORITY = "JMSPriority";
    String TIME_STAMP = "TimeStamp";
    String JMS_EXPIRATION = "JMSExpiration";
    String MSG_DESTINATION = "MessageDestination";
    String ANDES_MSG_METADATA_ID = "AndesMessageMetadataId";

    List<String> VIEW_MSG_CONTENT_COMPOSITE_ITEM_NAMES_DESC = Collections.unmodifiableList(Arrays.asList(JMS_PROPERTIES,
            CONTENT_TYPE, CONTENT, JMS_MESSAGE_ID, JMS_REDELIVERED,
            TIME_STAMP, MSG_DESTINATION, ANDES_MSG_METADATA_ID));

    /***
     * Retrieve all destination queue names.
     * @return List of queue names.
     */
    @MBeanAttribute(name="Queues",description = "All queue names")
    String[] getAllQueueNames();

    /**
     * Retrieve all queues with message counts
     *
     * @return List of all queues with the messageCounts
     */
    @MBeanAttribute(name = "AllQueueCounts", description = "Message counts of all queues")
    Map<String, Integer> getAllQueueCounts();

    /**
     * Retrieve current message count of a queue. This may be only a rough estimate in a fast pub/sub scenario.
     * @param queueName name of queue
     * @param msgPattern The exchange type used to transfer messages with the given queueName. e.g. "queue" or "topic"
     * @return Count of messages in store for the given queue.
     */
    @MBeanAttribute(name="MessageCount",description = "Message count of the queue")
    long getMessageCount(String queueName,String msgPattern) throws MBeanException;

    /***
     * Retrieve number of subscribers (active/inactive) for a given queue.
     * @param queueName name of queue
     * @return Number of subscriptions listening to the given queue.
     */
    @MBeanAttribute(name="SubscriptionCount", description = "Number of subscriptions for the queue")
    int getSubscriptionCount(String queueName);

    /***
     * Verify whether the given queue exists in broker.
     * @param queueName name of queue
     * @return true if the queue exists in the server.
     */
    @MBeanAttribute(name = " Queue Exists", description = "Check whether the queue exists in the server")
    boolean isQueueExists(@MBeanOperationParameter(name = "queueName" ,
            description = "Name of the queue to be checked") String queueName);

    /**
     * Purge the given queue both in terms of stored messages and in-memory messages.
     * Ideally, all messages not awaiting acknowledgement at the time of purge should be cleared
     * from the broker.
     *
     * @param queueName name of queue
     */
    @MBeanAttribute(name = " Delete All Messages In Queue ", description = "Delete all the " +
            "messages in the queue without removing queue bindings.")
    void deleteAllMessagesInQueue(@MBeanOperationParameter(name = "queueName",
            description = "Name of the queue to delete messages from") String queueName,
                                  @MBeanOperationParameter(name = "ownerName",
                                          description = "Username of user that calls for " +
                                                  "purge")
                                  String ownerName) throws MBeanException;

    /**
     * Delete a selected message list from a given Dead Letter Queue of a tenant.
     * @param andesMetadataIDs          The browser message Ids
     * @param destinationQueueName The Dead Letter Queue Name for the tenant
     */
    @MBeanAttribute(name = " DeleteMessages In Dead Letter Queue ", description = "Will Delete Messages From Dead Letter Queue")
    void deleteMessagesFromDeadLetterQueue(@MBeanOperationParameter(name = "andesMetadataIDs",
            description = "ID of the Messages to Be Deleted") long[] andesMetadataIDs, @MBeanOperationParameter(name = "destinationQueueName",
            description = "The Dead Letter Queue Name for the selected tenant") String destinationQueueName);

    /**
     * Restore a given message id list from the Dead Letter Channel to the same queue it was previously in before
     * moving to the Dead Letter Channel and remove them from the Dead Letter Channel.
     * @param andesMessageIds       The andes message Id list
     * @param destinationQueueName  Original destination queue of these messages.
     */
    @MBeanAttribute(name = "restoreMessagesFromDeadLetterQueue", description = "Will restore a specific set of"
            + " messages back to their original queue")
    void restoreMessagesFromDeadLetterQueue(
            @MBeanOperationParameter(name = "andesMessageIds",
                                     description = "IDs of the Messages to Be restored") long[] andesMessageIds,
            @MBeanOperationParameter(name = "destinationQueueName",
                                     description = "Original destination queue of the messages") String destinationQueueName)
            throws MBeanException;

    /**
     * Reroute a given message Id list from the Dead Letter Channel to a different queue in the same
     * tenant and remove the old messages from the Dead Letter Channel.
     *
     * @param sourceQueue       The Dead Letter Queue Name for the tenant
     * @param andesMessageIds   The browser message Ids
     * @param targetQueue       The new destination
     */
    @MBeanAttribute(name = "restoreMessagesFromDeadLetterQueueWithDifferentDestination", description = "Will reroute a specific set"
            + " of Messages of QueueA in DLC to a new target QueueB")
    void restoreMessagesFromDeadLetterQueueWithDifferentDestination(
            @MBeanOperationParameter(name = "andesMessageIds",
                                     description = "IDs of the Messages to Be Restored") long[] andesMessageIds,
            @MBeanOperationParameter(name = "sourceQueue",
                                     description = "The  original queue name of the messages") String sourceQueue,
            @MBeanOperationParameter(name = "targetQueue",
                                     description = "New destination queue for the messages") String targetQueue)
            throws MBeanException;

    /**
     * Browse queue for given id starting from last message id until it meet max message count
     *
     * @param queueName name of queue
     * @param nextMsgId last browse message id
     * @param maxMsgCount total message count to browse
     * @return list of messages
     */
    @MBeanAttribute(name = " Browse Queue ", description = "Browse messages of given queue")
    CompositeData[] browseQueue(@MBeanOperationParameter(name = "queueName", description = "Name of queue to browse " +
                                                                                           "messages") String queueName,
                                @MBeanOperationParameter(name = "lastMsgId", description = "Browse message this " +
                                                                                           "onwards") long nextMsgId,
                                @MBeanOperationParameter(name = "maxMsgCount", description = "Maximum message count " +
                                                                                             "per request") int
                                        maxMsgCount)
            throws MBeanException;

    /**
     * Retrieve current message count in the DLC for a specific queue.
     *
     * @param queueName name of queue
     * @return Count of messages in store for the given queue.
     */
    @MBeanAttribute(name = "NumberMessagesInDLCForQueue", description = "Message count in the DLC" +
                                                                        " for a specific queue")
    long getNumberOfMessagesInDLCForQueue(
            @MBeanOperationParameter(name = "queueName", description = "Name of queue to browse" +
                                                                       " DLC messages") String queueName)
            throws MBeanException;

    /**
     * Retrieve messages in DLC for a specific queue for given id starting from last message id
     * until it meet max message count.
     *
     * @param queueName       name of queue
     * @param nextMsgId       last browse message id
     * @param maxMessageCount total message count to browse
     * @return list of messages
     */
    @MBeanAttribute(name = "MessageInDLCForQueue", description = "Browse messages of given queue")
    CompositeData[] getMessageInDLCForQueue(
            @MBeanOperationParameter(name = "queueName", description = "Name of queue to browse " +
                                                               "in DLC messages") String queueName,
            @MBeanOperationParameter(name = "lastMsgId", description = "Browse message this " +
                                                                       "onwards") long nextMsgId,
            @MBeanOperationParameter(name = "maxMessageCount", description = "Maximum message " +
                                                         "count per request") int maxMessageCount)
            throws MBeanException;

    /**
     * Returns a paginated list of message metadata which are destined for the targetQueue, but currently living in the
     * Dead Letter Channel.
     *
     * @param targetQueue    Name of the destination queue
     * @param startMessageId Message Id to start the resultset with.
     * @param pageLimit      Maximum message count required in a single response
     * @return Array of {@link CompositeData}
     */
    @MBeanAttribute(name = "getMessageMetadataInDeadLetterChannel",
                    description = "List Message Metadata that live in Dead Letter Channel for a given queue.")
    CompositeData[] getMessageMetadataInDeadLetterChannel(
            @MBeanOperationParameter(name = "targetQueue",
                                     description = "Name of destination queue ") String targetQueue,
            @MBeanOperationParameter(name = "startMessageId",
                                     description = "Message Id to start the resultset with.") long startMessageId,
            @MBeanOperationParameter(name = "pageLimit",
                                     description = "Maximum message count required in a single response") int pageLimit)
            throws MBeanException;

    /**
     * Restore messages destined for the input sourceQueue into a different targetQueue.
     * If the sourceQueue is DLCQueue, all messages in the DLC will be restored to the targetQueue.
     *
     * @param sourceQueue Name of the source queue
     * @param targetQueue Name of the target queue.
     * @param internalBatchSize even with this method, the MB server will internally read messages in DLC in batches,
     *                          and simulate each batch as a new message list to the targetQueue. internalBatchSize
     *                          controls the number of messages processed in a single batch internally.
     * @throws MBeanException if an exception occurs while moving messages from the sourceQueue.
     */
    @MBeanAttribute(name = "rerouteAllMessagesInDeadLetterChannelForQueue", description = "Restore messages destined for "
            + "the input sourceQueue into a different targetQueue.")
    int rerouteAllMessagesInDeadLetterChannelForQueue(
            @MBeanOperationParameter(name = "sourceQueue", description = "Name of the source queue") String sourceQueue,
            @MBeanOperationParameter(name = "targetQueue", description = "Name of the source queue") String targetQueue,
            @MBeanOperationParameter(name = "internalBatchSize", description = "Number of messages processed in a "
                    + "single database call.") int internalBatchSize)
            throws MBeanException;
}
