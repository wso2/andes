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

/**
 * This interface contains all operations invoked by the UI console with relation to queues. (addition, deletion, purging, etc.)
 */
public interface QueueManagementInformation {

    static final String TYPE = "QueueManagementInformation";

    /***
     * Retrieve all destination queue names.
     * @return List of queue names.
     */
    @MBeanAttribute(name="Queues",description = "All queue names")
    String[] getAllQueueNames();

    /***
     * Retrieve current message count of a queue. This may be only a rough estimate in a fast pub/sub scenario.
     * @param queueName name of queue
     * @param msgPattern The exchange type used to transfer messages with the given queueName. e.g. "queue" or "topic"
     * @return Count of messages in store for the given queue.
     */
    @MBeanAttribute(name="MessageCount",description = "Message count of the queue")
    long getMessageCount(String queueName,String msgPattern);

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
     *
     * @param messageIDs          The browser message Ids
     * @param deadLetterQueueName The Dead Letter Queue Name for the tenant
     */
    @MBeanAttribute(name = " DeleteMessages In Dead Letter Queue ", description = "Will Delete Messages From Dead Letter Queue")
    void deleteMessagesFromDeadLetterQueue(@MBeanOperationParameter(name = "messageIDs",
            description = "ID of the Messages to Be Deleted") String[] messageIDs, @MBeanOperationParameter(name = "deadLetterQueueName",
            description = "The Dead Letter Queue Name for the selected tenant") String deadLetterQueueName);

    /**
     * Restore a given browser message Id list from the Dead Letter Queue to the same queue it was previous in before moving to the Dead Letter Queue
     * and remove them from the Dead Letter Queue.
     *
     * @param messageIDs          The browser message Ids
     * @param deadLetterQueueName The Dead Letter Queue Name for the tenant
     */
    @MBeanAttribute(name = " Restore Back a Specific set of Messages ", description = "Will Restore a Specific Set of Messages Back to Its Original Queue")
    void restoreMessagesFromDeadLetterQueue(@MBeanOperationParameter(name = "messageIDs",
            description = "IDs of the Messages to Be Restored") String[] messageIDs, @MBeanOperationParameter(name = "deadLetterQueueName",
            description = "The Dead Letter Queue Name for the selected tenant") String deadLetterQueueName);

    /**
     * Restore a given browser message Id list from the Dead Letter Queue to a different given queue in the same tenant and remove them from the Dead Letter Queue.
     *
     * @param messageIDs          The browser message Ids
     * @param destination         The new destination
     * @param deadLetterQueueName The Dead Letter Queue Name for the tenant
     */
    @MBeanAttribute(name = " Restore Back a Specific set of Messages ", description = "Will Restore a Specific Set of Messages Back to a Queue differnt from the original")
    void restoreMessagesFromDeadLetterQueue(@MBeanOperationParameter(name = "messageIDs",
            description = "IDs of the Messages to Be Restored") String[] messageIDs,@MBeanOperationParameter(name = "destination",
            description = "Destination of the message to be restored") String destination, @MBeanOperationParameter(name = "deadLetterQueueName",
            description = "The Dead Letter Queue Name for the selected tenant") String deadLetterQueueName);

}
