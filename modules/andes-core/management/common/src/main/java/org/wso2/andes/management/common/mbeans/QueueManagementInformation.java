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

public interface QueueManagementInformation {

    static final String TYPE = "QueueManagementInformation";

    @MBeanAttribute(name="Queues",description = "All queue names")
    String[] getAllQueueNames();

    @MBeanAttribute(name="Delete Queue" ,description = "Deleting the specified queue from the server")
    void deleteQueue(@MBeanOperationParameter(name = "queueName" ,
            description = "Name of the queue to be deleted") String queueName);

    @MBeanAttribute(name="MessageCount",description = "Message count of the queue")
    long getMessageCount(String queueName,String msgPattern);

    @MBeanAttribute(name="SubscriptionCount", description = "Number of subscriptions for the queue")
    int getSubscriptionCount(String queueName);

    @MBeanAttribute(name = " Queue Exists", description = "Check whether the queue exists in the server")
    boolean isQueueExists(@MBeanOperationParameter(name = "queueName" ,
            description = "Name of the queue to be checked") String queueName);

    @MBeanAttribute(name = " Delete All Messages In Queue ", description = "Delete all the messages in the queue without removing queue bindings.")
    void deleteAllMessagesInQueue(@MBeanOperationParameter(name = "queueName" ,
            description = "Name of the queue to delete messages from") String queueName);

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
