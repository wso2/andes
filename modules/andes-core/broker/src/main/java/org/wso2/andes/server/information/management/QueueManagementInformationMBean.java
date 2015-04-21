/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.andes.server.information.management;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.distruptor.inbound.InboundQueueEvent;
import org.wso2.andes.management.common.mbeans.QueueManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostImpl;

import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueueManagementInformationMBean extends AMQManagedObject implements QueueManagementInformation {

    private static Log log = LogFactory.getLog(QueueManagementInformationMBean.class);

    private final QueueRegistry queueRegistry;

    private final String PURGE_QUEUE_ERROR = "Error in purging queue : ";

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

    /**
     * AndesChannel for this dead letter channel restore which implements flow control.
      */
    AndesChannel andesChannel = Andes.getInstance().createChannel(new FlowControlListener() {
        @Override
        public void block() {
            restoreBlockedByFlowControl = true;
        }

        @Override
        public void unblock() {
            restoreBlockedByFlowControl = false;
        }
    });

    /***
     * Virtual host information are needed in the constructor to evaluate user permissions for
     * queue management actions.(e.g. purge)
     * @param vHostMBean Used to access the virtual host information
     * @throws NotCompliantMBeanException
     */
    public QueueManagementInformationMBean(VirtualHostImpl.VirtualHostMBean vHostMBean) throws NotCompliantMBeanException {
        super(QueueManagementInformation.class, QueueManagementInformation.TYPE);

        VirtualHost virtualHost = vHostMBean.getVirtualHost();

        queueRegistry = virtualHost.getQueueRegistry();
        disablePubAck = new DisablePubAckImpl();
    }

    public String getObjectInstanceName() {
        return QueueManagementInformation.TYPE;
    }

    /***
     * {@inheritDoc}
     * @return
     */
    public synchronized String[] getAllQueueNames() {

        try {
            List<String> queuesList = AndesContext.getInstance().getAMQPConstructStore().getQueueNames();
            Iterator itr = queuesList.iterator();
            //remove topic specific queues
            while (itr.hasNext()) {
                String destinationQueueName = (String) itr.next();
                if(destinationQueueName.startsWith("tmp_") || destinationQueueName.contains
                        ("carbon:") || destinationQueueName.startsWith("TempQueue")) {
                    itr.remove();
                }
            }
            String[] queues= new String[queuesList.size()];
            queuesList.toArray(queues);
            return queues;

        } catch (Exception e) {
          throw new RuntimeException("Error in accessing destination queues",e);
        }

    }

    /***
     * {@inheritDoc}
     * @return
     */
    public boolean isQueueExists(String queueName) {
        try {
            List<String> queuesList = AndesContext.getInstance().getAMQPConstructStore().getQueueNames();
            return queuesList.contains(queueName);
        } catch (Exception e) {
          throw new RuntimeException("Error in accessing destination queues",e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllMessagesInQueue(@MBeanOperationParameter(name = "queueName",
            description = "Name of the queue to delete messages from") String queueName,
                                         @MBeanOperationParameter(name = "ownerName",
                                                 description = "Username of user that calls for " +
                                                         "purge") String ownerName) throws
            MBeanException {

        AMQQueue queue = queueRegistry.getQueue(new AMQShortString(queueName));

        try {
            if (queue == null) {
                throw new JMException("The Queue " + queueName + " is not a registered queue.");
            }

            queue.purge(0l); //This is to trigger the AMQChannel purge event so that the queue
            // state of qpid is updated. This method also validates the request owner and throws
            // an exception if permission is denied.

            InboundQueueEvent andesQueue = AMQPUtils.createAndesQueue(queue);
            int purgedMessageCount = Andes.getInstance().purgeQueue(andesQueue, false);
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
        } catch (AMQException amqex) {
            throw new MBeanException(amqex, PURGE_QUEUE_ERROR + queueName);
        } catch (AndesException e) {
            throw new MBeanException(e, PURGE_QUEUE_ERROR + queueName);
        }
    }

    /**
     * Delete a selected message list from a given Dead Letter Queue of a tenant.
     *
     * @param messageIDs          The browser message Ids
     * @param deadLetterQueueName The Dead Letter Queue Name for the tenant
     */
    @Override
    public void deleteMessagesFromDeadLetterQueue(@MBeanOperationParameter(name = "messageIDs",
            description = "ID of the Messages to Be DELETED") String[] messageIDs,
                                                  @MBeanOperationParameter(name = "deadLetterQueueName",
            description = "The Dead Letter Queue Name for the selected tenant") String deadLetterQueueName) {
        List<Long> andesMessageIdList = getValidAndesMessageIdList(messageIDs);
        List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>(messageIDs.length);
        for (Long messageId : andesMessageIdList) {
            removableMetadataList.add(new AndesRemovableMetadata(messageId, deadLetterQueueName, deadLetterQueueName));
        }
        try {
            Andes.getInstance().deleteMessages(removableMetadataList, false);
        } catch (AndesException e) {
            throw new RuntimeException("Error deleting messages from Dead Letter Channel", e);
        }

        AndesUtils.unregisterBrowserMessageIds(messageIDs);

    }

    /**
     * Restore a given browser message Id list from the Dead Letter Queue to the same queue it was previous in before
     * moving to the Dead Letter Queue
     * and remove them from the Dead Letter Queue.
     *
     * @param messageIDs          The browser message Ids
     * @param deadLetterQueueName The Dead Letter Queue Name for the tenant
     */
    @Override
    public void restoreMessagesFromDeadLetterQueue(@MBeanOperationParameter(name = "messageIDs",
            description = "IDs of the Messages to Be Restored") String[] messageIDs,
                                                   @MBeanOperationParameter(name = "deadLetterQueueName",
            description = "The Dead Letter Queue Name for the selected tenant") String deadLetterQueueName) {
        List<Long> andesMessageIdList = getValidAndesMessageIdList(messageIDs);
        List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>(andesMessageIdList.size());

        try {
            Map<Long, List<AndesMessagePart>> messageContent = MessagingEngine.getInstance().getContent
                    (andesMessageIdList);

            boolean interruptedByFlowControl = false;

            for (Long messageId : andesMessageIdList) {
                if (restoreBlockedByFlowControl) {
                    interruptedByFlowControl = true;
                    break;
                }
                List<AndesMessageMetadata> messageMetadataListForOne = MessagingEngine.getInstance().getMetaDataList
                        (deadLetterQueueName, messageId, messageId);
                AndesMessageMetadata metadata = messageMetadataListForOne.get(0);
                String destination = metadata.getDestination();

                // Create a removable metadata to remove the current message
                removableMetadataList.add(new AndesRemovableMetadata(messageId, deadLetterQueueName,
                        deadLetterQueueName));

                // Set the new destination queue
                metadata.setDestination(destination);
                metadata.setStorageQueueName(AndesUtils.getStorageQueueForDestination(destination,
                        ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID(), false));
                AndesMessage andesMessage = new AndesMessage(metadata);

                // Update Andes message with all the chunk details
                List<AndesMessagePart> messageParts = messageContent.get(messageId);
                for (AndesMessagePart messagePart : messageParts) {
                    andesMessage.addMessagePart(messagePart);
                }

                // Handover message to Andes
                Andes.getInstance().messageReceived(andesMessage, andesChannel, disablePubAck);
            }

            // Delete old messages
            Andes.getInstance().deleteMessages(removableMetadataList, false);

            if (interruptedByFlowControl) {
                // Throw this out so UI will show this to the user as an error message.
                throw new RuntimeException("Message restore from dead letter queue has been interrupted by flow " +
                        "control. Please try again later.");
            }

        } catch (AndesException e) {
            throw new RuntimeException("Error restoring messages from " + deadLetterQueueName, e);
        }

        AndesUtils.unregisterBrowserMessageIds(messageIDs);
    }

    /**
     * Restore a given browser message Id list from the Dead Letter Queue to a different given queue in the same
     * tenant and remove them from the Dead Letter Queue.
     *
     * @param messageIDs          The browser message Ids
     * @param destination         The new destination
     * @param deadLetterQueueName The Dead Letter Queue Name for the tenant
     */
    @Override
    public void restoreMessagesFromDeadLetterQueue(@MBeanOperationParameter(name = "messageIDs",
            description = "IDs of the Messages to Be Restored") String[] messageIDs,
                                                   @MBeanOperationParameter(name = "destination",
            description = "Destination of the message to be restored") String destination,
                                                   @MBeanOperationParameter(name = "deadLetterQueueName",
            description = "The Dead Letter Queue Name for the selected tenant") String deadLetterQueueName) {
        List<Long> andesMessageIdList = getValidAndesMessageIdList(messageIDs);
        List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>(andesMessageIdList.size());

        try {
            Map<Long, List<AndesMessagePart>> messageContent = MessagingEngine.getInstance().getContent
                    (andesMessageIdList);

            boolean interruptedByFlowControl = false;

            for (Long messageId : andesMessageIdList) {
                if (restoreBlockedByFlowControl) {
                    interruptedByFlowControl = true;
                    break;
                }

                List<AndesMessageMetadata> messageMetadataListForOne = MessagingEngine.getInstance().getMetaDataList
                        (deadLetterQueueName, messageId, messageId);
                AndesMessageMetadata metadata = messageMetadataListForOne.get(0);

                // Create a removable metadata to remove the current message
                removableMetadataList.add(new AndesRemovableMetadata(messageId, deadLetterQueueName,
                        deadLetterQueueName));

                // Set the new destination queue
                metadata.setDestination(destination);
                metadata.setStorageQueueName(AndesUtils.getStorageQueueForDestination(destination,
                        ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID(), false));
                metadata.updateMetadata(destination, "amqp.direct");
                AndesMessage andesMessage = new AndesMessage(metadata);

                // Update Andes message with all the chunk details
                List<AndesMessagePart> messageParts = messageContent.get(messageId);
                for (AndesMessagePart messagePart : messageParts) {
                    andesMessage.addMessagePart(messagePart);
                }

                // Handover message to Andes
                Andes.getInstance().messageReceived(andesMessage, andesChannel, disablePubAck);
            }

            // Delete old messages
            Andes.getInstance().deleteMessages(removableMetadataList, false);

            if (interruptedByFlowControl) {
                // Throw this out so UI will show this to the user as an error message.
                throw new RuntimeException("Message restore from dead letter queue has been interrupted by flow " +
                        "control. Please try again later.");
            }

        } catch (AndesException e) {
            throw new RuntimeException("Error restoring messages from " + deadLetterQueueName, e);
        }

        AndesUtils.unregisterBrowserMessageIds(messageIDs);
    }

    /**
     * Retrieve a valid andes messageId list from a given browser message Id list.
     *
     * @param browserMessageIdList List of browser messageIds.
     * @return Valid Andes MessageId list
     */
    private List<Long> getValidAndesMessageIdList(String[] browserMessageIdList) {
        List<Long> andesMessageIdList = new ArrayList<Long>(browserMessageIdList.length);

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
     *
     * So from now on , we ll not provide minus values to the front end since
     * it is not acceptable
     *
     * */
    public long getMessageCount(String queueName,String msgPattern) {

/*        int messageCount = (int) messageStore.getCassandraMessageCountForQueue(queueName);
        if (messageCount < 0) {
            messageStore.incrementMessageCountForQueue(queueName, Math.abs(messageCount));
            messageCount = 0;
        }
        return messageCount;*/
        if (log.isDebugEnabled()) {
            log.debug("Counting at queue : " + queueName);
        }

        long messageCount = 0;
        try {
        if (msgPattern.equals("queue")) {
            messageCount = MessagingEngine.getInstance().getMessageCountOfQueue(queueName);
        } else if (msgPattern.equals("topic")) {
            //TODO: hasitha - to implement
        }

        } catch (AndesException e) {
            throw new RuntimeException("Error retrieving message count for the queue : " + queueName, e);
        }

        return messageCount;
    }

    /***
     * {@inheritDoc}
     */
    public int getSubscriptionCount( String queueName){
        try {
            return AndesContext.getInstance().getSubscriptionStore().numberOfSubscriptionsInCluster(queueName, false,
                    AndesSubscription.SubscriptionType.AMQP);
        } catch (Exception e) {
            throw new RuntimeException("Error in getting subscriber count",e);
        }
    }

}
