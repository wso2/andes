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

package org.wso2.andes.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;
import org.wso2.carbon.andes.core.AndesException;
import org.wso2.carbon.andes.core.AndesMessageMetadata;
import org.wso2.carbon.andes.core.MessagingEngine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * From JMS Spec
 * -----------------
 *
 * A client uses a QueueBrowser to look at messages on a destination without removing
 * them.
 * The browse methods return a java.util.Enumeration that is used to scan the
 * destination’s messages. It may be an enumeration of the entire content of a destination or
 * it may only contain the messages matching a message selector.
 * Messages may be arriving and expiring while the scan is done. JMS does not
 * require the content of an enumeration to be a static snapshot of destination content.
 * Whether these changes are visible or not depends on the JMS provider.
 * 
 * When someone made a QueueBroswer Subscription, we read messages for that destination and
 * send them to that subscription. 
 */

public class QueueBrowserDeliveryWorker {

    private Subscription subscription;
    private AMQQueue queue;
    private AMQProtocolSession session;

    private static Log log = LogFactory.getLog(QueueBrowserDeliveryWorker.class);

    public QueueBrowserDeliveryWorker(Subscription subscription, AMQQueue queue,
                                      AMQProtocolSession session) {
        this.subscription = subscription;
        this.queue = queue;
        this.session = session;
    }


    public void send() {
        List<QueueEntry> messages;
        try {
            messages = getSortedMessages();
            sendMessagesToClient(messages);
        } catch (AndesException e) {
            log.error("Error while sending message for Browser subscription", e);
        } finally {
            // It is essential to confirm auto close , since in the client side it waits to know the end of the messages
            subscription.confirmAutoClose();
        }
    }

    /**
     * Sends the browser subscription's messages to client
     * @param messages - matching messages of destination
     */
    private void sendMessagesToClient(List<QueueEntry> messages){
            if (messages.size() > 0) {
                for (QueueEntry message : messages) {
                    try {
                        if (subscription instanceof SubscriptionImpl.BrowserSubscription) {
                            subscription.send(message);
                        }

                    } catch (Exception e) {
                        log.error("Unexpected Error in Message Flusher Task " +
                                "while delivering the message : ", e);
                    }
                }
            }
    }

    /**
     * Get message destination entries sorted using the message Id in the ascending order.
     *
     * @return Sorted QueueEntry List
     * @throws AndesException
     */
    private List<QueueEntry> getSortedMessages() throws AndesException {

        String queueName = queue.getResourceName();
        long lastAssignedSlotMessageId = MessagingEngine.getInstance().getLastAssignedSlotMessageId(queueName);
        long messageIdDifference = 1024 * 256 * 5000;
        long lastReadMessageId = lastAssignedSlotMessageId - messageIdDifference;
        int countOfQueue = (int) MessagingEngine.getInstance().getMessageCountOfQueue(queueName);

        List<AndesMessageMetadata> queueMessageMetaData = new ArrayList<AndesMessageMetadata>();
        List<AndesMessageMetadata> currentlyReadMessageMetaData = MessagingEngine.getInstance()
                .getNextNMessageMetadataFromQueue(queueName, lastReadMessageId, countOfQueue);

        queueMessageMetaData.addAll(currentlyReadMessageMetaData);

        CustomComparator orderComparator = new CustomComparator();
        Collections.sort(queueMessageMetaData, orderComparator);
        //todo: hasitha - what abt setting client identifier (it is skipped)?
        List<AMQMessage>  AMQMessages  =  AMQPUtils.getEntryAMQMessageListFromAndesMetaDataList(queueMessageMetaData);
        List<QueueEntry> queueEntries = new ArrayList<QueueEntry>();
        for(AMQMessage message : AMQMessages) {
            queueEntries.add(AMQPUtils.convertAMQMessageToQueueEntry(message, queue));
        }
        return queueEntries;
    }

    public class CustomComparator implements Comparator<AndesMessageMetadata>{

        public int compare(AndesMessageMetadata message1, AndesMessageMetadata message2) {
            return (int) (message1.getMessageId()-message2.getMessageId());
        }
    }

}
