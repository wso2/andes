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
package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is for reading and flushing messages for a browser subscription. From JMS Spec
 * -----------------
 * <p>
 * A client uses a QueueBrowser to look at messages on a destination without removing
 * them.
 * The browse methods return a java.util.Enumeration that is used to scan the
 * destination's messages. It may be an enumeration of the entire content of a destination or
 * it may only contain the messages matching a message selector.
 * Messages may be arriving and expiring while the scan is done. JMS does not
 * require the content of an enumeration to be a static snapshot of destination content.
 * Whether these changes are visible or not depends on the JMS provider.
 * <p>
 * When someone made a QueueBrowser Subscription, we read messages for that destination and
 * send them to that subscription.
 */
public class QueueBrowserMessageFlusher {

    /**
     * AMQP transport level browser subscription
     */
    private Subscription subscription;

    /**
     * AMQP transport level queue messages are being browsed
     */
    private AMQQueue queue;

    private static Log log = LogFactory.getLog(QueueBrowserMessageFlusher.class);

    /**
     * Constructor for QueueBrowserMessageFlusher. This will be created once a browser subscription
     * is created from AMQP transport
     *
     * @param subscription AMQP browser subscription object
     * @param queue        queue specified at transport
     */
    public QueueBrowserMessageFlusher(Subscription subscription, AMQQueue queue) {
        this.subscription = subscription;
        this.queue = queue;
    }


    /**
     * Read messages from store and deliver to the underlying browser subscription.
     * This will read all messages of queue specified
     *
     * @throws AndesException on an issue reading messages from durable store
     */
    public void readAndSendFromMessageStore() throws AndesException {
        long startMessageIdOfBatch = 0;
        long lastMessageIdOfBatch = 0;
        int messageCountToRead = 10000000;
        int errorCount = 0;
        long messageCount = 0;
        List<QueueEntry> messages;

        try {
            while (true) {
                messages = readMessagesFromStore(startMessageIdOfBatch, messageCountToRead);
                if (messages.isEmpty()) {
                    break;
                }
                for (QueueEntry message : messages) {
                    try {
                        messageCount = messageCount + 1;
                        lastMessageIdOfBatch = message.getMessage().getMessageNumber();
                        subscription.send(message);
                    } catch (AMQException e) {
                        log.error("Error while delivering message id = "
                                + message.getMessage().getMessageNumber()
                                + " to browser subscription id= "
                                + subscription.getSubscriptionID());
                        errorCount = errorCount + 1;
                        if (errorCount > 20) {
                            break;  //no point of trying to send. Something is wrong
                        }
                    }
                }

                if (errorCount > 20) {
                    log.error("Stopping delivery to browser subscription id = " + subscription.getSubscriptionID());
                    break;  //no point of trying to send. Something is wrong
                }

                startMessageIdOfBatch = lastMessageIdOfBatch + 1;
            }
        } finally {
            // It is essential to confirm auto close , since in the client side it waits to know the end of the messages
            subscription.confirmAutoClose();
        }
    }

    /**
     * Get message destination entries sorted using the message Id in the ascending order.
     *
     * @return Sorted QueueEntry List
     * @throws AndesException on an error while reading from message store
     */
    private List<QueueEntry> readMessagesFromStore(long startMessageIdOfBatch, int messageCountToRead)
            throws AndesException {

        String queueName = queue.getResourceName();

        List<AndesMessageMetadata> currentlyReadMessageMetaData = MessagingEngine.getInstance()
                .getNextNMessageMetadataFromQueue(queueName, startMessageIdOfBatch, messageCountToRead);

        List<QueueEntry> queueEntries = new ArrayList<>();

        for (AndesMessageMetadata message : currentlyReadMessageMetaData) {
            AMQMessage amqMessage = AMQPUtils.getAMQMessageFromAndesMetaData(message);
            queueEntries.add(AMQPUtils.convertAMQMessageToQueueEntry(amqMessage, queue));
        }
        return queueEntries;
    }
}
