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
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.subscription.OutboundSubscription;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class holds utility methods for Andes. Commonly
 * accessible methods for the whole broker are piled
 * here
 */
public class AndesUtils {

    private static Log log = LogFactory.getLog(AndesUtils.class);

    //this constant will be used to prefix storage queue name for topics
    public final static String TOPIC_NODE_QUEUE_PREFIX =  "TopicQueue";

    //This will be used to co-relate between the message id used in the browser and the message id used internally in MB
    private static ConcurrentHashMap<String, Long> browserMessageIdCorrelater = new ConcurrentHashMap<String, Long>();

    private  static PrintWriter printWriterGlobal;


    public static String printAMQMessage(QueueEntry message){
        ByteBuffer buf = ByteBuffer.allocate(100); 
        int readCount = message.getMessage().getContent(buf, 0);
        return "("+ message.getMessage().getMessageNumber() + ")" + new String(buf.array(),0, readCount); 
    }

    /**
     * Register a mapping between browser message Id and Andes message Id. This is expected to be invoked
     * whenever messages are passed to the browser via a browser subscription and is expecting a return from browser
     * with browser message Id which needs to be resolved to Andes Message Id.
     *
     * These mappings should be cleaned after they have served their purpose.
     *
     * @param browserMessageId The browser message Id / External message Id
     * @param andesMessageId Respective Andes message Id
     */
    public static synchronized void registerBrowserMessageId(String browserMessageId, long andesMessageId) {
        browserMessageIdCorrelater.put(browserMessageId, andesMessageId);
    }

    /**
     * Remove the register browser message Id - andes message Id mapping. This is expected to be invoked
     * when the relevant mapping is no longer valid or no longer required.
     *
     * @param browserMessageIdList The browser message Id / External message Id list to be cleaned.
     */
    public static synchronized void unregisterBrowserMessageIds(String[] browserMessageIdList) {
        for(String browserMessageId : browserMessageIdList) {
            long andesMessageId = browserMessageIdCorrelater.remove(browserMessageId);

            if(log.isDebugEnabled()) {
                log.debug("Browser message Id " + browserMessageId + " related to Andes message Id " + andesMessageId +
                    " was removed from browserMessageIdCorrecter");
            }
        }
    }

    /**
     * Get the respective Andes message Id for a given browser message Id.
     *
     * @param browserMessageId The browser message Id / External message Id
     * @return Andes message Id
     */
    public static synchronized Long getAndesMessageId(String browserMessageId) {
        Long andesMessageId;
        if (browserMessageIdCorrelater.containsKey(browserMessageId)) {
            andesMessageId = browserMessageIdCorrelater.get(browserMessageId);
        } else {
            andesMessageId =  -1L;
        }
        return andesMessageId;
    }

    public static void writeToFile(String whatToWrite, String filePath) {
        try {
            if(printWriterGlobal == null) {
                BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter(filePath));
                printWriterGlobal = new PrintWriter(bufferedWriter);
            }

            printWriterGlobal.println(whatToWrite);

        } catch (IOException e) {
            System.out.println("Error. File to print received messages is not provided" + e);
        }

    }

    /**
     * Generate storage queue name for a given destination
     * @param destination subscribed routing key
     * @param nodeID id of this node
     * @param isTopic is destination represent a topic
     * @return  storage queue name for destination
     */
    public static String getStorageQueueForDestination(String destination, String nodeID, boolean isTopic) {
        String storageQueueName;
        // We need to add a prefix so that we could differentiate if queue is created under the same name
        //as topic
        if(isTopic) {
            storageQueueName = new StringBuilder(TOPIC_NODE_QUEUE_PREFIX).append("|").append(destination).append("|").append(nodeID).toString();
        } else {
            storageQueueName = destination;
        }
        return storageQueueName;
    }

    public static LocalSubscription createLocalSubscription(OutboundSubscription subscription,
                                                            String subscriptionID, String destination,
                                                            boolean isBoundToTopic, boolean isExclusive,
                                                            boolean isDurable, String subscribedNode,
                                                            long subscribeTime, String targetQueue,
                                                            String targetQueueOwner, String targetQueueBoundExchange,
                                                            String targetQueueBoundExchangeType,
                                                            Short isTargetQueueBoundExchangeAutoDeletable,
                                                            boolean hasExternalSubscriptions) {

        return new LocalSubscription(subscription, subscriptionID, destination, isBoundToTopic, isExclusive,
                isDurable, subscribedNode, subscribeTime, targetQueue, targetQueueOwner, targetQueueBoundExchange,
                targetQueueBoundExchangeType, isTargetQueueBoundExchangeAutoDeletable, hasExternalSubscriptions);

    }

    /**
     * create andes ack data message
     * @param channelID id of the connection message was received
     * @param messageID id of the message
     * @return Andes Ack Data
     */
    public static AndesAckData generateAndesAckMessage(UUID channelID, long messageID) throws AndesException {
        LocalSubscription localSubscription = AndesContext.getInstance().
                getSubscriptionStore().getLocalSubscriptionForChannelId(channelID);
        if(null == localSubscription) {
            log.error("Cannot handle acknowledgement for message ID = " + messageID + " as subscription is closed "
                    + "channelID= " + "" + channelID);
            return null;
        }
        DeliverableAndesMetadata metadata = localSubscription.getMessageByMessageID(messageID);
        return new AndesAckData(channelID, metadata);
    }

    /**
     * Get DeliverableAndesMetadata reference of a delivered message
     * @param messageID ID of the message
     * @param channelID ID of the channel message is delivered
     * @return DeliverableAndesMetadata reference
     * @throws AndesException
     */
    public static DeliverableAndesMetadata lookupDeliveredMessage(long messageID, UUID channelID) throws
            AndesException {
        LocalSubscription localSubscription = AndesContext.getInstance().getSubscriptionStore()
                .getLocalSubscriptionForChannelId(channelID);
        return localSubscription.getMessageByMessageID(messageID);
    }

    /**
     * Method to determine if a given destination represents a queue rather than a durable topic or a temporary topic
     * subscription.
     *
     * @param destination the name of the destination
     * @return true if the given destination is associated with a queue, false if it is a temporary topic or a
     * durable topic subscription
     */
    private static boolean isPersistentQueue(String destination) {
        if (destination.startsWith("tmp_") || destination.contains("carbon:") || destination
                .startsWith("TempQueue")) {
            return false;
        }
        return true;
    }

    /**
     * Method to filter queue names from a given list of destinations
     *
     * @param destinations the list of destinations
     * @return the filtered list of destinations which only include queue names
     */
    public static List<String> filterQueueDestinations(List<String> destinations) {
        Iterator itr = destinations.iterator();
        //remove topic specific queues
        while (itr.hasNext()) {
            String destinationQueueName = (String) itr.next();
            if (!(isPersistentQueue(destinationQueueName))) {
                itr.remove();
            }
        }
        return destinations;
    }

}
