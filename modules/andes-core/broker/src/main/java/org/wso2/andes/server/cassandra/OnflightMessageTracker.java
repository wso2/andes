/*
 *
 *   Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQStoreException;
import org.wso2.andes.kernel.*;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.slot.SlotDeliveryWorker;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class OnflightMessageTracker {

    private static Log log = LogFactory.getLog(OnflightMessageTracker.class);

    private int acktimeout = 10000;
    private int maximumRedeliveryTimes = 1;

    /**
     * In memory map keeping sent messages. If this map does not have an entry for a delivery
     * scheduled message it is a new message. Otherwise it is a redelivery
     */
    private LinkedHashMap<Long, MsgData> msgId2MsgData = new LinkedHashMap<Long, MsgData>();

    private Map<String, Long> deliveryTag2MsgID = new HashMap<String, Long>();
    private ConcurrentHashMap<UUID, HashSet<Long>> channelToMsgIDMap = new
            ConcurrentHashMap<UUID, HashSet<Long>>();
    private ConcurrentHashMap<Long, AndesMessageMetadata> messageIdToAndesMessagesMap = new
            ConcurrentHashMap<Long, AndesMessageMetadata>();
    private ConcurrentHashMap<Slot, AtomicInteger> pendingMessagesBySlot = new
            ConcurrentHashMap<Slot, AtomicInteger>();



    /**
     * In memory set keeping track of sent messageIds. Used to prevent duplicate message count
     * decrements
     */
    private HashSet<Long> deliveredButNotAckedMessages = new HashSet<Long>();

    private static final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor();
    private static final ScheduledExecutorService addedMessagedDeletionScheduler = Executors
            .newSingleThreadScheduledExecutor();

    private AtomicLong sendMessageCount = new AtomicLong();
    private AtomicLong sendButNotAckedMessageCount = new AtomicLong();
    private ConcurrentHashMap<String, ArrayList<AndesMessageMetadata>>
            queueTosentButNotAckedMessageMap = new ConcurrentHashMap<String,
            ArrayList<AndesMessageMetadata>>();


    private long startTime = -1;
    private ConcurrentHashMap<Long, Long> alreadyReadFromNodeQueueMessages = new
            ConcurrentHashMap<Long, Long>();

    /**
     * Class to keep tracking data of a message
     */
    public class MsgData {

        final long msgID;
        boolean ackreceived = false;
        final String queue;
        final long timestamp;
        final String deliveryID;
        final AMQChannel channel;
        int numOfDeliveries;
        boolean ackWaitTimedOut;

        public MsgData(long msgID, boolean ackreceived, String queue, long timestamp,
                       String deliveryID, AMQChannel channel, int numOfDeliveries,
                       boolean ackWaitTimedOut) {
            this.msgID = msgID;
            this.ackreceived = ackreceived;
            this.queue = queue;
            this.timestamp = timestamp;
            this.deliveryID = deliveryID;
            this.channel = channel;
            this.numOfDeliveries = numOfDeliveries;
            this.ackWaitTimedOut = ackWaitTimedOut;
        }
    }

    private static OnflightMessageTracker instance = new OnflightMessageTracker();

    public static OnflightMessageTracker getInstance() {
        return instance;
    }


    private OnflightMessageTracker() {

        this.acktimeout = ClusterResourceHolder.getInstance().getClusterConfiguration()
                .getMaxAckWaitTime() * 1000;
        this.maximumRedeliveryTimes = ClusterResourceHolder.getInstance().getClusterConfiguration()
                .getNumberOfMaximumDeliveryCount();
        /*
         * for all add and remove, following is executed, and it will remove the oldest entry if
         * needed
         */
        msgId2MsgData = new LinkedHashMap<Long, MsgData>() {
            private static final long serialVersionUID = -8681132571102532817L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, MsgData> eldest) {
                MsgData msgData = eldest.getValue();
                boolean todelete = (System.currentTimeMillis() - msgData.timestamp) > (acktimeout
                        * 10);
                if (todelete) {
                    if (!msgData.ackreceived) {
                        //reduce messages on flight on this channel
                        msgData.channel.decrementNonAckedMessageCount();
                        log.debug(
                                "No ack received for delivery tag " + msgData.deliveryID + " and " +
                                        "message id " + msgData.msgID);
                        //TODO notify the QueueDeliveryWorker to resend (it work now as well as
                        // flusher loops around, but this will be faster)
                    }
                    if (deliveryTag2MsgID.remove(msgData.deliveryID) == null) {
                        log.error(
                                "Cannot find delivery tag " + msgData.deliveryID + " and message " +
                                        "id " + msgData.msgID);
                    }
                }
                return todelete;
            }
        };

        /**
         * This thread will removed acked messages or messages that breached max redelivery count
         * from tracking
         * These messages are already scheduled to be removed from message store.
         */
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                //TODO replace this with Gvava Cache if possible
                synchronized (this) {
                    Iterator<MsgData> iterator = msgId2MsgData.values().iterator();
                    while (iterator.hasNext()) {
                        MsgData mdata = iterator.next();
                        if (mdata.ackreceived || (mdata.numOfDeliveries) > maximumRedeliveryTimes) {
                            iterator.remove();
                            deliveryTag2MsgID.remove(mdata.deliveryID);
                            if ((mdata.numOfDeliveries) > maximumRedeliveryTimes) {
                                log.warn(
                                        "Message " + mdata.msgID + " with " + mdata.deliveryID +
                                                " removed as it has gone though max redeliveries");
                            }
                        }
                    }
                }
            }
        }, 5, 10, TimeUnit.SECONDS);

        addedMessagedDeletionScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                //TODO replace this with Gvava Cache if possible
                synchronized (this) {
                    Iterator<Map.Entry<Long, Long>> keys = alreadyReadFromNodeQueueMessages
                            .entrySet().iterator();
                    while (keys.hasNext()) {
                        Map.Entry<Long, Long> entry = keys.next();
                        long timeStamp = entry.getValue();
                        if (timeStamp > 0 && (System.currentTimeMillis() - timeStamp) > 60000) {
                            keys.remove();
                            if (log.isDebugEnabled()) {
                                log.debug("TRACING>> OFMT-Removed Message Id-" + entry
                                        .getKey() + "-from alreadyReadFromNodeQueueMessages");
                            }

                        }
                    }
                }
            }
        }, 5, 10, TimeUnit.SECONDS);

    }

    public void handleFailure(long deliveryTag, UUID channelId) throws AMQException {
        long newTimeStamp = System.currentTimeMillis();
        String deliveryID = new StringBuffer(channelId.toString()).append("/").append(deliveryTag)
                .toString();
        Long messageId = deliveryTag2MsgID.get(deliveryID);
        if (messageId != null) {
            try {
                MsgData msgData = msgId2MsgData.get(messageId);
                msgData.ackWaitTimedOut = true;
                //re-queue the message to send again
                reQueueMessage(messageId);
                unMarkMessageAsAlreadyReadFromNodeQueueMessageInstantly(messageId);
            } catch (AndesException e) {
                log.warn("Message " + messageId + "re-queueing failed");
                throw new AMQException(AMQConstant.INTERNAL_ERROR,
                        "Message " + messageId + "re-queueing failed", e);
            }
        }
    }

    /**
     * Re-queue the message to be sent again
     *
     * @param messageId
     */
    private void reQueueMessage(long messageId) throws AndesException {
        AndesMessageMetadata metadata = messageIdToAndesMessagesMap.get(messageId);
        QueueDeliveryWorker.QueueDeliveryInfo queueDeliveryInfo = QueueDeliveryWorker.getInstance().
                getQueueDeliveryInfo(metadata.getDestination());
        queueDeliveryInfo.readButUndeliveredMessages.add(metadata);
        messageIdToAndesMessagesMap.remove(messageId);
    }

    /**
     * Message is allowed to be sent if and only if it is a new message or an already sent message
     * whose ack wait time out has happened
     *
     * @param messageId
     * @return boolean if the message should be sent
     */
    public synchronized boolean testMessage(long messageId) {
        long currentTime = System.currentTimeMillis();
        MsgData mdata = msgId2MsgData.get(messageId);
        //we do not redeliver the message until ack-timeout is breached
        if (mdata == null || (!mdata.ackreceived && mdata.ackWaitTimedOut)) {
            if (mdata != null) {
                mdata.channel.decrementNonAckedMessageCount();
            }
            return true;
        } else {
            return false;
        }
    }


    public boolean checkIfAlreadyReadFromNodeQueue(long messageID) {
        synchronized (this) {
            if (alreadyReadFromNodeQueueMessages.get(messageID) == null) {
                log.debug("TRACING>> OFMT-There is no item with messageID -" + messageID);
                return false;
            } else {
                log.debug("TRACING>> OFMT-There exists an item with messageID -" + messageID);
                return true;
            }
        }
    }

    public void markMessageAsReadFromNodeQueue(long messageID) {
        synchronized (this) {
            alreadyReadFromNodeQueueMessages.put(messageID, 0L);
        }
    }

    public void scheduleToDeleteMessageFromReadMessageFromNodeQueueMap(long messageID) {
        synchronized (this) {
            alreadyReadFromNodeQueueMessages.put(messageID, System.currentTimeMillis());
        }
    }

    public void unMarkMessageAsAlreadyReadFromNodeQueueMessageInstantly(long messageId) {
        alreadyReadFromNodeQueueMessages.remove(messageId);
    }

    /**
     * This cleanup the current message ID form tracking. Useful for undo changes in case of a
     * failure
     *
     * @param deliveryTag
     * @param messageId
     * @param channel
     */
    public void removeMessage(AMQChannel channel, long deliveryTag, long messageId) {
        String deliveryID = new StringBuffer(channel.getId().toString()).append("/")
                .append(deliveryTag)
                .toString();
        Long messageIDStored = deliveryTag2MsgID.remove(deliveryID);

        if (messageIDStored != null && messageIDStored.longValue() != messageId) {
            throw new RuntimeException(
                    "Delivery Tag " + deliveryID + " reused for " + messageId + " and " +
                            messageIDStored + " , this should not happen");
        }
        msgId2MsgData.remove(messageId);

        log.info("OFMT-Unexpected remove for messageID- " + messageId);
    }

    public synchronized boolean testAndAddMessage(AndesMessageMetadata andesMetaDataEntry,
                                                  long deliveryTag, AMQChannel channel)
            throws AMQException {

        //TODO - hasitha - are these AMQP specific checks?

        long messageId = andesMetaDataEntry.getMessageID();

        String queue = andesMetaDataEntry.getDestination();

        //String nodeSpecificQueueName = queue + "_" + ClusterResourceHolder.getInstance()
        // .getClusterManager().getMyNodeID();
        String nodeSpecificQueueName = queue;

        String deliveryID = new StringBuffer(channel.getId().toString()).append("/")
                .append(deliveryTag)
                .toString();

        long currentTime = System.currentTimeMillis();
        MsgData mdata = msgId2MsgData.get(messageId);
        int numOfDeliveriesOfCurrentMsg = 0;

        if (deliveryTag2MsgID.containsKey(deliveryID)) {
            throw new RuntimeException(
                    "Delivery Tag " + deliveryID + " reused, this should not happen");
        }
        if (mdata == null) {
            //this is a new message
            deliveredButNotAckedMessages.add(messageId);
            log.debug(
                    "TRACING>> OFMT-testAndAdd-scheduling new message to deliver with MessageID-"
                            + messageId);
        }
        //this is an already sent but ack wait time expired message
        else {
            numOfDeliveriesOfCurrentMsg = mdata.numOfDeliveries;
            // entry should have "ReDelivery" header
            andesMetaDataEntry.setRedelivered();
            // message has sent once, we will clean lists and consider it a new message,
            // but with delivery times tracked
            deliveryTag2MsgID.remove(mdata.deliveryID);
            msgId2MsgData.remove(messageId);
            log.debug(
                    "TRACING>> OFMT- testAndAdd-scheduling ack expired message to deliver with " +
                            "MessageID-" + messageId);
        }
        numOfDeliveriesOfCurrentMsg++;
        deliveryTag2MsgID.put(deliveryID, messageId);
        msgId2MsgData.put(messageId,
                new MsgData(messageId, false, nodeSpecificQueueName, currentTime,
                        deliveryID, channel, numOfDeliveriesOfCurrentMsg, false));
        sendButNotAckedMessageCount.incrementAndGet();

        HashSet<Long> messagesDeliveredThroughThisChannel = channelToMsgIDMap.get(channel.getId());
        if (messagesDeliveredThroughThisChannel == null) {
            messagesDeliveredThroughThisChannel = new HashSet<Long>();
            messagesDeliveredThroughThisChannel.add(messageId);
            channelToMsgIDMap.put(channel.getId(), messagesDeliveredThroughThisChannel);
        } else {
            messagesDeliveredThroughThisChannel.add(messageId);
        }
        messageIdToAndesMessagesMap.put(messageId, andesMetaDataEntry);
        /**
         * any custom checks or procedures that should be executed before message delivery should
         * happen here. Any message
         * rejected at this stage will be dropped from the node queue permanently.
         */

        //check if number of redelivery tries has breached.
        if (numOfDeliveriesOfCurrentMsg > ClusterResourceHolder.getInstance()
                .getClusterConfiguration()
                .getNumberOfMaximumDeliveryCount()) {
            log.warn(
                    "Number of Maximum Redelivery Tries Has Breached. Dropping The Message: " +
                            messageId + "From Queue " + queue);
            return false;
            //check if queue entry has expired. Any expired message will not be delivered
        } else if (andesMetaDataEntry.isExpired()) {
            log.warn("Message is expired. Dropping The Message: " + messageId);
            return false;
        }
        return true;
    }

    public synchronized void ackReceived(UUID channelID, long messageId)
            throws AMQStoreException, AndesException {
        MsgData msgData = msgId2MsgData.get(messageId);
        if (msgData != null) {
            msgData.ackreceived = true;

            //how much time took between delivery and ack receive
            long timeTook = (System.currentTimeMillis() - msgData.timestamp);
            PerformanceCounter.recordAckReceived(msgData.queue, (int) timeTook);

            // then update the tracker
            log.debug("TRACING>> OFMT-Ack received for MessageID-" + msgData.msgID);

            sendButNotAckedMessageCount.decrementAndGet();
            channelToMsgIDMap.get(channelID).remove(messageId);
            AndesMessageMetadata metadata = messageIdToAndesMessagesMap.remove(messageId);

            /*
            decrement pending message count and check whether the slot is empty. If so read the
            slot again from message store.
             */
            decrementMessageCountInSlotAndCheckToResend(metadata.getSlot(), msgData.queue);

        } else {
            throw new RuntimeException("No message data found for messageId " + messageId);
        }

    }

    public void releaseAckTrackingSinceChannelClosed(AMQChannel channel) {
        HashSet<Long> sentButNotAckedMessages = channelToMsgIDMap.get(channel.getId());

        if (sentButNotAckedMessages != null && sentButNotAckedMessages.size() > 0) {
            Iterator iterator = sentButNotAckedMessages.iterator();
            if (iterator != null) {
                while (iterator.hasNext()) {
                    long messageId = (Long) iterator.next();
                    if (msgId2MsgData.get(messageId) != null) {
                        String destinationQueueName = msgId2MsgData.remove(messageId).queue;
                        sendButNotAckedMessageCount.decrementAndGet();
                        AndesMessageMetadata queueEntry = messageIdToAndesMessagesMap
                                .remove(messageId);
                        ArrayList<AndesMessageMetadata> undeliveredMessages =
                                queueTosentButNotAckedMessageMap
                                        .get(destinationQueueName);
                        if (undeliveredMessages == null) {
                            undeliveredMessages = new ArrayList<AndesMessageMetadata>();
                            undeliveredMessages.add(queueEntry);
                            queueTosentButNotAckedMessageMap
                                    .put(destinationQueueName, undeliveredMessages);
                            log.debug(
                                    "TRACING>> OFMT- Added message-" + messageId + "-to delivered" +
                                            " but not acked list");
                        } else {
                            undeliveredMessages.add(queueEntry);
                        }
                    }
                }
            }
        }
        channelToMsgIDMap.remove(channel.getId());
    }

    public synchronized void updateDeliveredButNotAckedMessages(long messageID) {
        deliveredButNotAckedMessages.remove(messageID);
    }

    /**
     * Remove all tracking information from a message moved to Dead Letter Channel.
     *
     * @param messageId message ID
     */
    public void removeTrackingInformationForDeadMessages(long messageId) {

        //if it is an already sent but not acked message we will not decrement message count again
        MsgData messageData = msgId2MsgData.get(messageId);
        if (messageData != null) {
            //we do following to stop trying to delete message again when acked someday
            deliveredButNotAckedMessages.remove(messageId);
        }
    }

    public long getSentButNotAckedMessageCount() {
        return sendButNotAckedMessageCount.get();
    }

    public ArrayList<AndesMessageMetadata> getSentButNotAckedMessagesOfQueue(String queueName) {
        return queueTosentButNotAckedMessageMap.remove(queueName);
    }

    /**
     * decrement message count in slot and if it is zerocheck the slot again to resend
     * @param slot
     * @param queueName
     * @throws AndesException
     */
    public void decrementMessageCountInSlotAndCheckToResend(Slot slot, String queueName) throws AndesException {
        AtomicInteger pendingMessageCount = pendingMessagesBySlot.get(slot);
        int messageCount = pendingMessageCount.decrementAndGet();
        if(messageCount == 0){
                /*
                All the Acks for the slot has bee received. Check the slot again for unsend
                messages and if there are any send them and delete the slot.
                 */
            SlotDeliveryWorker slotWorker = SlotDeliveryWorkerManager.getInstance()
                    .getSlotWorker(queueName);
            slotWorker.checkForSlotCompletionAndResend(slot);
        }

    }

    /**
     * increment the message count in a slot
     * @param slot
     */
    public void incrementMessageCountInSlot(Slot slot){
        AtomicInteger pendingMessageCount = pendingMessagesBySlot.get(slot);
        if (pendingMessageCount == null) {
            pendingMessagesBySlot.putIfAbsent(slot, new AtomicInteger());
            pendingMessageCount = pendingMessagesBySlot.get(slot);
        }
        pendingMessageCount.incrementAndGet();
    }
}
