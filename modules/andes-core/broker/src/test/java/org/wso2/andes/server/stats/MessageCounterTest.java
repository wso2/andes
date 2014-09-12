package org.wso2.andes.server.stats;

import junit.framework.TestCase;
import org.wso2.andes.messageStore.CQLBasedMessageStoreImpl;

import java.util.Map;
import java.util.Set;

/**
 * Tests for org.wso2.andes.server.stats.MessageCounter
 */
public class MessageCounterTest extends TestCase {

    MessageCounter messageCounter = MessageCounter.getInstance();
    public static final String QUEUE_NAME = "testQueue";

    public void testUpdateOngoingMessageStatus() {
        messageCounter.updateOngoingMessageStatus(1L, MessageCounterKey.MessageCounterType.PUBLISH_COUNTER, QUEUE_NAME, 1L);
        messageCounter.updateOngoingMessageStatus(1L, MessageCounterKey.MessageCounterType.DELIVER_COUNTER, QUEUE_NAME, 2L);
        messageCounter.updateOngoingMessageStatus(1L, MessageCounterKey.MessageCounterType.ACKNOWLEDGED_COUNTER, QUEUE_NAME, 3L);

        try {
            Thread.sleep(CQLBasedMessageStoreImpl.STATS_STATEMENT_EXECUTE_SCHEDULE_TIME * 1000); // wait for the message to be inserted
            Set<MessageStatus> messageStatuses = messageCounter.getOnGoingMessageStatus(QUEUE_NAME, 0L, 2L, 0L, 5L, false);
            assertEquals(1, messageStatuses.size());

            for(MessageStatus entry : messageStatuses) {
                long messageId = entry.getMessageId();
                assertEquals(1L, messageId);
                long pubilshedTime = entry.getPublishedTime();
                long deliveredTime = entry.getDeliveredTime();
                long acknowledgedTime = entry.getAcknowledgedTime();
                assertEquals(1L, pubilshedTime);
                assertEquals(2L, deliveredTime);
                assertEquals(3L, acknowledgedTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void testGetMessageStatusChangeTimes() {
        messageCounter.updateOngoingMessageStatus(1L, MessageCounterKey.MessageCounterType.PUBLISH_COUNTER, QUEUE_NAME, 1L);
        messageCounter.updateOngoingMessageStatus(2L, MessageCounterKey.MessageCounterType.PUBLISH_COUNTER, QUEUE_NAME, 1L);

        try {
            Thread.sleep(CQLBasedMessageStoreImpl.STATS_STATEMENT_EXECUTE_SCHEDULE_TIME * 1000); // wait for the message to be inserted
            Map<Long, Long> messageStatuses = messageCounter.getMessageStatusChangeTimes(QUEUE_NAME, 0L, 2L, 0L, 5L, MessageCounterKey.MessageCounterType.PUBLISH_COUNTER);
            assertEquals(2, messageStatuses.size());

            long i = 1;
            for(Map.Entry<Long, Long> entry : messageStatuses.entrySet()) {
                long messageId = entry.getKey();
                assertEquals(i, messageId);
                long pubilshedTime = entry.getValue();
                assertEquals(1L, pubilshedTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void testGetMessageStatusCounts() {
        messageCounter.updateOngoingMessageStatus(1l, MessageCounterKey.MessageCounterType.PUBLISH_COUNTER, QUEUE_NAME, 1L);
        messageCounter.updateOngoingMessageStatus(2l, MessageCounterKey.MessageCounterType.PUBLISH_COUNTER, QUEUE_NAME, 1L);

        try {
            Thread.sleep(CQLBasedMessageStoreImpl.STATS_STATEMENT_EXECUTE_SCHEDULE_TIME * 1000); // wait for the message to be inserted
            long count = messageCounter.getMessageStatusCounts(QUEUE_NAME, 0L, 2L);
            assertEquals(2L, count);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
