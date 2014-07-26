package org.wso2.andes.kernel.distrupter;

import com.lmax.disruptor.EventHandler;

import org.wso2.andes.kernel.AndesSubscription;

import java.util.ArrayList;
import java.util.List;

/**
 * We do this to make Listener take turns while running. So we can run many copies of these and control number
 * of IO threads through that.
 */
public class SubscriptionDataSender implements EventHandler<SubscriptionDataEvent> {
    private int writerCount;
    private int turn;
    private AndesSubscription delivery;
    // TODO - Disruptor make this configurable
    private int MAX_BUFFER_LIMIT = 100;

    private List<SubscriptionDataEvent> messageList = new ArrayList<SubscriptionDataEvent>();

    public SubscriptionDataSender(int writerCount, int turn, AndesSubscription delivery) {
        this.writerCount = writerCount;
        this.turn = turn;
        this.delivery = delivery;
    }

    public void onEvent(final SubscriptionDataEvent event, final long sequence, final boolean endOfBatch) throws Exception {
//        // We write in sequence per subscription
//        long calculatedTurn = Math.abs(event.subscription.getSubscriptionID() % writerCount);
//
//        if (calculatedTurn == turn) {
//            messageList.add(event);
//        }
//
//        if (messageList.size() > MAX_BUFFER_LIMIT || (endOfBatch)) {
//            // Send messages
//            if (messageList.size() > 0) {
//                delivery.sendAsynchronouslyToQueueEndPoint(messageList);
//                messageList.clear();
//            }
//        }
    }
}
