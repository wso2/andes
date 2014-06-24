package org.wso2.andes.kernel.distrupter;

import com.lmax.disruptor.EventFactory;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;

public class SubscriptionDataEvent {
    // Used to send data to the end points
    public Subscription subscription;
    public QueueEntry message;

    public static class CassandraDEventFactory implements EventFactory<SubscriptionDataEvent> {
        @Override
        public SubscriptionDataEvent newInstance() {
            return new SubscriptionDataEvent();
        }
    }

    public static EventFactory<SubscriptionDataEvent> getFactory() {
        return new CassandraDEventFactory();
    }

}
