package org.wso2.andes.kernel.distrupter;

import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.QueueAddress;

import com.lmax.disruptor.EventFactory;

import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.server.queue.AMQQueue;

public class CassandraDataEvent {
    // Used to write data to the Cassandra
    public boolean isPart = false;
    public AndesMessageMetadata metadata;
    public AndesMessagePart part;

    public static class CassandraDEventFactory implements EventFactory<CassandraDataEvent> {
        @Override
        public CassandraDataEvent newInstance() {
            return new CassandraDataEvent();
        }
    }

    public static EventFactory<CassandraDataEvent> getFactory() {
        return new CassandraDEventFactory();
    }

}
