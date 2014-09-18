package org.wso2.andes.server.cluster.coordination.hazelcast;


import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;

/**
 * This listener class is triggered when coordinator in the cluster is changed
 **/

public class ThriftCooordinatorListener implements MessageListener {

    @Override
    public void onMessage(Message message) {
        ClusterNotification clusterNotification = (ClusterNotification) message.getMessageObject();

    }
}
