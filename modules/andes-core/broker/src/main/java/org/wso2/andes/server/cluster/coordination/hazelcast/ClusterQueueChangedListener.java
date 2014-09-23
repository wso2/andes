/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.QueueListener;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;

import java.util.ArrayList;
import java.util.List;

/**
 * This listener class is triggered when queue change happened in cluster via HazelCast.
 */
public class ClusterQueueChangedListener implements MessageListener {
    private static Log log = LogFactory.getLog(ClusterQueueChangedListener.class);
    private List<QueueListener> queueListeners = new ArrayList<QueueListener>();

    /**
     * Register a listener interested on queue changes in cluster
     *
     * @param listener listener to be registered
     */
    public void addQueueListener(QueueListener listener) {
        queueListeners.add(listener);
    }

    @Override
    public void onMessage(Message message) {
        ClusterNotification clusterNotification = (ClusterNotification) message.getMessageObject();
        log.info("Handling cluster gossip: received a queue change notification " + clusterNotification.getDescription());
        AndesQueue andesQueue = new AndesQueue(clusterNotification.getEncodedObjectAsString());
        QueueListener.QueueChange change = QueueListener.QueueChange.valueOf(clusterNotification.getChangeType());
        try {
            for (QueueListener queueListener : queueListeners) {
                queueListener.handleClusterQueuesChanged(andesQueue, change);
            }
        } catch (AndesException e) {
            log.error("error while handling cluster queue change notification", e);
        }
    }
}
