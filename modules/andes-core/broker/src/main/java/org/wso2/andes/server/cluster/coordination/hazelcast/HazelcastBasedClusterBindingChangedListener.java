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

package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.wso2.andes.kernel.BindingListener;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;


/**
 * This listener class is triggered when binding change happened in cluster via HazelCast.
 */
public class HazelcastBasedClusterBindingChangedListener implements MessageListener {

    /**
     * Listener to handle cluster event.
     */
    private ClusterNotificationHandler eventListener = new ClusterNotificationHandler();

    /**
     * Register a listener interested on binding changes in cluster.
     *
     * @param listener listener to be registered
     */
    public void addBindingListener(BindingListener listener) {
        eventListener.addBindingListener(listener);
    }

    @Override
    public void onMessage(Message message) {
        ClusterNotification clusterNotification = (ClusterNotification) message.getMessageObject();
        eventListener.handleClusterBindingsChanged(clusterNotification.getEncodedObjectAsString(),
                BindingListener.BindingEvent.valueOf(clusterNotification.getChangeType()));
    }
}
