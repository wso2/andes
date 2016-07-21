/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.ExchangeNotificationHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * This listener class is triggered when exchange change happened in cluster via HazelCast.
 */
public class HZBasedClusterExchangeChangedListener implements MessageListener {

    private static Log log = LogFactory.getLog(HZBasedClusterExchangeChangedListener.class);

    /**
     * Listeners interested for exchange change notifications
     */
    private List<ExchangeNotificationHandler> exchangeListeners = new ArrayList<>();

    /**
     * Register a listener interested in exchange changes in cluster
     *
     * @param listener listener to be registered
     */
    public void addExchangeListener(ExchangeNotificationHandler listener) {
        exchangeListeners.add(listener);
    }

    @Override
    public void onMessage(Message message) {
        if (!(message.getPublishingMember().localMember())) {
            ClusterNotification clusterNotification = (ClusterNotification) message.getMessageObject();
            if (log.isDebugEnabled()) {
                log.debug("Handling cluster gossip: received a exchange change notification " + clusterNotification
                        .getEncodedObjectAsString());
            }
            for (ExchangeNotificationHandler exchangeListener : exchangeListeners) {
                exchangeListener.handleClusterNotification(clusterNotification);
            }
        }
    }
}
