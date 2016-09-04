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
import org.wso2.andes.kernel.AndesRecoveryTask;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.DBSyncNotificationHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * The following listener will receive notification through {@link com.hazelcast.core.ITopic} to execute the andes
 * recovery task to sync DB information such as subscription with the in-memory store
 */
public class HZBasedDatabaseSyncNotificationListener implements MessageListener {

    private static Log log = LogFactory.getLog(HZBasedDatabaseSyncNotificationListener.class);

    /**
     * Listeners interested for DB sync notifications
     */
    private List<DBSyncNotificationHandler> dbSyncNotificationListeners = new ArrayList<>();


    /**
     * Register a listener interested on DB sync notification requests on cluster
     *
     * @param listener listener to be registered
     */
    public void addHandler(DBSyncNotificationHandler listener) {
        dbSyncNotificationListeners.add(listener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onMessage(Message message) {
        //TODO: do we need to handle this only for non-local members?

        ClusterNotification clusterNotification = (ClusterNotification) message.getMessageObject();
        if (log.isDebugEnabled()) {
            log.debug("Handling cluster gossip: received a DB sync request " + clusterNotification
                    .getEncodedObjectAsString());
        }
        log.info("DB sync request received after a split brain recovery from cluster " + message
                .getPublishingMember());
        for (DBSyncNotificationHandler dbSyncNotificationListener : dbSyncNotificationListeners) {
            dbSyncNotificationListener.handleClusterNotification(clusterNotification);
        }
        log.info("DB sync completed for the request from cluster " + message.getPublishingMember());

    }
}
