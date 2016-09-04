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

package org.wso2.andes.server.cluster.coordination;


import org.wso2.andes.kernel.AndesRecoveryTask;
import org.wso2.andes.kernel.ClusterNotificationListener;
import org.wso2.andes.server.ClusterResourceHolder;

/**
 * ClusterNotificationListener implementation listening for DB sync request notifications
 * and handling them
 */
public class DBSyncNotificationHandler implements ClusterNotificationListener {

    @Override
    public void handleClusterNotification(ClusterNotification notification) {
        AndesRecoveryTask andesRecoveryTask = ClusterResourceHolder.getInstance().getAndesRecoveryTask();
        andesRecoveryTask.executeNow();
    }
}
