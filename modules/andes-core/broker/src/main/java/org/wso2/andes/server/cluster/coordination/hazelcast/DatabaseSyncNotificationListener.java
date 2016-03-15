/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.andes.kernel.AndesRecoveryTask;
import org.wso2.andes.server.ClusterResourceHolder;

/**
 * The following listener will receive notification through {@link com.hazelcast.core.ITopic} to execute the andes
 * recovery task to sync DB information such as subscription with the
 * {@link org.wso2.andes.subscription.SubscriptionStore} cluster maps.
 */
public class DatabaseSyncNotificationListener implements MessageListener {

    /**
     * {@inheritDoc}
     */
    @Override
    public void onMessage(Message message) {
        AndesRecoveryTask andesRecoveryTask = ClusterResourceHolder.getInstance().getAndesRecoveryTask();
        andesRecoveryTask.scheduleNow();
    }
}
