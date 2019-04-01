/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.server.information.management;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.management.common.mbeans.DurableTopicSubscriptionInformation;
import org.wso2.andes.server.management.DefaultManagedObject;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

public class DurableTopicSubscriptionInformationMBean extends DefaultManagedObject
        implements DurableTopicSubscriptionInformation {

    private String storageQueue;

    public DurableTopicSubscriptionInformationMBean(String storageQueue) throws NotCompliantMBeanException {
        super(DurableTopicSubscriptionInformation.class, DurableTopicSubscriptionInformation.TYPE);
        this.storageQueue = storageQueue;
    }

    @Override
    public long getMsgCount() {
        try {
            return MessagingEngine.getInstance().getApproximateQueueMessageCount(storageQueue);
        } catch (AndesException e) {
            throw new RuntimeException("Error in retrieving pending message count", e);
        }
    }

    @Override
    public String getObjectInstanceName() {
        int index = storageQueue.indexOf(':');
        return ObjectName.quote(storageQueue.substring(index + 1));
    }
}
