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

package org.wso2.andes.kernel.disruptor.inbound;

/**
 * Information container for queue
 */
public class QueueInfo {

    private String queueName;
    private boolean isDurable;
    private String queueOwner;
    private boolean isExclusive;
    private boolean isShared;

    /**
     * Create information bean for Storage queue
     *
     * @param queueName   name of the queue
     * @param isDurable   true if queue is durable
     * @param isShared    true if queue is shared
     * @param queueOwner  owner of the queue
     * @param isExclusive true if queue is exclusive
     */
    public QueueInfo(String queueName, boolean isDurable, boolean isShared, String queueOwner, boolean isExclusive) {
        this.queueName = queueName;
        this.isDurable = isDurable;
        this.isShared = isShared;
        if (null == queueOwner || queueOwner.equals("")) {
            this.queueOwner = "null";
        } else {
            this.queueOwner = queueOwner;
        }
        this.isExclusive = isExclusive;
    }

    public boolean isDurable() {
        return isDurable;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getQueueOwner() {
        return queueOwner;
    }

    public boolean isExclusive() {
        return isExclusive;
    }

    public boolean isShared() {
        return isShared;
    }
}
