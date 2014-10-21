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

package org.wso2.andes.kernel;

/**
 * Listener listening for queue changes local and cluster
 */
public interface QueueListener {

    public static enum QueueChange {
        Added,
        Deleted,
        Purged
    }

    /**
     * handle queue has changed in the cluster
     *
     * @param andesQueue changed queue
     * @param changeType what type of change has happened
     */
    public void handleClusterQueuesChanged(AndesQueue andesQueue, QueueChange changeType) throws AndesException;

    /**
     * handle a queue has changed in the local node
     *
     * @param andesQueue changed queue
     * @param changeType what type of change has happened
     */
    public void handleLocalQueuesChanged(AndesQueue andesQueue, QueueChange changeType) throws AndesException;
}
