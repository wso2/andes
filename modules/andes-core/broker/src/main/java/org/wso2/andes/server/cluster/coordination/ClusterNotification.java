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

package org.wso2.andes.server.cluster.coordination;

import java.io.Serializable;

/**
 * This class represents a cluster notification to be transfer via HazelCast
 */
public class ClusterNotification implements Serializable {

    public static final String QUEUE_ADDED = "QUEUE_ADDED";
    public static final String QUEUE_DELETED = "QUEUE_DELETED";
    public static final String QUEUE_PURGED = "QUEUE_PURGED";
    public static final String BINDING_ADDED = "BINDING_ADDED";
    public static final String BINDING_DELETED = "BINDING_DELETED";
    public static final String EXCHANGE_ADDED = "EXCHANGE_ADDED";
    public static final String EXCHANGE_DELETED = "EXCHANGE_DELETED";
    public static final String SUBSCRIPTION_ADDED = "SUBSCRIPTION_ADDED";
    public static final String SUBSCRIPTION_DELETED = "SUBSCRIPTION_DELETED";
    public static final String SUBSCRIPTION_DISCONNECTED = "SUBSCRIPTION_DISCONNECTED";
    public static final String SUBSCRIPTION_MERGED = "SUBSCRIPTION_MERGED";

    /**
     * The type of change represented by the notification.
     */
    private String changeType;

    /**
     * The notification encoded as a string.
     */
    private String encodedObjectAsString;

    /**
     * The node by which the cluster notification was originally sent.
     */
    private String originatedNode;

    /**
     * Create an instance of cluster notification given the notification and the change type.
     *
     * @param encodedAsString encoded string to transfer thro
     * @param changeType      change happened (added/deleted etc)
     */
    public ClusterNotification(String encodedAsString, String changeType) {
        this.encodedObjectAsString = encodedAsString;
        this.changeType = changeType;
    }

    /**
     * Create an instance of cluster notification given the notification, the change type and the node from which the
     * cluster notification originated.
     *
     * @param encodedAsString encoded string to transfer thro
     * @param changeType      change happened (added/deleted etc)
     */
    public ClusterNotification(String encodedAsString, String changeType, String originatedNode) {
        this.encodedObjectAsString = encodedAsString;
        this.changeType = changeType;
        this.originatedNode = originatedNode;
    }

    /**
     * Get encoded string notification carries
     *
     * @return encoded notification
     */
    public String getEncodedObjectAsString() {
        return encodedObjectAsString;
    }

    /**
     * Get the change notification carries
     *
     * @return change the type of the change
     */
    public String getChangeType() {
        return changeType;
    }

    /**
     * Get the node from which the notification was originated.
     *
     * @return the originated node
     */
    public String getOriginatedNode() {
        return originatedNode;
    }
}
