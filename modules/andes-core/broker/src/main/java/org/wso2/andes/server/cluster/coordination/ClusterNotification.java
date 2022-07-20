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
 * This class represents a cluster notification to be transfer
 */
public class ClusterNotification implements Serializable {
    /**
     * The artifact carried in the notification
     * (i.e queue, messageRouter, binding, subscription)
     */
    private String notifiedArtifact;

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
     * Human readable description of the notification.
     */
    private String description;


    /**
     * Create an instance of cluster notification given the notification, the change type and the node from which the
     * cluster notification originated.
     *
     * @param encodedAsString  encoded string to transfer thro
     * @param notifiedArtifact artifact that is notified by this notification
     * @param changeType       change happened (added/deleted etc)
     * @param description      human readable description of the notification
     * @param originatedNode   ID of the node notification originated from
     */
    public ClusterNotification(String encodedAsString, String notifiedArtifact, String changeType, String
            description, String originatedNode) {
        this.encodedObjectAsString = encodedAsString;
        this.notifiedArtifact = notifiedArtifact;
        this.changeType = changeType;
        this.description = description;
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
     * Get what the notified artifact is. Is the what has changed at the notification
     * originated node
     *
     * @return String value of {@link org.wso2.andes.kernel.ClusterNotificationListener.NotifiedArtifact}
     */
    public String getNotifiedArtifact() {
        return notifiedArtifact;
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


    /**
     * Get human readable description of the notification
     *
     * @return description
     */
    public String getDescription() {
        return description;
    }
}
