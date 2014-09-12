/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.andes.server.stats;

/**
 * This class holds the MessageStatus values for a single Message and can be used to store message status data
 * retrieved from the database.
 */
public class MessageStatus {

    private Long messageId;
    private String queueName;
    private Long publishedTime;
    private Long deliveredTime;
    private Long acknowledgedTime;

    /**
     * Ge the message Id.
     * @return message Id
     */
    public Long getMessageId() {
        return messageId;
    }

    /**
     * Set the message Id.
     * @param paramMessageId message Id
     */
    public void setMessageId(Long paramMessageId) {
        this.messageId = paramMessageId;
    }

    /**
     * Get the message queue name.
     * @return queue name
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Set the message queue name for the current object.
     * @param paramQueueName the queue name
     */
    public void setQueueName(String paramQueueName) {
        this.queueName = paramQueueName;
    }

    /**
     * Get the message published time from the current object.
     * @return message published time
     */
    public Long getPublishedTime() {
        return publishedTime;
    }

    /**
     * Set the message published time for the current object in millis.
     * @param paramPublishedTime message published time in millis
     */
    public void setPublishedTime(Long paramPublishedTime) {
        this.publishedTime = paramPublishedTime;
    }

    /**
     * Get the message delivered time from the current object in millis.
     * @return message delivered time in millis
     */
    public Long getDeliveredTime() {
        return deliveredTime;
    }

    /**
     * Set the message delivered time for the current object with timemillis.
     * @param paramDeliveredTime the delivered time in millis
     */
    public void setDeliveredTime(Long paramDeliveredTime) {
        this.deliveredTime = paramDeliveredTime;
    }

    /**
     * Get the message acknowledged time from the current object.
     * @return message acknowledged time in millis
     */
    public Long getAcknowledgedTime() {
        return acknowledgedTime;
    }

    /**
     * Set the message acknowledge time for the current object in millis.
     * @param paramAcknowledgedTime message acknowledge time in millis
     */
    public void setAcknowledgedTime(Long paramAcknowledgedTime) {
        this.acknowledgedTime = paramAcknowledgedTime;
    }
}
