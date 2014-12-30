/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * Wrapper class of message acknowledgment data publish to disruptor
 */
public class AndesAckData {

    private DeliverableAndesMessageMetadata messageReference;
    private AndesChannel ackedChannel;

    public AndesAckData(AndesChannel ackedChannel, DeliverableAndesMessageMetadata messageReference) {
        this.ackedChannel = ackedChannel;
        this.messageReference = messageReference;
    }

    public DeliverableAndesMessageMetadata getMessageReference() {
        return  messageReference;
    }

    public AndesChannel getAckedChannel() {
        return ackedChannel;
    }

    public long getMessageID() {
        return messageReference.getMessageID();
    }

    public String getDestination() {
        return messageReference.getDestination();
    }

    public String getMsgStorageDestination() {
        return messageReference.getStorageQueueName();
    }

    public boolean isTopic() {
        return messageReference.isTopic;
    }

    public long getChannelID() {
        return ackedChannel.getChannelID();
    }

}
