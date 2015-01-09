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
 * Wrapper class of message removable meta data
 *
 */
public class AndesRemovableMetadata {

    private long messageID;
    private String storageDestination;
    private String messageDestination;
    private boolean isForTopic;

    public AndesRemovableMetadata(long messageID, String messageDestination, String storageDestination){
        this.messageID = messageID;
        this.messageDestination = messageDestination;
        this.storageDestination = storageDestination;
    }

    public long getMessageID() {
        return messageID;
    }

    public String getStorageDestination() {
        return storageDestination;
    }

    public String getMessageDestination() {
        return messageDestination;
    }

    public boolean isForTopic() {
        return isForTopic;
    }

    public void setForTopic(boolean isForTopic) {
        this.isForTopic = isForTopic;
    }
}
