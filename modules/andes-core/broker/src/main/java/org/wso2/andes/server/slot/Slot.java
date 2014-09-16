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
package org.wso2.andes.server.slot;

import java.io.Serializable;

public class Slot implements Serializable {

    private long messageCount;
    private long startMessageId;
    private long endMessageId;
    private String queue;


    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getQueue() {
        return queue;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }

    public long getEndMessageId() {
        return endMessageId;
    }

    public void setEndMessageId(long endMessageId) {
        this.endMessageId = endMessageId;
    }

    public long getStartMessageId() {
        return startMessageId;
    }

    public void setStartMessageId(long startMessageId) {
        this.startMessageId = startMessageId;
    }

    public void setMessageRange(long startMessageId, long endMessageId){
       this.startMessageId = startMessageId;
        this.endMessageId = endMessageId;
    }

}
