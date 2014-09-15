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

package org.wso2.andes.server.slot.thrift;


import org.apache.thrift.TException;
import org.wso2.andes.server.slot.thrift.gen.SlotInfo;
import org.wso2.andes.server.slot.thrift.gen.SlotManagementService;

/**
 * A wrapper client for the native thrift client
 */

public class MBThriftClient {

    SlotManagementService.Client client;

    public MBThriftClient(SlotManagementService.Client client) {
        this.client = client;
    }


    /**
     * getSlot method. Returns SlotInfo Object which will contain the slot information, when the queue name is given
     * @param queueName   name of the queue
     * @return  SlotInfo object
     * @throws TException in case of an connection error
     */
    public SlotInfo getSlot(String queueName) throws TException {
        return client.getSlot(queueName);
    }


    /**
     * updateMessageId method. This method will update the message ID list in the SlotManager
     * @param queueName name of the queue
     * @param messageId a known message ID
     * @throws TException in case of an connection error
     */
    public void updateMessageId(String queueName, long messageId) throws TException {
       client.updateMessageId(queueName,messageId);
    }
}
