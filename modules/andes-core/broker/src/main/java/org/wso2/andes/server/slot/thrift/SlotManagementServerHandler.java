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
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.slot.SlotManager;
import org.wso2.andes.server.slot.thrift.gen.SlotInfo;
import org.wso2.andes.server.slot.thrift.gen.SlotManagementService;

public class SlotManagementServerHandler implements SlotManagementService.Iface {

    private static SlotManager slotManager = SlotManager.getInstance();

    @Override
    public SlotInfo getSlot(String queueName) throws TException {
        Slot slot = slotManager.getSlot(queueName);
        SlotInfo slotInfo = new SlotInfo(slot.getStartMessageId(), slot.getEndMessageId(), slot.getQueue());
        return slotInfo;
    }

    @Override
    public void updateMessageId(String queueName, long messageId) throws TException {
       slotManager.updateMessageID(queueName,messageId);
    }
}
