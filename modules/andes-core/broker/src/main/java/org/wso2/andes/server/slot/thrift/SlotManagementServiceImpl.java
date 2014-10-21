/*
 *
 *   Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package org.wso2.andes.server.slot.thrift;

import org.apache.thrift.TException;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.slot.SlotManager;
import org.wso2.andes.server.slot.thrift.gen.SlotInfo;
import org.wso2.andes.server.slot.thrift.gen.SlotManagementService;

/**
 * This is the implementation of SlotManagementService interface
 */

public class SlotManagementServiceImpl implements SlotManagementService.Iface {

    private static SlotManager slotManager = SlotManager.getInstance();

    @Override
    public SlotInfo getSlotInfo(String queueName, String nodeId) throws TException {
        if (AndesContext.getInstance().getClusteringAgent().isCoordinator()) {
            Slot slot = slotManager.getSlot(queueName, nodeId);
            SlotInfo slotInfo = new SlotInfo();
            if (null != slot) {
                slotInfo = new SlotInfo(slot.getStartMessageId(), slot.getEndMessageId(),
                        slot.getQueueName());
            }
            return slotInfo;
        } else {
            throw new TException("This node is not the slot coordinator right now");
        }
    }

    @Override
    public void updateMessageId(String queueName, long messageId) throws TException {
        if (AndesContext.getInstance().getClusteringAgent().isCoordinator()) {
            slotManager.updateMessageID(queueName, messageId);
        } else {
            throw new TException("This node is not the slot coordinator right now");
        }
    }

    @Override
    public void deleteSlot(String queueName, SlotInfo slotInfo, String nodeId) throws TException {
        if (AndesContext.getInstance().getClusteringAgent().isCoordinator()) {
            Slot slot = new Slot();
            slot.setStartMessageId(slotInfo.getStartMessageId());
            slot.setEndMessageId(slotInfo.getEndMessageId());
            slot.setQueueName(slotInfo.getQueueName());
            slotManager.deleteSlot(queueName, slot, nodeId);
        } else {
            throw new TException("This node is not the slot coordinator right now");
        }
    }

    @Override
    public void reAssignSlotWhenNoSubscribers(String nodeId, String queueName) throws TException {
        if (AndesContext.getInstance().getClusteringAgent().isCoordinator()) {
            slotManager.reAssignSlotWhenNoSubscribers(nodeId, queueName);
        } else {
            throw new TException("This node is not the slot coordinator right now");
        }
    }

}
