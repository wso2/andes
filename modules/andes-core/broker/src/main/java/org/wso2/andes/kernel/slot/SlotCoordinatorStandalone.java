/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.slot;

/**
 * This class is responsible of coordinating with the Standalone Slot Manager
 */
public class SlotCoordinatorStandalone implements SlotCoordinator {

    private  SlotManagerStandalone slotManagerStandalone;

    public SlotCoordinatorStandalone(){
        slotManagerStandalone= new SlotManagerStandalone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Slot getSlot(String queueName) {
        Slot slot = slotManagerStandalone.getSlot(queueName);
        if(null == slot){
           slot = new Slot();
        }
        return slot;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMessageId(String queueName, long startMessageId, long endMessageId) throws ConnectionException {
        slotManagerStandalone.updateMessageID(queueName,endMessageId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSlotDeletionSafeZone(long currentSlotDeleteSafeZone) {
        //We do not do anything here since safe zone is not applicable to standalone mode
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteSlot(String queueName, Slot slot) throws ConnectionException {
        //We do not do anything here since slot deletion is not applicable to standalone mode. We
        // don't keep slot assignment details in standalone mode
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reAssignSlotWhenNoSubscribers(String queueName) throws ConnectionException {
        //We do not do anything here since slot reassigning is not applicable to standalone mode.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearAllActiveSlotRelationsToQueue(String queueName) {
        slotManagerStandalone.clearAllActiveSlotRelationsToQueue(queueName);
    }
}
