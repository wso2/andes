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

package org.wso2.andes.server.cluster.coordination.rdbms;

import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotState;
import org.wso2.andes.server.cluster.coordination.SlotAgent;
import org.wso2.andes.store.AndesDataIntegrityViolationException;

import java.util.Set;
import java.util.TreeSet;

public class DatabaseSlotAgent implements SlotAgent {

	private AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createSlot(long startMessageId, long endMessageId,
						   String storageQueueName, String assignedNodeId)
							throws AndesException{
		andesContextStore
				.createSlot(startMessageId, endMessageId,storageQueueName, assignedNodeId);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteSlot(String nodeId, String queueName, long startMessageId, long endMessageId) throws AndesException{
		andesContextStore.deleteSlot(startMessageId, endMessageId);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteSlotAssignmentByQueueName(String nodeId, String queueName) throws AndesException{
		andesContextStore.deleteSlotAssignmentByQueueName(nodeId, queueName);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public Slot getUnAssignedSlot(String queueName) throws AndesException{
		return andesContextStore.selectUnAssignedSlot(queueName);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateSlotAssignment(String nodeId, String queueName, Slot allocatedSlot)
			throws AndesException{
		andesContextStore.createSlotAssignment(nodeId, queueName,
												allocatedSlot.getStartMessageId(), allocatedSlot.getEndMessageId());
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getQueueToLastAssignedId(String queueName) throws AndesException{
		return andesContextStore.getQueueToLastAssignedId(queueName);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setQueueToLastAssignedId(String queueName, long lastAssignedId) throws AndesException{
		andesContextStore.setQueueToLastAssignedId(queueName, lastAssignedId);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public Long getNodeToLastPublishedId(String nodeId) throws AndesException{
		return andesContextStore.getNodeToLastPublishedId(nodeId);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setNodeToLastPublishedId(String nodeId, long lastPublishedId) throws AndesException{
		andesContextStore.setNodeToLastPublishedId(nodeId, lastPublishedId);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removePublisherNode(String nodeId) throws AndesException {
		andesContextStore.removePublisherNodeId(nodeId);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public TreeSet<String> getMessagePublishedNodes() throws AndesException{
		return andesContextStore.getMessagePublishedNodes();
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setSlotState(long startMessageId, long endMessageId, SlotState slotState)
																			throws AndesException{
		andesContextStore.setSlotState(startMessageId, endMessageId, slotState);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public Slot getOverlappedSlot(String nodeId, String queueName) throws AndesException{
		return andesContextStore.getOverlappedSlot(queueName);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void addMessageId(String queueName, long messageId) throws AndesException {
		try {
			andesContextStore.addMessageId(queueName, messageId);
		} catch (AndesDataIntegrityViolationException ignore) {
			//Same message id can be added to list when slots are overlapped. In RDBMS slot store
			//composite primary key of queue name and message id have been used to avoid duplicates.
			//Therefore primary key violation exception can be ignored.
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public TreeSet<Long> getMessageIds(String queueName) throws AndesException{
		return andesContextStore.getMessageIds(queueName);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteMessageId(String queueName, long messageId) throws AndesException {
		andesContextStore.deleteMessageId(messageId);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteSlotsByQueueName(String queueName) throws AndesException {
		andesContextStore.deleteSlotsByQueueName(queueName);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteMessageIdsByQueueName(String queueName) throws AndesException {
		andesContextStore.deleteMessageIdsByQueueName(queueName);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public TreeSet<Slot> getAssignedSlotsByNodeId(String nodeId) throws AndesException {
		return andesContextStore.getAssignedSlotsByNodeId(nodeId);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public TreeSet<Slot> getAllSlotsByQueueName(String nodeId, String queueName) throws AndesException{
		return andesContextStore.getAllSlotsByQueueName(queueName);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reassignSlot(Slot slotToBeReassigned) throws AndesException {
        andesContextStore.deleteSlotAssignment(slotToBeReassigned.getStartMessageId(),
                                               slotToBeReassigned.getEndMessageId());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteOverlappedSlots(String nodeId) throws AndesException {
		//Not necessary in RDBMS mode, because RDBMS mode uses single table to store information
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateOverlappedSlots(String nodeId, String queueName, TreeSet<Slot> overlappedSlots) throws AndesException {
		for(Slot slot: overlappedSlots) {
			this.setSlotState(slot.getStartMessageId(), slot.getEndMessageId(), SlotState.OVERLAPPED);
		}
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public Set<String> getAllQueues() throws AndesException{
		return andesContextStore.getAllQueues();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clearSlotStorage() throws AndesException {
		andesContextStore.clearSlotStorage();
	}
}