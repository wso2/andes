/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel.dtx;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.disruptor.inbound.AndesInboundStateEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.slot.SlotMessageCounter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.transaction.xa.Xid;

public class DtxBranch implements AndesInboundStateEvent {

    /**
     * Class logger
     */
    private static final Logger LOGGER = Logger.getLogger(DtxBranch.class);

    /**
     * Internal XID has this value when the branch is not in prepared state
     */
    public static final int NULL_XID = -1;

    private final Xid xid;
    private final DtxRegistry dtxRegistry;
    private Map<Long, State> associatedSessions = new HashMap<>();
    private InboundEventManager eventManager;
    private Runnable callback;

    /**
     * Getter for enqueueList
     */
    public ArrayList<AndesMessage> getEnqueueList() {
        return enqueueList;
    }

    /**
     * Keep the list of messages that need to be published when the transaction commits
     */
    private ArrayList<AndesMessage> enqueueList = new ArrayList<>();

    /**
     * Getter for dequeueList
     */
    public List<AndesAckData> getDequeueList() {
        return dequeueList;
    }

    /**
     * Keep the list of messages that need to be acked when the transaction commits
     */
    private List<AndesAckData> dequeueList = new ArrayList<>();

    /**
     * Current branch state
     */
    private State state = State.ACTIVE;

    /**
     * Used to keep the internal xid value used in message store
     */
    private long internalXid = NULL_XID;

    public DtxBranch(Xid xid, DtxRegistry dtxRegistry, InboundEventManager eventManager) {
        this.xid = xid;
        this.dtxRegistry = dtxRegistry;
        this.eventManager = eventManager;
    }

    public Xid getXid() {
        return xid;
    }

    public boolean associateSession(long sessionID) {
        return associatedSessions.put(sessionID, State.ACTIVE) != null;
    }

    public boolean disassociateSession(long sessionID) {
        return associatedSessions.remove(sessionID) != null;
    }

    public boolean resumeSession(long sessionID) {
        if (associatedSessions.containsKey(sessionID) && associatedSessions.get(sessionID) == State.SUSPENDED) {
            associatedSessions.put(sessionID, State.ACTIVE);
            return true;
        }
        return false;
    }

    public boolean isAssociated(long sessionId) {
        return associatedSessions.containsKey(sessionId);
    }

    public boolean suspendSession(long sessionId) {
        State state = associatedSessions.get(sessionId);
        if (null != state && state == State.ACTIVE) {
            associatedSessions.put(sessionId, State.SUSPENDED);
            return true;
        } else {
            return false;
        }
    }

    public boolean markAsFailedSession(long sessionId) {
        State state = associatedSessions.get(sessionId);
        if (null != state && state == State.ACTIVE) {
            associatedSessions.put(sessionId, State.ROLLBACK_ONLY);
            return true;
        } else {
            return false;
        }
    }

    public void enqueueMessage(AndesMessage andesMessage) {
        enqueueList.add(andesMessage);
    }

    public void enqueueMessages(Collection<AndesMessage> messagesList) {
        enqueueList.addAll(messagesList);
    }

    public boolean hasAssociatedActiveSessions() {
        if (hasAssociatedSessions()) {
            for (State state : associatedSessions.values()) {
                if (state != State.SUSPENDED) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasAssociatedSessions() {
        return !associatedSessions.isEmpty();
    }

    public void clearAssociations() {
        associatedSessions.clear();
    }

    public boolean expired() {
        // TODO implement transaction timeouts
        return false;
    }

    public State getState() {
        return state;
    }

    public void prepare() throws AndesException {
        LOGGER.debug("Performing prepare for DtxBranch {}" + xid);
        internalXid = dtxRegistry.storeRecords(xid, enqueueList, dequeueList);
    }

    public void setState(State state) {
        this.state = state;
    }

    public void dequeueMessages(List<AndesAckData> ackList) {
        dequeueList.addAll(ackList);
    }

    public void rollback() throws AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Performing rollback for DtxBranch {}" + xid);
        }

        if (internalXid != NULL_XID) {
            dtxRegistry.removePreparedRecords(internalXid);
            internalXid = NULL_XID;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Cannot rollback since could not find a internal XID " + "{}" + xid);
            }
        }
    }

    public void commit(Runnable callback, AndesChannel channel) throws AndesException {
        this.callback = callback;
        eventManager.requestDtxCommitEvent(this, channel);
    }

    @Override
    public void updateState() throws AndesException {
        SlotMessageCounter.getInstance().recordMetadataCountInSlot(enqueueList);
        enqueueList.clear();
        dequeueList.clear();
        callback.run();

        LOGGER.debug("Dtx commit messages state updated. Internal Xid " + internalXid);
        // TODO: Handle exceptions
    }

    @Override
    public String eventInfo() {
        return null;
    }

    public void clearEnqueueList() {
        enqueueList.clear();
    }

    /**
     * Getter for internalXid
     */
    public long getInternalXid() {
        return internalXid;
    }

    public enum State {
        SUSPENDED, ACTIVE, ROLLBACK_ONLY, PREPARED, FORGOTTEN, TIMED_OUT;
    }
}
