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
import org.wso2.andes.kernel.Andes;
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
import java.util.concurrent.ScheduledFuture;
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
     * Branch's transaction timeout value
     */
    private long timeout;

    /**
     * Future of the scheduled timeout task. Null if no scheduled timeout tasks
     */
    private ScheduledFuture<?> timeoutFuture;

    /**
     * Expiration time calculated from the timeout value.
     */
    private long _expiration;

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

    /**
     * Session id of the session which created the branch
     */
    private long createdSessionId;

    /**
     * Reference to the {@link Andes}
     */
    private Andes andesApi;

    public DtxBranch(long sessionID, Xid xid, DtxRegistry dtxRegistry, InboundEventManager eventManager) {
        this.xid = xid;
        this.dtxRegistry = dtxRegistry;
        this.eventManager = eventManager;
        this.createdSessionId = sessionID;
        andesApi = Andes.getInstance();
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

    /**
     * Id of the session which created the branch
     * @return session id
     */
    public long getCreatedSessionId() {
        return createdSessionId;
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

    /**
     * Check if there are any associated sessions
     *
     * @return True if there are any associated sessions, false otherwise
     */
    public boolean hasAssociatedSessions() {
        return !associatedSessions.isEmpty();
    }

    public void clearAssociations() {
        associatedSessions.clear();
    }

    public boolean expired() {
        return (timeout != 0 && _expiration < System.currentTimeMillis()) || state == State.TIMED_OUT;
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

    public synchronized void rollback() throws AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Performing rollback for DtxBranch {}" + xid);
        }

        cancelTimeoutTaskIfExists();

        if (internalXid != NULL_XID) {
            for (AndesAckData ackData: dequeueList) {
                andesApi.messageRejected(ackData.getAcknowledgedMessage(), ackData.getChannelID());
            }
            dtxRegistry.removePreparedRecords(internalXid);
            internalXid = NULL_XID;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Cannot rollback since could not find a internal XID " + "{}" + xid);
            }
        }
    }

    /**
     * Cancel the timeout tasks if one is already set for the current branch
     */
    private void cancelTimeoutTaskIfExists() {
        if (timeoutFuture != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Attempting to cancel previous timeout task future for DtxBranch " + xid);
            }

            boolean succeeded = timeoutFuture.cancel(false);
            timeoutFuture = null;

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Cancelling previous timeout task {} for DtxBranch " + (succeeded ? "succeeded" : "failed")
                                     + xid);
            }
        }
    }

    /**
     * Commit the changes (enqueue and dequeue actions) to andes core
     *
     * @param callback callback that called after completing the commit task
     * @param channel  corresponding channel object
     * @throws AndesException
     */
    public void commit(Runnable callback, AndesChannel channel) throws AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Performing commit for DtxBranch " + xid);
        }

        cancelTimeoutTaskIfExists();

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

    /**
     * Set the transaction timeout of the current branch and schedule a timeout task.
     *
     * @param timeout transaction timeout value in seconds
     */
    public void setTimeout(long timeout) {
        cancelTimeoutTaskIfExists();

        this.timeout = timeout;
        _expiration = timeout == 0 ? 0 : System.currentTimeMillis() + (1000 * timeout);

        if (timeout == 0) {
            timeoutFuture = null;
        } else {
            long delay = 1000 * timeout;

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Scheduling timeout and rollback after " + timeout + "s for DtxBranch " + xid);
            }

            timeoutFuture = dtxRegistry.scheduleTask(delay, new Runnable() {
                public void run() {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Timing out DtxBranch " + xid);
                    }

                    setState(State.TIMEDOUT);
                    try {
                        rollback();
                    } catch (AndesException e) {
                        LOGGER.error("Error while rolling back dtx due to store exception");
                    }
                }
            });
        }
    }

    /**
     * States of a {@link DtxBranch}
     */
    public enum State {
        SUSPENDED, ACTIVE, ROLLBACK_ONLY, PREPARED, FORGOTTEN, TIMED_OUT, HEUR_COM, TIMEDOUT, HEUR_RB
    }
}
