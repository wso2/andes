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
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
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

/**
 * Class which holds information relates to a specific {@link Xid} within the broker
 */
public class DtxBranch implements AndesInboundStateEvent {

    /**
     * Class logger
     */
    private static final Logger LOGGER = Logger.getLogger(DtxBranch.class);

    /**
     * Internal XID has this value when the branch is not in prepared state
     */
    private static final int NULL_XID = -1;

    /**
     * XID used to identify the dtx branch by external parties
     */
    private final Xid xid;

    /**
     * Registry used to keep dtx branch related information
     */
    private final DtxRegistry dtxRegistry;

    /**
     * List of associated sessions to this branch
     */
    private Map<Long, State> associatedSessions = new HashMap<>();

    /**
     * Event manager used to publish commit event
     */
    private InboundEventManager eventManager;

    /**
     * Keep callback to be called after committing
     */
    private DisruptorEventCallback callback;

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
     * Keep the list of messages that need to be published when the transaction commits
     */
    private ArrayList<AndesMessage> enqueueList = new ArrayList<>();

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

    /**
     * Default constructor
     *
     * @param sessionID    session ID of originating session
     * @param xid          XID used to identify the dtx branch by external parties
     * @param dtxRegistry  registry used to keep dtx branch related information
     * @param eventManager event manager used to publish commit event
     */
    DtxBranch(long sessionID, Xid xid, DtxRegistry dtxRegistry, InboundEventManager eventManager) {
        this.xid = xid;
        this.dtxRegistry = dtxRegistry;
        this.eventManager = eventManager;
        this.createdSessionId = sessionID;
        andesApi = Andes.getInstance();
    }

    /**
     * Getter for enqueueList
     */
    public ArrayList<AndesMessage> getEnqueueList() {
        return enqueueList;
    }

    /**
     * Getter for dequeueList
     */
    public List<AndesAckData> getDequeueList() {
        return dequeueList;
    }

    /**
     * Getter for XID
     *
     * @return XID of the branch
     */
    public Xid getXid() {
        return xid;
    }

    /**
     * Associate a session to current branch.
     *
     * @param sessionID session identifier of the session
     * @return True if a new entry, False otherwise
     */
    boolean associateSession(long sessionID) {
        return associatedSessions.put(sessionID, State.ACTIVE) != null;
    }

    /**
     * Disassociate the given session from the branch
     *
     * @param sessionID session identifier of the session
     * @return True if there is a matching entry, False otherwise
     */
    boolean disassociateSession(long sessionID) {
        return associatedSessions.remove(sessionID) != null;
    }

    /**
     * Resume a session if it is suspended
     *
     * @param sessionID session identifier of the session
     * @return True if there is a matching suspended entry
     */
    boolean resumeSession(long sessionID) {
        if (associatedSessions.containsKey(sessionID) && associatedSessions.get(sessionID) == State.SUSPENDED) {
            associatedSessions.put(sessionID, State.ACTIVE);
            return true;
        }
        return false;
    }

    /**
     * Check if a session is associated with the branch
     *
     * @param sessionId session identifier of the session
     * @return True is the session is associated with the branch
     */
    boolean isAssociated(long sessionId) {
        return associatedSessions.containsKey(sessionId);
    }

    /**
     * Id of the session which created the branch
     * @return session id
     */
    long getCreatedSessionId() {
        return createdSessionId;
    }

    /**
     * Suspend a associated active session
     *
     * @param sessionId session identifier of the session
     * @return True if a matching active sessions if found, False otherwise
     */
    boolean suspendSession(long sessionId) {
        State state = associatedSessions.get(sessionId);
        if (null != state && state == State.ACTIVE) {
            associatedSessions.put(sessionId, State.SUSPENDED);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Enqueue a single message to the branch
     * @param andesMessage enqueue record
     */
    public void enqueueMessage(AndesMessage andesMessage) {
        enqueueList.add(andesMessage);
    }

    /**
     * Enqueue a list of messages to the branch
     * @param messagesList list of enqueue records
     */
    public void enqueueMessages(Collection<AndesMessage> messagesList) {
        enqueueList.addAll(messagesList);
    }

    /**
     * Check if the branch has active associated sessions
     *
     * @return True if there are active associated sessions
     */
    boolean hasAssociatedActiveSessions() {
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
    boolean hasAssociatedSessions() {
        return !associatedSessions.isEmpty();
    }

    /**
     * Clear all association from branch
     */
    void clearAssociations() {
        associatedSessions.clear();
    }

    /**
     * Check if the branch is expired
     * @return True if the branch is expired, False otherwise
     */
    public boolean expired() {
        return (timeout != 0 && _expiration < System.currentTimeMillis()) || state == State.TIMED_OUT;
    }

    /**
     * Get current state of the branch
     * @return state of the branch
     */
    public State getState() {
        return state;
    }

    /**
     * Persist enqueue and dequeue records
     * @throws AndesException if an internal error occured
     */
    public void prepare() throws AndesException {
        LOGGER.debug("Performing prepare for DtxBranch {}" + xid);
        internalXid = dtxRegistry.storeRecords(xid, enqueueList, dequeueList);
    }

    /**
     * Set the state of the branch
     * @param state new state to be set
     */
    public void setState(State state) {
        this.state = state;
    }

    /**
     * Add a list of dequeue records to the branch
     *
     * @param ackList list of dequeue records
     */
    void dequeueMessages(List<AndesAckData> ackList) {
        dequeueList.addAll(ackList);
    }

    /***
     * Cancel timeout task and remove corresponding enqueue and dequeue records from the dtx registry.
     * @throws AndesException if an internal error occured
     */
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
     * @throws AndesException if an internal error occured
     */
    public void commit(DisruptorEventCallback callback, AndesChannel channel) throws AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Performing commit for DtxBranch " + xid);
        }

        cancelTimeoutTaskIfExists();

        this.callback = callback;
        eventManager.requestDtxCommitEvent(this, channel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateState() throws AndesException {
        try {
            SlotMessageCounter.getInstance().recordMetadataCountInSlot(enqueueList);
            enqueueList.clear();
            dequeueList.clear();
            callback.execute();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Dtx commit messages state updated. Internal Xid " + internalXid);
            }

        } catch (Exception exception) {
            callback.onException(exception);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String eventInfo() {
        return null;
    }

    /**
     * Clear the list of enqueu records
     */
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
