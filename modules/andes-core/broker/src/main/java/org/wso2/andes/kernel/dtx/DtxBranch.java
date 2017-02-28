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
import org.wso2.andes.kernel.disruptor.DisruptorEventCallback;
import org.wso2.andes.kernel.disruptor.inbound.AndesInboundStateEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventContainer;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.slot.SlotMessageCounter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import javax.transaction.xa.Xid;

/**
 * Class which holds information relates to a specific {@link Xid} within the broker
 */
public class DtxBranch {

    /**
     * Class logger
     */
    private static final Logger LOGGER = Logger.getLogger(DtxBranch.class);

    /**
     * Internal XID has this value when the branch is not in prepared state
     */
    public static final int NULL_XID = -1;

    /**
     * Session Id of a session that is recovered from storage
     */
    public static final int RECOVERY_SESSION_ID = -1;

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
     * Messages that were dequeued within a the transaction branch which is in prepared state.
     */
    private List<AndesPreparedMessageMetadata> preparedDequeueMessages;

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
     * Reference to the specific command that needs to be executed when updateState method is invoked by the
     * {@link org.wso2.andes.kernel.disruptor.inbound.StateEventHandler}
     */
    private Runnable updateSlotCommand;

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
    }

    /**
     * Getter for enqueueList
     */
    public ArrayList<AndesMessage> getEnqueueList() {
        return enqueueList;
    }

    /**
     * Set the messages that were acknowledged but not commited withing the transaction that needs to be restored when
     * rollback is called.
     * @param messagesToRestore {@link List} of {@link AndesPreparedMessageMetadata}
     */
    public void setMessagesToRestore(List<AndesPreparedMessageMetadata> messagesToRestore) {
        dequeueList.clear();
        this.preparedDequeueMessages = messagesToRestore;
    }

    /**
     * Set the messages to be stored in Database
     * @param messagesToStore {@link Collection} of {@link AndesMessage}
     */
    public void setMessagesToStore(Collection<AndesMessage> messagesToStore) {
        this.enqueueList.clear();
        this.enqueueList.addAll(messagesToStore);
    }

    void clearEnqueueList() {
        enqueueList.clear();
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
        internalXid = dtxRegistry.storeRecords(this);
    }

    /**
     * Retrieve the messages that need to be restored on a rollback event
     *
     * @return List of {@link AndesPreparedMessageMetadata}
     */
    public List<AndesPreparedMessageMetadata> getMessagesToRestore() {
        return Collections.unmodifiableList(preparedDequeueMessages);
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
     *
     * @param callback {@link DisruptorEventCallback}
     * @throws AndesException if an internal error occurred
     */
    public synchronized void rollback(DisruptorEventCallback callback) throws AndesException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Performing rollback for DtxBranch {}" + xid);
        }
        this.callback = callback;
        cancelTimeoutTaskIfExists();

        if (internalXid != NULL_XID) {
            updateSlotCommand = new DtxRollbackCommand();
            eventManager.requestDtxEvent(this, null, InboundEventContainer.Type.DTX_ROLLBACK_EVENT);
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Cannot rollback since could not find a internal XID " + "{}" + xid);
            }
            callback.execute();
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
        updateSlotCommand = new DtxCommitCommand();
        eventManager.requestDtxEvent(this, channel, InboundEventContainer.Type.DTX_COMMIT_EVENT);
    }

    /**
     * Write the committed messages to the database
     *
     * @throws AndesException throws AndesException on database error
     */
    public void writeToDbOnCommit() throws AndesException {
        dtxRegistry.getStore().updateOnCommit(internalXid, enqueueList);
    }

    /**
     * Update the state of the transaction and respond to the client on dtx.commt and dtx.rollback events
     *
     * @param event {@link InboundEventContainer}
     * @throws AndesException
     */
    public void updateState(InboundEventContainer event) throws AndesException {
        if (event.hasErrorOccurred()) {
            callback.onException(new Exception(event.getError()));
        } else {
            updateSlotCommand.run();
        }
    }

    /**
     *
     * @param messageList
     */
    private void updateSlotAndClearLists(List<AndesMessage> messageList) {
        try {

            SlotMessageCounter.getInstance().recordMetadataCountInSlot(messageList);
            enqueueList.clear();
            dequeueList.clear();
            callback.execute();
            internalXid = NULL_XID;

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Messages state updated. Internal Xid " + internalXid);
            }

        } catch (Exception exception) {
            callback.onException(exception);
        }
    }

    /**
     * Update data store when rollback is called. Restore the acknowledged but not yet committed messages and
     * information regarding the transaction
     *
     * @throws AndesException throws {@link AndesException} when there is an internal data store error.
     */
    public void writeToDbOnRollback() throws AndesException {
        dtxRegistry.getStore().updateOnRollback(internalXid, preparedDequeueMessages);
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

                    setState(State.TIMED_OUT);
                    try {
                        rollback(new DisruptorEventCallback() {
                            @Override
                            public void execute() {
                                // nothing to do be done. This is an internally triggered event
                            }

                            @Override
                            public void onException(Exception exception) {
                                // nothing to be done. This is an internally triggered event
                            }
                        });
                    } catch (AndesException e) {
                        LOGGER.error("Error while rolling back dtx due to store exception");
                    }
                }
            });
        }
    }

    /**
     * Recover branch details from the persistent storage for already prepared transactions
     *
     * @param nodeId node id of the server
     * @return internal xid of the node
     * @throws AndesException throws {@link AndesException} when data storage exception occur
     */
    boolean recoverFromStore(String nodeId) throws AndesException {
        internalXid = dtxRegistry.getStore().recoverBranchData(this, nodeId);
        if (internalXid != NULL_XID) {
            setState(State.PREPARED);
            return true;
        }
        return false;
    }

    /**
     * States of a {@link DtxBranch}
     */
    public enum State {

        /**
         * The branch was suspended in a dtx.end
         */
        SUSPENDED,

        /**
         * Branch is registered in DtxRegistry
         */
        ACTIVE,

        /**
         * Branch can only be rolled back
         */
        ROLLBACK_ONLY,

        /**
         * Branch is in prepared state. Branch can only be committed or rolled back after this
         */
        PREPARED,

        /**
         * Branch was unregistered from DtxRegistry
         */
        FORGOTTEN,

        /**
         * Branch expired
         */
        TIMED_OUT,

        /**
         * Branch heuristically committed
         */
        HEUR_COM,

        /**
         * Branch received a prepare call and in the process of persisting
         */
        PRE_PREPARE,

        /**
         * Branch heuristically rolled back
         */
        HEUR_RB
    }

    /**
     * Command to update slots for Dtx Commit event
     */
    private class DtxCommitCommand implements Runnable {

        @Override
        public void run() {
            updateSlotAndClearLists(enqueueList);
        }
    }

    /**
     * Command to update slots for Dtx Rollback event
     */
    private class DtxRollbackCommand implements Runnable {

        @Override
        public void run() {
            List<AndesMessage> dequeuedMessages = new ArrayList<>();
            for (AndesPreparedMessageMetadata metadata: preparedDequeueMessages) {
                dequeuedMessages.add(new AndesMessage(metadata));
            }
            updateSlotAndClearLists(dequeuedMessages);
        }
    }
 }
