/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 *
 */

package org.wso2.andes.server.cluster.coordination.rdbms;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;

import java.util.ArrayList;
import java.util.List;

/**
 * The task that runs periodically to detect membership change events.
 */
class MembershipListenerTask implements Runnable {

    /**
     * Class logger
     */
    private static final Logger logger = Logger.getLogger(MembershipListenerTask.class);

    /**
     * Context store object to communicate with the database for the context store.
     */
    private AndesContextStore contextStore = AndesContext.getInstance().getAndesContextStore();

    /**
     * Node id of the node for which the reader reads member changes.
     */
    private String nodeID;

    /**
     * List used to hold all the registered subscribers
     */
    private List<RDBMSMembershipListener> listeners;

    /**
     * Default Constructor
     *
     * @param nodeId Local node ID used to uniquely identify the node within cluster
     */
    MembershipListenerTask(String nodeId) {
        this.nodeID = nodeId;
        listeners = new ArrayList<>();
    }

    /**
     * The task that is periodically run to read membership events and to notify the listeners.
     */
    @Override
    public void run() {

        try {
            // Read the membership changes from the store and notify the changes
            List<MembershipEvent> membershipEvents = readMembershipEvents();

            if (!membershipEvents.isEmpty()) {
                for (MembershipEvent event : membershipEvents) {
                    switch (event.getMembershipEventType()) {
                    case MEMBER_ADDED:
                        notifyMemberAddition(event.getMember());
                        break;
                    case MEMBER_REMOVED:
                        notifyMemberRemoval(event.getMember());
                        break;
                    case COORDINATOR_CHANGED:
                        notifyCoordinatorChangeEvent(event.getMember());
                        break;
                    default:
                        logger.error("Unknown cluster event type: " + event.getMembershipEventType());
                        break;
                    }
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("No membership events to sync");
                }
            }
        } catch (Throwable e) {
            logger.warn("Error occurred while reading membership events.", e);
        }
    }

    private void notifyCoordinatorChangeEvent(String member) {
        for (RDBMSMembershipListener listener : listeners) {
            listener.coordinatorChanged(member);
        }
    }

    private void notifyMemberRemoval(String member) {
        for (RDBMSMembershipListener listener : listeners) {
            listener.memberRemoved(member);
        }
    }

    private void notifyMemberAddition(String member) {
        for (RDBMSMembershipListener listener : listeners) {
            listener.memberAdded(member);
        }
    }

    /**
     * Method to read membership events.

     * <p>This will read all membership events that a are recorded for a particular node and clear all of those once
     * read.
     *
     * @return list membership events
     * @throws AndesException
     */
    private List<MembershipEvent> readMembershipEvents() throws AndesException {
        List<MembershipEvent> events = contextStore.readMemberShipEvents(nodeID);

        return events;
    }

    /**
     * Add a listener to be notified of the cluster membership events
     *
     * @param membershipListener membership listener object
     */
    void addEventListener(RDBMSMembershipListener membershipListener) {
        listeners.add(membershipListener);
    }

    /**
     * Remove a previously added listener
     *
     * @param membershipListener membership listener object
     */
    void removeEventListener(RDBMSMembershipListener membershipListener) {
        listeners.remove(membershipListener);
    }
}
