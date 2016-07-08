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
     * Listeners to listen to membership changes.
     */
    private RDBMSMembershipListener membershipListener;

    /**
     * Logger to log information.
     */
    Logger log = Logger.getLogger(MembershipListenerTask.class);

    /**
     * context store object to communicate with the database for the context store.
     */
    AndesContextStore contextStore = AndesContext.getInstance().getAndesContextStore();

    /**
     * the node id of the node for which the reader reads memebr changes.
     */
    String nodeID;

    /**
     * The task that is periodically run to read membership events and to notify the listeners.
     */
    @Override
    public void run() {

        try {
            // Read the membership changes from the store and notify the changes
            List<MembershipEvent> membershipEvents = readMembershipEvents(nodeID);

            if (!membershipEvents.isEmpty()) {
                for (MembershipEvent event : membershipEvents) {
                    switch (event.getMembershipEventType()) {
                        case MEMBER_ADDED:
                            membershipListener.memberAdded(event.getMember());
                            break;
                        case MEMBER_REMOVED:
                            membershipListener.memberRemoved(event.getMember());
                            break;
                        case COORDINATOR_CHANGED:
                            membershipListener.coordinationChanged(event.getMember());
                            break;
                        default:
                            log.error("Unknown cluster event type: " + event.getMembershipEventType());
                            break;
                    }
                }
            } else {
                log.info("No membership events to sync");
            }
        } catch (AndesException e) {
            e.printStackTrace();
        }
    }

    /**
     * Set a listener to listen to the cluster membership changes stored.
     *
     * @param listener Membership lister which acts upon member changes
     * @param nodeId   node id to which the listener listens to
     */
    public void setMembershipListener(RDBMSMembershipListener listener, String nodeId) {
        this.nodeID = nodeId;
        membershipListener = listener;
    }

    /**
     * Method to read membership events.
     * <p/>
     * This will read all membership events that a are recorded for a particular node and clear all of those once read.
     *
     * @param nodeID
     * @return
     * @throws AndesException
     */
    private List<MembershipEvent> readMembershipEvents(String nodeID) throws AndesException {
        List<MembershipEvent> events = contextStore.readMemberShipEvents(nodeID);
        contextStore.clearMembershipEvents(nodeID);
        return events;
    }
}
