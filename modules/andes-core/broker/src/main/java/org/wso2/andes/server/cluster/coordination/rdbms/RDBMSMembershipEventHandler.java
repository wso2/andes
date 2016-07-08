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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.QueueListener;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Class to detect membership changed and to write them into the database.
 */
public class RDBMSMembershipEventHandler {

    private static ScheduledExecutorService clusterMembershipReaderTaskScheduler;

    private MembershipListenerTask membershipListenerTask;

    Logger log = Logger.getLogger(RDBMSMembershipEventHandler.class);

    /**
     * context store object to communicate with the database for the context store.
     */
    AndesContextStore contextStore = AndesContext.getInstance().getAndesContextStore();

    /**
     * My node id in the cluster.
     */
    String myNodeID;

    List<String> clusterNodes;

    public RDBMSMembershipEventHandler(String nodeID) {
        myNodeID = nodeID;
    }

    /**
     * Method to store the event of member removed in the database.
     */
    public void memberRemoved(String nodeID) throws AndesException{
        storeMembershipEvent(MembershipEventType.MEMBER_ADDED.getCode(), nodeID);
    }

    /**
     * Method to store the event of member added in the database.
     */
    public void memberAdded(String nodeID) throws AndesException {
        storeMembershipEvent(MembershipEventType.MEMBER_REMOVED.getCode(), nodeID);
    }

    /**
     * Method to store the event of a coordinator change in the database.
     */
    public void coordinatorChanged(String nodeID) throws AndesException {
        storeMembershipEvent(MembershipEventType.COORDINATOR_CHANGED.getCode(), nodeID);
    }

    /**
     * Method to start the membership listener task.
     */
    public void start() {
        int threadPoolCount = 1;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("ClusterEventReaderTask-%d").build();
        clusterMembershipReaderTaskScheduler = Executors.newScheduledThreadPool(threadPoolCount, namedThreadFactory);
        membershipListenerTask = new MembershipListenerTask();

        //TODO make this configurable
        int scheduledPeriod = 1;
        //        Integer scheduledPeriod = AndesConfigurationManager.readValue(AndesConfiguration.);

        clusterMembershipReaderTaskScheduler.scheduleAtFixedRate(membershipListenerTask, scheduledPeriod,
                scheduledPeriod, TimeUnit.SECONDS);
        log.info("RDBMS cluster event listener started.");
    }

    /**
     * Method to stop the membership listener task.
     */
    public void stop() {

    }

    /**
     * method to store membership event destined to be read by each node.
     *
     * @param membershipEventType the type of the membership event as an int
     * @param nodeID              the node id which triggered the event
     * @throws AndesException
     */
    public void storeMembershipEvent(int membershipEventType, String nodeID) throws AndesException {
        contextStore.storeMembershipEvent(clusterNodes, MembershipEventType.COORDINATOR_CHANGED.getCode(), nodeID);
    }
}
