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
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Class to detect membership changed and to write them into the database.
 */
public class RDBMSMembershipEventingEngine {

    /**
     * Class logger
     */
    private static final Logger logger = Logger.getLogger(RDBMSMembershipEventingEngine.class);

    /**
     * Context store object to communicate with the database for the context store.
     */
    private AndesContextStore contextStore;

    /**
     * Executor service used to run the event listening task
     */
    private static ScheduledExecutorService clusterMembershipReaderTaskScheduler;

    /**
     * Task used to get cluster events
     */
    private MembershipListenerTask membershipListenerTask;

    /**
     * Default constructor
     */
    public RDBMSMembershipEventingEngine() {
        contextStore = AndesContext.getInstance().getAndesContextStore();
    }

    /**
     * Method to start the membership listener task.
     */
    public void start(String nodeId) {

        //TODO: this needs to be fixed
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("ClusterEventReaderTask-%d").build();
        clusterMembershipReaderTaskScheduler = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);

        membershipListenerTask = new MembershipListenerTask(nodeId);

        int scheduledPeriod = AndesConfigurationManager
                .readValue(AndesConfiguration.RDBMS_BASED_EVENT_POLLING_INTERVAL);

        clusterMembershipReaderTaskScheduler.scheduleWithFixedDelay(membershipListenerTask, scheduledPeriod,
                scheduledPeriod, TimeUnit.MILLISECONDS);
        logger.info("RDBMS cluster event listener started.");
    }

    /**
     * Method to stop the membership listener task.
     */
    public void stop() {
        clusterMembershipReaderTaskScheduler.shutdown();
    }

    /**
     * Method to store membership event destined to be read by each node.
     *
     * @param membershipEventType the type of the membership event as an int
     * @param nodeID              the node id which triggered the event
     * @throws AndesException
     */
    public void notifyMembershipEvent(List<String> nodes, MembershipEventType membershipEventType, String nodeID)
            throws AndesException {
        contextStore.storeMembershipEvent(nodes, membershipEventType.getCode(), nodeID);
    }

    /**
     * Add a listener to be notified of the cluster membership events
     *
     * @param membershipListener membership listener object
     */
    public void addEventListener(RDBMSMembershipListener membershipListener) {
        membershipListenerTask.addEventListener(membershipListener);
    }

    /**
     * Remove a previously added listener
     *
     * @param membershipListener membership listener object
     */
    public void removeEventListener(RDBMSMembershipListener membershipListener) {
        membershipListenerTask.removeEventListener(membershipListener);
    }
}
