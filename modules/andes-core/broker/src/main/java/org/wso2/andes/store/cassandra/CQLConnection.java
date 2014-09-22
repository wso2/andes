/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.andes.store.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.store.cassandra.dao.GenericCQLDAO;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.GlobalQueueManager;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import com.datastax.driver.core.Cluster;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import static org.wso2.andes.store.cassandra.CassandraConstants.*;

public class CQLConnection implements DurableStoreConnection {

    private Cluster cluster;
    private static Log log = LogFactory.getLog(CQLConnection.class);
    private boolean isCassandraConnectionLive = false;
    private int gcGraceSeconds;
    private final static int DEFAULT_GC_GRACE_SECOND_VALUE = 864000;


    @Override
    public void initialize(String jndiLookupName) throws AndesException {
        try {
            // todo these configs should be read from data source config file
            int replicationFactor = 1;
            String strategyClass = "org.apache.cassandra.locator.SimpleStrategy";
            String readConsistancyLevel = "QUORUM";
            String writeConsistancyLevel = "QUORUM";
            String gcGraceSecondsString = "864000";
            setGcGraceSeconds(Integer.parseInt(gcGraceSecondsString));

            cluster = InitialContext.doLookup(jndiLookupName);

            GenericCQLDAO.setCluster(cluster);
            createKeySpace(replicationFactor, strategyClass);

            /*ConfigurableConsistencyLevel configurableConsistencyLevel = new ConfigurableConsistencyLevel();
            if (readConsistancyLevel == null || readConsistancyLevel.isEmpty()) {
                configurableConsistencyLevel.setDefaultReadConsistencyLevel(HConsistencyLevel.QUORUM);
            } else {
                configurableConsistencyLevel.setDefaultReadConsistencyLevel(HConsistencyLevel.valueOf(readConsistancyLevel));
            }
            if (writeConsistancyLevel == null || writeConsistancyLevel.isEmpty()) {
                configurableConsistencyLevel.setDefaultWriteConsistencyLevel(HConsistencyLevel.QUORUM);
            } else {
                configurableConsistencyLevel.setDefaultWriteConsistencyLevel(HConsistencyLevel.valueOf(writeConsistancyLevel));
            }

            keyspace.setConsistencyLevelPolicy(configurableConsistencyLevel);
*/
            //start Cassandra connection live check
            isCassandraConnectionLive = true;
            checkCassandraConnection();

        } catch (NamingException e) {
            throw new AndesException("Couldn't look up jndi entry for " +
                    "\"" + jndiLookupName + "\"" + e);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Cannot Initialize Cassandra Connection", e);
        }

    }

    @Override
    public void close() {
        stopTasks();
        //TODO: hasitha - this is not logical. Need to fix
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            //silently ignore
        }
        cluster.shutdown();
    }

    @Override
    public boolean isLive() {
        return isCassandraConnectionLive;
    }

    @Override
    public Object getConnection() {
        return this;
    }

    private void createKeySpace(int replicationFactor, String strategyClass) throws CassandraDataAccessException {
        CQLDataAccessHelper.createKeySpace(cluster, GenericCQLDAO.CLUSTER_SESSION, KEYSPACE, replicationFactor, strategyClass);
    }

    public Cluster getCluster() {
        return cluster;
    }


    //TODO:better way is to use a listener

    /**
     * start all background threads accessing durable store
     */
    private void startTasks() {
        //TODO: Hasitha - review what to start
        try {
            if (MessagingEngine.getInstance().getDurableMessageStore() != null
                    && AndesContext.getInstance().getSubscriptionStore() != null) {

                MessagingEngine.getInstance().startMessageDelivery();

                ClusterManager cm = ClusterResourceHolder.getInstance().getClusterManager();

                if (cm != null) {
                    GlobalQueueManager gqm = cm.getGlobalQueueManager();
                    if (gqm != null) {
                        log.info("Starting all global queue workers locally");
                        gqm.startAllQueueWorkersLocally();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error while starting broker tasks back. Not retrying...", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * stop all background threads accessing durable store
     */
    private void stopTasks() {
        //TODO: Hasitha - review what to stop

        MessagingEngine.getInstance().stopMessageDelivery();

        ClusterManager cm = ClusterResourceHolder.getInstance().getClusterManager();
        if (cm != null) {
            GlobalQueueManager gqm = cm.getGlobalQueueManager();
            if (gqm != null) {
                log.info("Stopping all global queue workers locally");
                gqm.stopAllQueueWorkersLocally();
            }
        }
    }

    /**
     * exponential backoff thread to
     * check if cassandra connection is live
     */
    private void checkCassandraConnection() {
        Thread cassandraConnectionCheckerThread = new Thread(new Runnable() {
            public void run() {
                int retriedCount = 0;
                while (true) {
                    try {
                        if (CQLDataAccessHelper.isKeySpaceExist(KEYSPACE)) {
                            boolean previousState = isCassandraConnectionLive;
                            isCassandraConnectionLive = true;
                            retriedCount = 0;
                            if (previousState == false) {
                                //start back all tasks accessing cassandra
                                log.info("Cassandra Message Store is alive....");
                                startTasks();
                            }
                            Thread.sleep(10000);
                        }
                    } catch (CassandraDataAccessException e) {
                        try {
                            if (e.getMessage().contains("All host pools marked down. Retry burden pushed out to client")) {

                                isCassandraConnectionLive = false;
                                //print the error log several times
                                if (retriedCount < 5) {
                                    log.error(e);
                                }
                                retriedCount += 1;
                                if (retriedCount == 4) {
                                    //stop all tasks accessing  Cassandra
                                    log.error("Cassandra Message Store is Inaccessible....");
                                    stopTasks();
                                }
                                log.info("Waiting for Cassandra connection configured to become live...");

                                if (retriedCount <= 10) {
                                    Thread.sleep(6000);
                                } else {
                                    if (retriedCount == 120) {
                                        retriedCount = 10;
                                    }
                                    Thread.sleep(500 * retriedCount);
                                }
                            }
                        } catch (InterruptedException ex) {
                            //silently ignore
                        } catch (Exception ex) {
                            log.error("Error while checking if Cassandra Connection is alive.", ex);
                        }
                    } catch (InterruptedException e) {
                        //silently ignore
                    } catch (Exception e) {
                        log.error("Error while checking if Cassandra Connection is alive.", e);
                    }
                }
            }
        });
        cassandraConnectionCheckerThread.start();
    }


    public int getGcGraceSeconds() {
        return gcGraceSeconds;
    }

    public void setGcGraceSeconds(int gcGraceSeconds) {
        this.gcGraceSeconds = gcGraceSeconds;
    }
}
