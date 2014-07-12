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

package org.wso2.andes.server.cassandra;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HectorException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.GlobalQueueManager;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.server.store.util.CassandraDataAccessHelper;
import org.wso2.andes.server.util.AndesUtils;

import java.util.ArrayList;

import static org.wso2.andes.messageStore.CassandraConstants.*;
import static org.wso2.andes.messageStore.CassandraConstants.CLUSTER_KEY;

public class CassandraConnection implements DurableStoreConnection {

    private Cluster cluster;
    private Keyspace keyspace;
    private static Log log = LogFactory.getLog(CassandraConnection.class);
    private boolean isCassandraConnectionLive;

    @Override
    public void initialize(Configuration configuration) throws AndesException {

        try {
            String userName = (String) configuration.getProperty(USERNAME_KEY);
            String password = (String) configuration.getProperty(PASSWORD_KEY);
            Object connections = configuration.getProperty(CONNECTION_STRING);
            int replicationFactor = configuration.getInt(REPLICATION_FACTOR, 1);
            String strategyClass = configuration.getString(STRATERGY_CLASS);
            String readConsistancyLevel = configuration.getString(READ_CONSISTENCY_LEVEL);
            String writeConsistancyLevel = configuration.getString(WRITE_CONSISTENCY_LEVEL);
            String connectionString = "";

            if (connections instanceof ArrayList) {
                ArrayList<String> cons = (ArrayList<String>) connections;

                for (String c : cons) {
                    connectionString += c + ",";
                }
                connectionString = connectionString.substring(0, connectionString.length() - 1);
            } else if (connectionString instanceof String) {
                connectionString = (String) connections;
                if (connectionString.indexOf(":") > 0) {
                    String host = connectionString.substring(0, connectionString.indexOf(":"));
                    int port = AndesUtils.getInstance().getCassandraPort();
                    connectionString = host + ":" + port;
                }
            }
            String clusterName = (String) configuration.getProperty(CLUSTER_KEY);
            cluster = CassandraDataAccessHelper.createCluster(userName, password, clusterName, connectionString);
            createKeySpace(replicationFactor, strategyClass);

            ConfigurableConsistencyLevel configurableConsistencyLevel = new ConfigurableConsistencyLevel();
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

            //start Cassandra connection live check
            checkCassandraConnection();

        } catch (CassandraDataAccessException e) {
            log.error("Cannot Initialize Cassandra Connection", e);
            throw new AndesException(e);
        }
    }

    @Override
    public void close() {
        stopTasks();
    }

    @Override
    public boolean isLive() {
        return isCassandraConnectionLive;
    }

    @Override
    public Object getConnection() {
        return this;
    }

    private void createKeySpace(int replicationFactor, String strategyClass) {
        this.keyspace = CassandraDataAccessHelper.createKeySpace(cluster, KEYSPACE, replicationFactor, strategyClass);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Keyspace getKeySpace() {
        return keyspace;
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

                MessagingEngine.getInstance().startMessageDelivey();

                ClusterManager cm = ClusterResourceHolder.getInstance().getClusterManager();

                if (cm != null) {
                    GlobalQueueManager gqm = cm.getGlobalQueueManager();
                    if (gqm != null) {
                        log.info("Starting all global queue workers locally");
                        gqm.startAllQueueWorkersLocally();
                    }
                }
                if (ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer() != null) {
                    log.info("Starting syncing exchanges, queues and bindings");
                    ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer().start();
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
        if (ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer() != null) {
            log.info("Stopping syncing exchanges, queues and bindings");
            ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer().stop();
        }
    }

    private void checkCassandraConnection() {
        Thread cassandraConnectionCheckerThread = new Thread(new Runnable() {
            public void run() {
                int retriedCount = 0;
                while (true) {
                    try {
                        if (cluster.describeClusterName() != null) {
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
                    } catch (HectorException e) {
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


}
