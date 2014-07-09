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

import static org.wso2.andes.messageStore.CassandraConstants.CLUSTER_KEY;
import static org.wso2.andes.messageStore.CassandraConstants.CONNECTION_STRING;
import static org.wso2.andes.messageStore.CassandraConstants.KEYSPACE;
import static org.wso2.andes.messageStore.CassandraConstants.PASSWORD_KEY;
import static org.wso2.andes.messageStore.CassandraConstants.READ_CONSISTENCY_LEVEL;
import static org.wso2.andes.messageStore.CassandraConstants.REPLICATION_FACTOR;
import static org.wso2.andes.messageStore.CassandraConstants.STRATERGY_CLASS;
import static org.wso2.andes.messageStore.CassandraConstants.USERNAME_KEY;
import static org.wso2.andes.messageStore.CassandraConstants.WRITE_CONSISTENCY_LEVEL;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.dao.GenericCQLDAO;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.GlobalQueueManager;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;
import org.wso2.andes.server.store.util.CQLDataAccessHelper.ClusterConfiguration;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.server.util.AndesUtils;

import com.datastax.driver.core.Cluster;

public class CQLConnection implements DurableStoreConnection {

    private Cluster cluster;
    private static Log log = LogFactory.getLog(CQLConnection.class);
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

            int port = 9042;
            boolean isExternalCassandraServerRequired = ClusterResourceHolder.getInstance().
                    getClusterConfiguration().getIsExternalCassandraserverRequired();
            
            List<String> hosts = new ArrayList<String>();

            if (connections instanceof ArrayList && isExternalCassandraServerRequired) {
                List<String> cons = (ArrayList<String>) connections;
                for(String connection : cons) {
                   String host = connection.split(":")[0];
                    port = Integer.parseInt(connection.split(":")[1]);
                    hosts.add(host);
                }

            } else if (connections instanceof String && isExternalCassandraServerRequired) {
                String connectionString = (String) connections;
                if (connectionString.indexOf(":") > 0) {
                    String host = connectionString.split(":")[0];
                    hosts.add(host);
                    port = Integer.parseInt(connectionString.split(":")[1]);
                }
            }  else {
                String defaultHost = "localhost";
                int defaultPort = AndesUtils.getInstance().getCassandraPort();
                port = defaultPort;
                hosts.add(defaultHost);
            }

            String clusterName = (String) configuration.getProperty(CLUSTER_KEY);
            ClusterConfiguration clusterConfig = new ClusterConfiguration(userName, password, clusterName, hosts , port);

            log.info("Initializing Cassandra Message Store: HOSTS=" + hosts + " PORT=" + port);

            cluster = CQLDataAccessHelper.createCluster(clusterConfig);
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
            checkCassandraConnection();

        } catch (CassandraDataAccessException e) {
            log.error("Cannot Initialize Cassandra Connection", e);
            throw new AndesException(e);
        }
    }

    @Override
    public void close() {
        stopTasks();
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
        CQLDataAccessHelper.createKeySpace(cluster,GenericCQLDAO.CLUSTER_SESSION,  KEYSPACE, replicationFactor, strategyClass);
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
            if (MessagingEngine.getInstance().getCassandraBasedMessageStore() != null
                    && MessagingEngine.getInstance().getSubscriptionStore() != null) {

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
                        if (CQLDataAccessHelper.isKeySpaceExist(KEYSPACE) ) {
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


}
