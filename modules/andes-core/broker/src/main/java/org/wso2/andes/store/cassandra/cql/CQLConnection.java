/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 */

package org.wso2.andes.store.cassandra.cql;

import com.datastax.driver.core.Cluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.store.cassandra.CassandraConstants;
import org.wso2.andes.store.cassandra.cql.dao.GenericCQLDAO;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import static org.wso2.andes.store.cassandra.CassandraConstants.KEYSPACE;

public class CQLConnection implements DurableStoreConnection {

    private Cluster cluster;
    private static Log log = LogFactory.getLog(CQLConnection.class);
    private boolean isCassandraConnectionLive = false;
    private int gcGraceSeconds;

    @Override
    public void initialize(ConfigurationProperties connectionProperties) throws AndesException {
        String jndiLookupName = "";
        try {

            jndiLookupName = connectionProperties.getProperty(CassandraConstants
                                                                      .PROP_JNDI_LOOKUP_NAME);

            String replicationFactor = connectionProperties.getProperty(CassandraConstants
                                                                                .PROP_REPLICATION_FACTOR);
            if(replicationFactor.isEmpty()){
                replicationFactor = CassandraConstants.DEFAULT_REPLICATION_FACTOR;
            }
            String strategyClass = connectionProperties.getProperty(CassandraConstants
                                                                            .PROP_STRATEGY_CLASS);

            if (strategyClass.isEmpty()){
                strategyClass = CassandraConstants.DEFAULT_STRATEGY_CLASS;
            }
            String readConsistencyLevel = connectionProperties.getProperty(CassandraConstants.PROP_READ_CONSISTENCY);
            if (readConsistencyLevel.isEmpty()) {
                readConsistencyLevel = CassandraConstants.DEFAULT_READ_CONSISTENCY;
            }

            String writeConsistencyLevel = connectionProperties.getProperty(CassandraConstants
                                                                                    .PROP_WRITE_CONSISTENCY);
            if(writeConsistencyLevel.isEmpty()) {
                writeConsistencyLevel = CassandraConstants.DEFAULT_WRITE_CONSISTENCY;
            }

            String gcGraceSeconds = connectionProperties.getProperty(CassandraConstants
                                                                                   .PROP_GC_GRACE_SECONDS);

            if(gcGraceSeconds.isEmpty()) {
                gcGraceSeconds = CassandraConstants.DEFAULT_GC_GRACE_SECONDS;
            }

            setGcGraceSeconds(Integer.parseInt(gcGraceSeconds));
            //todo remove this if elsee condtions after testing
            if (cluster == null) {
                cluster = InitialContext.doLookup(jndiLookupName);
            }

            //set consistency levels
            GenericCQLDAO.setReadConsistencyLevel(readConsistencyLevel);
            GenericCQLDAO.setWriteConsistencyLevel(writeConsistencyLevel);

            GenericCQLDAO.setCluster(cluster);
            createKeySpace(Integer.parseInt(replicationFactor), strategyClass);


            //start Cassandra connection live check
            isCassandraConnectionLive = true;
            checkCassandraConnection();

        } catch (NamingException e) {
            throw new AndesException("Couldn't look up jndi entry for " +
                                     "\"" + jndiLookupName + "\"", e);
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
        cluster.close();
    }

    @Override
    public boolean isLive() {
        return isCassandraConnectionLive;
    }

    @Override
    public Object getConnection() {
        return this;
    }

    private void createKeySpace(int replicationFactor, String strategyClass)
            throws CassandraDataAccessException {
        CQLDataAccessHelper
                .createKeySpace(cluster, GenericCQLDAO.CLUSTER_SESSION, KEYSPACE, replicationFactor,
                                strategyClass);
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
            Andes.getInstance().startMessageDelivery();

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

        Andes.getInstance().stopMessageDelivery();
    }

    /**
     * exponential backoff thread to check if cassandra connection is live
     */
    private void checkCassandraConnection() {
        Thread cassandraConnectionCheckerThread = new Thread(new Runnable() {
            public void run() {
                int retriedCount = 0;
                while (true) {
                    try {
                        boolean previousState = isCassandraConnectionLive;
                        if (!CQLDataAccessHelper.isKeySpaceExist(KEYSPACE)) {
                            //if DB was cleared, create key space etc
                            AndesKernelBoot.reInitializeAndesStores();
                        }
                        //if there was no exception connection is live
                        isCassandraConnectionLive = true;
                        retriedCount = 0;
                        if (previousState == false) {
                            //start back all tasks accessing cassandra
                            log.info("Cassandra Message Store is alive");
                            startTasks();
                        }
                        Thread.sleep(10000);
                    } catch (CassandraDataAccessException e) {
                        try {
                            if (e.getCause().getMessage().contains(
                                    "All host(s) tried for query failed")) {

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
                                log.info(
                                        "Waiting for Cassandra connection configured to become " +
                                        "live...");

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

    /**
     *
     * @return gcGraceSeconds
     */
    public int getGcGraceSeconds() {
        return gcGraceSeconds;
    }

    /**
     *set gcGraceSeconds which is an upper bound on the amount of time the cluster had to
     * propagate tombstones.
     * @param gcGraceSeconds
     */
    public void setGcGraceSeconds(int gcGraceSeconds) {
        this.gcGraceSeconds = gcGraceSeconds;
    }

    public void setCluster(Cluster cluster){
        this.cluster = cluster;
    }
}
