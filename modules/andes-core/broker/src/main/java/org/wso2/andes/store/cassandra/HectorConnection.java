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

package org.wso2.andes.store.cassandra;

import com.datastax.driver.core.exceptions.UnavailableException;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HectorException;
import org.apache.log4j.Logger;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;

import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Hector based connection to Cassandra
 */
public class HectorConnection implements DurableStoreConnection {

    private static final Logger log = Logger.getLogger(HectorConnection.class);

    /**
     * GC grace seconds to clean Cassandra
     */
    private int gcGraceSeconds;

    /**
     * Cassandra cluster instance
     */
    private Cluster cluster;

    /**
     * Cassandra Keyspace instance
     */
    private Keyspace keyspace;

    /**
     * Flag to get the availability of Cassandra connection.
     */
    private boolean isCassandraConnectionLive = false;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(ConfigurationProperties connectionProperties) throws AndesException {
        String jndiLookupName = "";
        try {

            jndiLookupName = connectionProperties.getProperty(CassandraConstants
                    .PROP_JNDI_LOOKUP_NAME);

            String replicationFactor = connectionProperties.getProperty(CassandraConstants
                    .PROP_REPLICATION_FACTOR);

            if (replicationFactor.isEmpty()) {
                replicationFactor = CassandraConstants.DEFAULT_REPLICATION_FACTOR;
            }

            String strategyClass = connectionProperties.getProperty(CassandraConstants
                    .PROP_STRATEGY_CLASS);

            if (strategyClass.isEmpty()) {
                strategyClass = CassandraConstants.DEFAULT_STRATEGY_CLASS;
            }

	        String readConsistencyLevel = connectionProperties.getProperty(CassandraConstants
			                                                                       .PROP_READ_CONSISTENCY);
	        if (readConsistencyLevel.isEmpty()) {
		        readConsistencyLevel = CassandraConstants.DEFAULT_READ_CONSISTENCY;
	        }

	        String writeConsistencyLevel = connectionProperties.getProperty(CassandraConstants
			                                                                        .PROP_WRITE_CONSISTENCY);
	        if (writeConsistencyLevel.isEmpty()) {
		        writeConsistencyLevel = CassandraConstants.DEFAULT_WRITE_CONSISTENCY;
	        }

            String gcGraceSeconds = connectionProperties.getProperty(CassandraConstants.PROP_GC_GRACE_SECONDS);

            if (gcGraceSeconds.isEmpty()) {
                gcGraceSeconds = CassandraConstants.DEFAULT_GC_GRACE_SECONDS;
            }

            setGcGraceSeconds(Integer.parseInt(gcGraceSeconds));

            if (cluster == null) {
                cluster = InitialContext.doLookup(jndiLookupName);
            }

	        // set consistency levels
	        ConfigurableConsistencyLevel configurableConsistencyLevel = new
			        ConfigurableConsistencyLevel();
	        configurableConsistencyLevel.setDefaultReadConsistencyLevel(HConsistencyLevel.valueOf(
			        readConsistencyLevel));
	        configurableConsistencyLevel.setDefaultWriteConsistencyLevel(HConsistencyLevel.valueOf(
			        writeConsistencyLevel));
	        //create keyspace with consistency levels
	        createKeySpace(Integer.parseInt(replicationFactor), strategyClass,configurableConsistencyLevel);

	        //start Cassandra connection live check
            isCassandraConnectionLive = true;
            checkCassandraConnection();

        } catch (NamingException e) {
            throw new AndesException("Couldn't look up jndi entry for " +
                    "\"" + jndiLookupName + "\"" + e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        stopTasks();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLive() {
        return isCassandraConnectionLive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getConnection() {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * {@inheritDoc}
     */
    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * {@inheritDoc}
     */
    public Keyspace getKeySpace() {
        return keyspace;
    }

    /**
     * {@inheritDoc}
     */
    public void setKeyspace(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * {@inheritDoc}
     */
    public int getGcGraceSeconds() {
        return this.gcGraceSeconds;
    }

    /**
     * Set gcGraceSeconds which is an upper bound on the amount of time the cluster had to
     * propagate tombstones.
     *
     * @param gcGraceSeconds number of seconds
     */
    public void setGcGraceSeconds(int gcGraceSeconds) {
        this.gcGraceSeconds = gcGraceSeconds;
    }

    /**
     * Create a keyspace
     *
     * @param replicationFactor replication factor to configured in keyspace
     * @param strategyClass Cassandra strategy class
     * @param consistencyLevel ConfigurableConsistencyLevel
     */
    private void createKeySpace(int replicationFactor, String strategyClass,
                                ConfigurableConsistencyLevel consistencyLevel) throws AndesException {
        try{
            this.keyspace = HectorDataAccessHelper.createKeySpace(cluster,
                    CassandraConstants.DEFAULT_KEYSPACE,
                    replicationFactor, strategyClass,consistencyLevel);
        }catch(UnavailableException e){
            throw new AndesException("Not enough nodes for replication" + e);
        }
    }

    /**
     * Check the availability of Hector based Cassandra connection
     */
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
                            if (!previousState) {
                                //start back all tasks accessing cassandra
                                log.info("Cassandra Message Store is alive....");
                                startTasks();
                            }
                            Thread.sleep(10000);
                        }
                    } catch (HectorException e) {
                        try {
                            if (e.getMessage().contains("All host pools marked down. " +
                                    "Retry " +
                                    "burden pushed out to client")) {

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

    /**
     * Start all background threads accessing durable store
     */
    private void startTasks() {
        try {
            Andes.getInstance().startMessageDelivery();
        } catch (Exception e) {
            throw new RuntimeException("Error while starting broker tasks back. Not retrying.", e);
        }
    }

    /**
     * Stop all background threads accessing durable store
     */
    private void stopTasks() {
        Andes.getInstance().stopMessageDelivery();
    }
}
