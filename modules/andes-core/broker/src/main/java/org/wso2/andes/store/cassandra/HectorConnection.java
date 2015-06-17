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

import javax.naming.InitialContext;
import javax.naming.NamingException;

import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;

import com.datastax.driver.core.exceptions.UnavailableException;

/**
 * Hector based connection to Cassandra
 */
public class HectorConnection extends DurableStoreConnection {

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
     * {@inheritDoc}
     */
    @Override
    public void initialize(ConfigurationProperties connectionProperties) throws AndesException {
        
        super.initialize(connectionProperties);
        
        String jndiLookupName = "";
        try {

            jndiLookupName = connectionProperties.getProperty(HectorConstants
                    .PROP_JNDI_LOOKUP_NAME);

            String keyspace = connectionProperties.getProperty(HectorConstants.PROP_KEYSPACE);

            if (keyspace.isEmpty()) {
                keyspace = connectionProperties.getProperty(HectorConstants.DEFAULT_KEYSPACE);
            }

            String replicationFactor = connectionProperties.getProperty(HectorConstants
                    .PROP_REPLICATION_FACTOR);

            if (replicationFactor.isEmpty()) {
                replicationFactor = HectorConstants.DEFAULT_REPLICATION_FACTOR;
            }

            String strategyClass = connectionProperties.getProperty(HectorConstants
                    .PROP_STRATEGY_CLASS);

            if (strategyClass.isEmpty()) {
                strategyClass = HectorConstants.DEFAULT_STRATEGY_CLASS;
            }

	        String readConsistencyLevel = connectionProperties.getProperty(HectorConstants
			                                                                       .PROP_READ_CONSISTENCY);
	        if (readConsistencyLevel.isEmpty()) {
		        readConsistencyLevel = HectorConstants.DEFAULT_READ_CONSISTENCY;
	        }

	        String writeConsistencyLevel = connectionProperties.getProperty(HectorConstants
			                                                                        .PROP_WRITE_CONSISTENCY);
	        if (writeConsistencyLevel.isEmpty()) {
		        writeConsistencyLevel = HectorConstants.DEFAULT_WRITE_CONSISTENCY;
	        }

            String gcGraceSeconds = connectionProperties.getProperty(HectorConstants.PROP_GC_GRACE_SECONDS);

            if (gcGraceSeconds.isEmpty()) {
                gcGraceSeconds = HectorConstants.DEFAULT_GC_GRACE_SECONDS;
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
	        createKeySpace(keyspace, Integer.parseInt(replicationFactor), strategyClass,configurableConsistencyLevel);

        } catch (NamingException e) {
            throw new AndesException("Couldn't look up jndi entry for " +
                    "\"" + jndiLookupName + "\"" + e);
        }
    }

    /**
     * {@inheritDoc}
     * Remove the cluster and shutdown the {@link HConnectionManager} (underneath)
     */
    @Override
    public void close() {
        HFactory.shutdownCluster(cluster);
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
     * @param keyspace cassandra keyspace name
     * @param replicationFactor replication factor to configured in keyspace
     * @param strategyClass Cassandra strategy class
     * @param consistencyLevel ConfigurableConsistencyLevel
     */
    private void createKeySpace(String keyspace, int replicationFactor, String strategyClass,
                                ConfigurableConsistencyLevel consistencyLevel) throws AndesException {
        try{
            this.keyspace = HectorDataAccessHelper.createKeySpace(cluster,
                    keyspace,
                    replicationFactor, strategyClass, consistencyLevel);
        }catch(UnavailableException e){
            throw new AndesException("Not enough nodes for replication" + e);
        }
    }

    
    /**
     * Tests whether cluster/keyspace is reachable.
     * @return
     */
    public boolean isReachable() {

        boolean reachable = false;
        try {
            cluster.describeClusterName();
            reachable = true;
        } catch (HectorException ignore) {
            
        }

        return reachable;
    }
    
}
