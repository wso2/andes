/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.store.StoreHealthListener;

import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * CQL based connection object for Cassandra. Supports connecting to any Cassandra 2.xx node
 */
public class CQLConnection extends DurableStoreConnection {

    /**
     *  Session holds connections to a Cassandra cluster, allowing it to be queried.
     *  Each session maintains multiple connections to the cluster nodes.
     */
    private Session session;

    /**
     * Holds information and known state of a Cassandra cluster.
     * This is the main entry point of the Cassandra driver.
     */
    private Cluster cluster;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(ConfigurationProperties connectionProperties) throws AndesException {

        super.initialize(connectionProperties);
        
        String jndiLookupName = connectionProperties.getProperty(CQLConstants.PROP_JNDI_LOOKUP_NAME);
        

        try {
            cluster = InitialContext.doLookup(jndiLookupName);
            session = cluster.newSession();

            CassandraConfig config = new CassandraConfig();
            config.parse(connectionProperties);

            // TODO: for NetworkTopologyStrategy keyspace creation is different
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + config.getKeyspace() +
                    " WITH replication = {'class':'" + config.getStrategyClass() + "', " +
                    "'replication_factor':" + config.getReplicationFactor() + "};");

            session.execute("USE " + config.getKeyspace());

        } catch (NamingException e) {
            throw new AndesException("Couldn't look up JNDI entry for " + jndiLookupName, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        session.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getConnection() {
        return session;
    }

    /**
     * Returns the Session object. This is a long lived object connected to MB keyspace
     * @return Session
     */
    public Session getSession() {
        return session;
    }
    
    public Cluster getCluster(){
       return cluster;
    }
    
}
