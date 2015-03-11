/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.store.AndesStoreUnavailableException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;


/**
 * Contains utility methods required for both {@link CQLBasedMessageStoreImpl} and {@link CQLBasedAndesContextStoreImpl}
 */
public class CQLUtils {

    private static final Logger log = Logger.getLogger(CQLUtils.class);
    
    /**
     * Tests whether data can be inserted to cluster with specified write consistency level. 
     * @param cqlConnection connection to be used.
     * @param config cassandra configuration
     * @param testString test data
     * @param testTime test data (long value
     * @return true if data can be written.
     */
    boolean testInsert(CQLConnection cqlConnection, CassandraConfig config, String testString, long testTime) {

        boolean insertSucess = false;

        try {
            Statement statement =
                                  QueryBuilder.insertInto(config.getKeyspace(), CQLConstants.MSG_STORE_STATUS_TABLE).ifNotExists()
                                              .value(CQLConstants.NODE_ID, testString).value(CQLConstants.TIME_STAMP, testTime)
                                              .setConsistencyLevel(config.getWriteConsistencyLevel());
            execute(cqlConnection, statement,
                    "testing data insertion with write consistency level : " + config.getWriteConsistencyLevel());
            insertSucess = true;
        } catch (AndesException ignore) {

        }

        return insertSucess;
    }

    /**
     * Tests whether data can be read from the cluster with specified (in configuration) read consistency level.
     * @param cqlConnection connection to be used.
     * @param config cassandra configuration.
     * @param testString test data
     * @param testTime test data( long value)
     * @return true of data can be read from cluster.
     */
    boolean testRead(CQLConnection cqlConnection, CassandraConfig config, String testString, long testTime) {

        boolean readSucess = false;

        try {
            Statement statement =
                                  QueryBuilder.select().column(CQLConstants.NODE_ID).column(CQLConstants.TIME_STAMP)
                                              .from(config.getKeyspace(), CQLConstants.MSG_STORE_STATUS_TABLE)
                                              .setConsistencyLevel(config.getReadConsistencyLevel());

            execute(cqlConnection, statement, "testing data read with read consistency level: " + config.getReadConsistencyLevel());
            readSucess = true;
        } catch (AndesException ignore) {

        }

        return readSucess;
    }

    /**
     *  Tests whether data can be deleted from the cluster with specified (in configuration) write consistency level.
     * @param cqlConnection connection to be used.
     * @param config Cassandra configuration 
     * @param testString test data
     * @param testTime test data (long)
     * @return
     */
    boolean testDelete(CQLConnection cqlConnection, CassandraConfig config, String testString, long testTime) {

        boolean deleteSucess = false;

        try {

            Statement statement =
                                  QueryBuilder.delete().from(config.getKeyspace(), CQLConstants.MSG_STORE_STATUS_TABLE)
                                              .where(eq(CQLConstants.NODE_ID, testString)).and(eq(CQLConstants.TIME_STAMP, testTime))
                                              .setConsistencyLevel(config.getWriteConsistencyLevel());

            execute(cqlConnection, statement, "testing data deletes with write consistency level: " + config.getWriteConsistencyLevel());
            deleteSucess = true;
        } catch (AndesException ignore) {

        }

        return deleteSucess;
    }

    /**
     * Checks weather Cassandra cluster is reachable.
     * 
     * @param cqlConnection
     *            connection to be used.
     * @return true if {@link Metadata} can be retrieved for the cluster.
     */
    boolean isReachable(CQLConnection cqlConnection){
        
        boolean isReachable = false;
        try {
            // this will check the connectivity to cassandra cluster.
            Metadata metadata = cqlConnection.getCluster().getMetadata();

            for (Host host : metadata.getAllHosts()) {
                log.info(String.format("cassandra [%s] with socket address [%s] is operational", host.getAddress(),
                                       host.getSocketAddress()));
            }
            isReachable = true;
        } catch (DriverException driverEx) {
            // DriverException is the top level exception for all cassandra
            // errors.
            log.info("error occurred while checking the operation status of cassandra server(s)", driverEx);
        }
        
        return isReachable;
    }
    
    /**
     * Create the table required for testing for Cassandra availability.
     * Known usages at {@link CQLBasedMessageStoreImpl} and
     * {@link CQLBasedAndesContextStoreImpl}
     * 
     * @param cqlConnection
     *            connection to be used
     * @param config
     *            Cassandra configuration
     */
    void createSchema(CQLConnection cqlConnection, CassandraConfig config){
        
        Statement statement = SchemaBuilder.createTable(config.getKeyspace(), CQLConstants.MSG_STORE_STATUS_TABLE).ifNotExists().
                addPartitionKey(CQLConstants.NODE_ID, DataType.text()).
                addClusteringColumn(CQLConstants.TIME_STAMP, DataType.bigint()).
                withOptions().clusteringOrder(CQLConstants.TIME_STAMP, SchemaBuilder.Direction.ASC).
                gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());
        
        cqlConnection.getSession().execute(statement);
    }
    
    
    
    /**
     * Executes the statement using the given session and returns the resultSet.
     * Additionally this catches any NoHostAvailableException or QueryExecutionException thrown by CQL driver and
     * rethrows an AndesException
     *
     * @param cqlConnection CQL connection to be used.
     * @param statement statement to be executed
     * @param task description of the task that is done by the statement
     * @return ResultSet
     * @throws AndesException
     */
    private ResultSet execute(CQLConnection cqlConnection, Statement statement, String task) throws AndesStoreUnavailableException {
        try {
            return cqlConnection.getSession().execute(statement);
        } catch (NoHostAvailableException e) {
            
            log.error("Unable to connect to cassandra cluster for " + task, e);
            
            Map<InetSocketAddress,Throwable> errors = e.getErrors();
            for ( Entry<InetSocketAddress, Throwable> err: errors.entrySet()){
                log.error("Error occured while connecting to cassandra server: " + err.getKey() + " error: ", err.getValue());
            }
            
            throw new AndesStoreUnavailableException("error occured while trying to connect to cassandra server(s) for " + task, e);
            
        } catch (QueryExecutionException e) {
            throw new AndesStoreUnavailableException("Error occurred while " + task, e);
        }
    }
    
    
}
