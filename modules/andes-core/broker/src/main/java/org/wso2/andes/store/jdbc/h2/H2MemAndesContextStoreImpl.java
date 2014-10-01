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
 * under the License. and limitations under the License.
 */

package org.wso2.andes.store.jdbc.h2;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.ConfigurationProperties;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.store.jdbc.JDBCAndesContextStoreImpl;
import org.wso2.andes.store.jdbc.JDBCConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This is H2 in memory mode specific AndesContextStore implementation Table creation at startup is
 * done in this implementation
 */
public class H2MemAndesContextStoreImpl extends JDBCAndesContextStoreImpl {

    private static final Logger logger = Logger.getLogger(H2MemAndesContextStoreImpl.class);

    /**
     * Creates durable subscriptions table in H2 database
     */
    protected static final String CREATE_DURABLE_SUBSCRIPTION_TABLE =
            "CREATE TABLE IF NOT EXISTS durable_subscriptions (" +
                    "sub_id VARCHAR NOT NULL," +
                    "destination_identifier VARCHAR NOT NULL," +
                    "data VARCHAR NOT NULL" +
                    ");";

    /**
     * Creates node info table in H2 database
     */
    protected static final String CREATE_NODE_INFO_TABLE = "CREATE TABLE IF NOT EXISTS node_info (" +
            "node_id VARCHAR NOT NULL," +
            "data VARCHAR NOT NULL," +
            "PRIMARY KEY(node_id)" +
            ");";

    /**
     * Creates exchanges table in H2 database
     */
    protected static final String CREATE_EXCHANGES_TABLE = "CREATE TABLE IF NOT EXISTS exchanges (" +
            "name VARCHAR NOT NULL," +
            "data VARCHAR NOT NULL," +
            "PRIMARY KEY(name)" +
            ");";

    /**
     * Create queue_info table in H2 database
     */
    protected static final String CREATE_QUEUE_INFO_TABLE = "CREATE TABLE IF NOT EXISTS queue_info (" +
            "name VARCHAR NOT NULL," +
            "data VARCHAR NOT NULL," +
            "PRIMARY KEY(name)" +
            ");";
    /**
     * Creates bindings table in H2 database
     */
    protected static final String CREATE_BINDINGS_TABLE =
            "CREATE TABLE IF NOT EXISTS bindings (" +
                    "exchange_name VARCHAR NOT NULL," +
                    "queue_name VARCHAR NOT NULL," +
                    "binding_info VARCHAR NOT NULL," +
                    "FOREIGN KEY (exchange_name) REFERENCES exchanges (name)," +
                    "FOREIGN KEY (queue_name) REFERENCES queue_info (name)" +
                    ");";

    /**
     * logging string for task of creating database tables
     */
    protected static final String TASK_CREATING_DB_TABLES = "creating database tables";

    /**
     * Creates an in memory database connection and initialise the in memory data source.
     * connectionProperties provided here are IGNORED and in-memory mode connection properties
     * used internally.
     * <p/>
     * NOTE: connectionProperties are ignored to minimise error in pointing the in memory store
     * to a wrong message store
     *
     * @param connectionProperties ignored
     * @return DurableStoreConnection
     * @throws AndesException
     */
    public DurableStoreConnection init(ConfigurationProperties connectionProperties) throws
            AndesException {
        DurableStoreConnection durableStoreConnection = super.init(JDBCConnection
                .getInMemoryConnectionProperties());

        // Additionally create in memory database tables
        createTables();
        logger.info("In memory Andes context store initialised");
        return durableStoreConnection;
    }

    /**
     * This method creates all the DB tables used for H2 based Andes Context Store.
     *
     * @throws org.wso2.andes.kernel.AndesException
     */
    public void createTables() throws AndesException {
        String[] queries = {
                CREATE_DURABLE_SUBSCRIPTION_TABLE,
                CREATE_NODE_INFO_TABLE,
                CREATE_EXCHANGES_TABLE,
                CREATE_QUEUE_INFO_TABLE,
                CREATE_BINDINGS_TABLE
        };

        Connection connection = null;
        Statement stmt = null;
        try {
            connection = getConnection();
            stmt = connection.createStatement();
            for (String q : queries) {
                stmt.addBatch(q);
            }
            stmt.executeBatch();

        } catch (SQLException e) {
            rollback(connection, TASK_CREATING_DB_TABLES);
            throw new AndesException("Error occurred while creating database tables", e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                logger.error(TASK_CREATING_DB_TABLES);
            }
            close(connection, TASK_CREATING_DB_TABLES);
        }
    }


}
