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
import org.wso2.andes.store.jdbc.JDBCConnection;
import org.wso2.andes.store.jdbc.JDBCMessageStoreImpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This is H2 in memory mode specific MessageStore implementation Table creation at startup is done
 * in this implementation
 */
public class H2MemMessageStoreImpl extends JDBCMessageStoreImpl {

    private static final Logger logger = Logger.getLogger(H2MemMessageStoreImpl.class);
    /**
     * Create messages table in H2 database
     */
    protected static final String CREATE_MESSAGES_TABLE = "CREATE TABLE IF NOT EXISTS messages (" +
            "message_id BIGINT, " +
            "offset INT, " +
            "content BLOB NOT NULL, " +
            "PRIMARY KEY (message_id,offset)" +
            ");";

    /**
     * Create queues table in H2 database
     */
    protected static final String CREATE_QUEUES_TABLE = "CREATE TABLE IF NOT EXISTS queues (" +
            "queue_id INT AUTO_INCREMENT, " +
            "name VARCHAR NOT NULL, " +
            "UNIQUE (name)," +
            "PRIMARY KEY (queue_id)" +
            ");";

    /**
     * Create metadata table in H2 database
     */
    protected static final String CREATE_METADATA_TABLE = "CREATE TABLE IF NOT EXISTS metadata (" +
            "message_id BIGINT, " +
            "queue_id INT, " +
            "data BINARY, " +
            "PRIMARY KEY (message_id, queue_id), " +
            "FOREIGN KEY (queue_id) " +
            "REFERENCES queues (queue_id) " +
            ");";

    /**
     * Creates expiration data table in H2 database
     */
    protected static final String CREATE_EXPIRATION_DATA_TABLE = "CREATE TABLE IF NOT EXISTS expiration_data (" +
            "message_id BIGINT UNIQUE," +
            "expiration_time BIGINT, " +
            "destination VARCHAR NOT NULL, " +
            "FOREIGN KEY (message_id) " +
            "REFERENCES metadata " +
            "(message_id)" +
            ");";

    /**
     * Creates reference counts table in H2 database
     */
    protected static final String CREATE_REF_COUNT_TABLE = "CREATE TABLE IF NOT EXISTS " +
            "reference_counts ( " +
            "message_id BIGINT, " +
            "reference_count INT, " +
            "PRIMARY KEY (message_id)" +
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
     * @param connectionProperties IGNORED
     * @return DurableStoreConnection
     * @throws AndesException
     */
    @Override
    public DurableStoreConnection initializeMessageStore(ConfigurationProperties connectionProperties)
            throws AndesException {

        // use the initialisation logic of JDBC MessageStore
        DurableStoreConnection durableStoreConnection = super.initializeMessageStore(JDBCConnection
                .getInMemoryConnectionProperties());

        // Additionally create in memory database tables
        createTables();
        logger.info("In-memory message store initialised");
        return durableStoreConnection;
    }


    /**
     * This method creates all the DB tables used for H2 based In memory Message Store.
     *
     * @throws AndesException
     */
    public void createTables() throws AndesException {
        String[] queries = {
                CREATE_MESSAGES_TABLE,
                CREATE_QUEUES_TABLE,
                CREATE_METADATA_TABLE,
                CREATE_EXPIRATION_DATA_TABLE,
                CREATE_REF_COUNT_TABLE
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
            throw new AndesException("Error occurred while creating in memory database tables", e);
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
