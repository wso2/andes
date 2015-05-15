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

package org.wso2.andes.store.rdbms.h2;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.store.rdbms.RDBMSConnection;
import org.wso2.andes.store.rdbms.RDBMSMessageStoreImpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This is H2 in memory mode specific MessageStore implementation Table creation at startup is done
 * in this implementation
 */
public class H2MemMessageStoreImpl extends RDBMSMessageStoreImpl {

    private static final Logger logger = Logger.getLogger(H2MemMessageStoreImpl.class);
    /**
     * Create messages table in H2 database
     */
    protected static final String CREATE_CONTENT_TABLE = "CREATE TABLE IF NOT EXISTS MB_CONTENT (" +
            "MESSAGE_ID BIGINT, " +
            "CONTENT_OFFSET INT, " +
            "MESSAGE_CONTENT BLOB NOT NULL, " +
            "PRIMARY KEY (MESSAGE_ID,CONTENT_OFFSET)" +
            ");";

    /**
     * Create queues table in H2 database
     */
    protected static final String CREATE_QUEUES_TABLE = "CREATE TABLE IF NOT EXISTS MB_QUEUE_MAPPING (" +
            "QUEUE_ID INT AUTO_INCREMENT, " +
            "QUEUE_NAME VARCHAR NOT NULL, " +
            "UNIQUE (QUEUE_NAME)," +
            "PRIMARY KEY (QUEUE_ID)" +
            ");";

    /**
     * Create metadata table in H2 database
     */
    protected static final String CREATE_METADATA_TABLE = "CREATE TABLE IF NOT EXISTS MB_METADATA (" +
            "MESSAGE_ID BIGINT, " +
            "QUEUE_ID INT, " +
            "MESSAGE_METADATA BINARY, " +
            "PRIMARY KEY (MESSAGE_ID, QUEUE_ID), " +
            "FOREIGN KEY (QUEUE_ID) REFERENCES MB_QUEUE_MAPPING (QUEUE_ID) " +
            ");";

    /**
     * Creates expiration data table in H2 database
     */
    protected static final String CREATE_EXPIRATION_DATA_TABLE = "CREATE TABLE IF NOT EXISTS MB_EXPIRATION_DATA (" +
            "MESSAGE_ID BIGINT UNIQUE," +
            "EXPIRATION_TIME BIGINT, " +
            "MESSAGE_DESTINATION VARCHAR NOT NULL, " +
            "FOREIGN KEY (MESSAGE_ID) REFERENCES MB_METADATA (MESSAGE_ID)" +
            ");";

    /**
     * Create retain message content table in H2 database
     */
    protected static final String CREATE_RETAIN_CONTENT_TABLE = "CREATE TABLE IF NOT EXISTS MB_RETAINED_CONTENT (" +
                                                                "MESSAGE_ID BIGINT, " +
                                                                "CONTENT_OFFSET INT, " +
                                                                "MESSAGE_CONTENT BLOB NOT NULL, " +
                                                                "PRIMARY KEY (MESSAGE_ID,CONTENT_OFFSET)" +
                                                                ");";

    /**
     * Create retained metadata table in H2 database
     */
    protected static final String CREATE_RETAIN_METADATA_TABLE = "CREATE TABLE IF NOT EXISTS MB_RETAINED_METADATA (" +
                                                                 "TOPIC_ID INT, " +
                                                                 "TOPIC_NAME VARCHAR NOT NULL, " +
                                                                 "MESSAGE_ID BIGINT, " +
                                                                 "MESSAGE_METADATA BINARY, " +
                                                                 "PRIMARY KEY (TOPIC_ID), " +
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
     * @param contextStore AndesContextStore
     * @param connectionProperties ConfigurationProperties
     * @return DurableStoreConnection
     * @throws AndesException
     */
    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore, 
                                                         ConfigurationProperties connectionProperties)
            throws AndesException {

        // in memory mode should only run in single node mode
        if(AndesContext.getInstance().isClusteringEnabled()) {
            throw new AndesException("In memory mode is not supported in cluster setup");
        }

        // use the initialisation logic of JDBC MessageStore
        DurableStoreConnection durableStoreConnection = super.initializeMessageStore(
                contextStore, RDBMSConnection.getInMemoryConnectionProperties());

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
                CREATE_CONTENT_TABLE,
                CREATE_QUEUES_TABLE,
                CREATE_METADATA_TABLE,
                CREATE_EXPIRATION_DATA_TABLE,
                CREATE_RETAIN_METADATA_TABLE,
                CREATE_RETAIN_CONTENT_TABLE
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
                logger.error(TASK_CREATING_DB_TABLES, e);
            }
            close(connection, TASK_CREATING_DB_TABLES);
        }
    }
}
