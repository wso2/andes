/*
 *
 *   Copyright (c) 2005-2011, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.andes.store.jdbc;

/**
 * JDBC storage related prepared statements, table names, column names and tasks are grouped
 * in this class.
 */
public class JDBCConstants {

    // jndi lookup
    protected static final String H2_MEM_JNDI_LOOKUP_NAME = "jdbc/InMemoryStore";

    // Configuration properties
    protected static final String PROP_JNDI_LOOKUP_NAME = "dataSource";
    protected static final String PROP_CREATE_TABLES = "createTables";

    // Message Store tables

    protected static final String MESSAGES_TABLE = "messages";
    protected static final String METADATA_TABLE = "metadata";
    protected static final String QUEUES_TABLE = "queues";
    protected static final String REF_COUNT_TABLE = "reference_counts";
    protected static final String EXPIRATION_TABLE = "expiration_data";

    // Message Store table columns
    protected static final String MESSAGE_ID = "message_id";
    protected static final String QUEUE_ID = "queue_id";
    protected static final String QUEUE_NAME = "name";
    protected static final String REF_COUNT = "reference_count";
    protected static final String METADATA = "data";
    protected static final String MSG_OFFSET = "offset";
    protected static final String MESSAGE_CONTENT = "content";
    protected static final String EXPIRATION_TIME = "expiration_time";
    protected static final String DESTINATION_QUEUE = "destination";

    // Andes Context Store tables
    protected static final String DURABLE_SUB_TABLE = "durable_subscriptions";
    protected static final String NODE_INFO_TABLE = "node_info";
    protected static final String EXCHANGES_TABLE = "exchanges";
    protected static final String BINDINGS_TABLE = "bindings";
    protected static final String QUEUE_INFO_TABLE = "queue_info";

    // Andes Context Store table columns
    protected static final String DURABLE_SUB_ID = "sub_id";
    protected static final String DESTINATION_IDENTIFIER = "destination_identifier";
    protected static final String DURABLE_SUB_DATA = "data";
    protected static final String NODE_ID = "node_id";
    protected static final String NODE_INFO = "data";
    protected static final String EXCHANGE_NAME = "name";
    protected static final String EXCHANGE_DATA = "data";
    protected static final String BINDING_INFO = "binding_info";
    protected static final String BINDING_QUEUE_NAME = "queue_name";
    protected static final String BINDING_EXCHANGE_NAME = "exchange_name";
    protected static final String QUEUE_INFO = "data";


    // prepared statements for Message Store
    protected static final String PS_INSERT_MESSAGE_PART =
            "INSERT INTO " + MESSAGES_TABLE + "(" +
            MESSAGE_ID + "," +
            MSG_OFFSET + "," +
            MESSAGE_CONTENT + ") " +
            "VALUES (?, ?, ?)";

    protected static final String PS_DELETE_MESSAGE_PARTS =
            "DELETE " +
            " FROM " + MESSAGES_TABLE +
            " WHERE " + MESSAGE_ID + "=?";

    protected static final String PS_RETRIEVE_MESSAGE_PART =
            "SELECT " + MESSAGE_CONTENT +
            " FROM " + MESSAGES_TABLE +
            " WHERE " + MESSAGE_ID + "=?" +
            " AND " + MSG_OFFSET + "=?";

    protected static final String PS_INSERT_METADATA =
            "INSERT INTO " + METADATA_TABLE + " (" +
            MESSAGE_ID + "," +
            QUEUE_ID + "," +
            METADATA + ")" +
            " VALUES ( ?,?,? )";

    protected static final String PS_INSERT_EXPIRY_DATA =
            "INSERT INTO " + EXPIRATION_TABLE + " (" +
            MESSAGE_ID + "," +
            EXPIRATION_TIME + "," +
            DESTINATION_QUEUE + ")" +
            " VALUES ( ?,?,? )";

    protected static final String PS_ALIAS_FOR_COUNT = "count";

    protected static final String PS_SELECT_QUEUE_MESSAGE_COUNT =
            "SELECT COUNT(" + QUEUE_ID + ") AS " + PS_ALIAS_FOR_COUNT +
            " FROM " + METADATA_TABLE +
            " WHERE " + QUEUE_ID + "=?";

    protected static final String PS_SELECT_METADATA =
            "SELECT " + METADATA +
            " FROM " + METADATA_TABLE +
            " WHERE " + MESSAGE_ID + "=?";

    protected static final String PS_SELECT_METADATA_RANGE_FROM_QUEUE =
            "SELECT " + MESSAGE_ID + "," + METADATA +
            " FROM " + METADATA_TABLE +
            " WHERE " + QUEUE_ID + "=?" +
            " AND " + MESSAGE_ID +
            " BETWEEN ?" +
            " AND ?";

    protected static final String PS_SELECT_METADATA_FROM_QUEUE =
            "SELECT " + MESSAGE_ID + "," + METADATA +
            " FROM " + METADATA_TABLE +
            " WHERE " + MESSAGE_ID + ">?" +
            " AND " + QUEUE_ID + "=?";

    protected static final String PS_DELETE_METADATA =
            "DELETE " +
            " FROM " + METADATA_TABLE +
            " WHERE " + QUEUE_ID + "=?" +
            " AND " + MESSAGE_ID + "=?";

    protected static final String PS_DELETE_EXPIRY_DATA = "DELETE " +
                                                          " FROM " + EXPIRATION_TABLE +
                                                          " WHERE " + MESSAGE_ID + "=?";

    protected static final String PS_DELETE_METADATA_FROM_QUEUE =
            "DELETE " +
            " FROM " + METADATA_TABLE +
            " WHERE " + QUEUE_ID + "=?" +
            " AND " + MESSAGE_ID + "=?";

    protected static final String PS_SELECT_EXPIRED_MESSAGES =
            "SELECT " + MESSAGE_ID + "," + DESTINATION_QUEUE +
            " FROM " + EXPIRATION_TABLE +
            " WHERE " + EXPIRATION_TIME + "<" + System.currentTimeMillis();

    protected static final String PS_SELECT_QUEUE_ID =
            "SELECT " + QUEUE_ID +
            " FROM " + QUEUES_TABLE +
            " WHERE " + QUEUE_NAME + "=?";

    // prepared statements for Andes Context Store
    protected static final String PS_INSERT_DURABLE_SUBSCRIPTION =
            "INSERT INTO " + DURABLE_SUB_TABLE + " (" +
            DESTINATION_IDENTIFIER + ", " +
            DURABLE_SUB_ID + ", " +
            DURABLE_SUB_DATA + ") " +
            " VALUES (?,?,?)";

    protected static final String PS_SELECT_ALL_DURABLE_SUBSCRIPTIONS =
            "SELECT " + DESTINATION_IDENTIFIER + "," + DURABLE_SUB_DATA +
            " FROM " + DURABLE_SUB_TABLE;

    protected static final String PS_DELETE_DURABLE_SUBSCRIPTION =
            "DELETE FROM " + DURABLE_SUB_TABLE +
            " WHERE " + DESTINATION_IDENTIFIER + "=? " +
            " AND " + DURABLE_SUB_ID + "=?";

    protected static final String PS_INSERT_NODE_INFO =
            "INSERT INTO " + NODE_INFO_TABLE + " ( " +
            NODE_ID + "," +
            NODE_INFO + ") " +
            " VALUES (?,?)";

    protected static final String PS_SELECT_ALL_NODE_INFO =
            "SELECT * " +
            "FROM " + NODE_INFO_TABLE;

    protected static final String PS_DELETE_NODE_INFO =
            "DELETE FROM " + NODE_INFO_TABLE +
            " WHERE " + NODE_ID + "=?";

    protected static final String PS_STORE_EXCHANGE_INFO =
            "INSERT INTO " + EXCHANGES_TABLE + " (" +
            EXCHANGE_NAME + "," +
            EXCHANGE_DATA + ") " +
            " VALUES (?,?)";

    protected static final String PS_SELECT_ALL_EXCHANGE_INFO =
            "SELECT " + EXCHANGE_DATA +
            " FROM " + EXCHANGES_TABLE;

    protected static final String PS_SELECT_EXCHANGE =
            "SELECT " + EXCHANGE_DATA +
            " FROM " + EXCHANGES_TABLE +
            " WHERE " + EXCHANGE_NAME + "=?";

    protected static final String PS_DELETE_EXCHANGE =
            "DELETE FROM " + EXCHANGES_TABLE +
            " WHERE " + EXCHANGE_NAME + "=?";

    protected static final String PS_INSERT_QUEUE_INFO =
            "INSERT INTO " + QUEUE_INFO_TABLE + " (" +
            QUEUE_NAME + "," + QUEUE_INFO + ") " +
            " VALUES (?,?)";

    protected static final String PS_SELECT_ALL_QUEUE_INFO =
            "SELECT " + QUEUE_INFO +
            " FROM " + QUEUE_INFO_TABLE;

    protected static final String PS_DELETE_QUEUE_INFO =
            "DELETE FROM " + QUEUE_INFO_TABLE +
            " WHERE " + QUEUE_NAME + "=?";

    protected static final String PS_INSERT_BINDING =
            "INSERT INTO " + BINDINGS_TABLE + " ( " +
            BINDING_EXCHANGE_NAME + "," +
            BINDING_QUEUE_NAME + "," +
            BINDING_INFO + " ) " +
            " VALUES (?,?,?)";

    protected static final String PS_SELECT_BINDINGS_FOR_EXCHANGE =
            "SELECT " + BINDING_INFO +
            " FROM " + BINDINGS_TABLE +
            " WHERE " + BINDING_EXCHANGE_NAME + "=?";

    protected static final String PS_DELETE_BINDING =
            "DELETE FROM " + BINDINGS_TABLE +
            " WHERE " + BINDING_EXCHANGE_NAME + "=? " +
            " AND " + BINDING_QUEUE_NAME + "=?";

    // Message Store related jdbc tasks executed
    protected static final String TASK_STORING_MESSAGE_PARTS = "storing message parts.";
    protected static final String TASK_DELETING_MESSAGE_PARTS = "deleting message parts.";
    protected static final String TASK_RETRIEVING_MESSAGE_PARTS = "retrieving message parts.";
    protected static final String TASK_ADDING_METADATA_LIST = "adding metadata list.";
    protected static final String TASK_ADDING_METADATA = "adding metadata.";
    protected static final String TASK_ADDING_METADATA_TO_QUEUE = "adding metadata to " +
                                                                  "destination. ";
    protected static final String TASK_ADDING_METADATA_LIST_TO_QUEUE = "adding metadata list to " +
                                                                       "destination. ";
    protected static final String TASK_RETRIEVING_QUEUE_MSG_COUNT = "retrieving message count for" +
                                                                    " queue. ";
    protected static final String TASK_RETRIEVING_METADATA = "retrieving metadata for message id. ";
    protected static final String TASK_RETRIEVING_METADATA_RANGE_FROM_QUEUE = "retrieving " +
                                                                              "metadata within a range from queue. ";
    protected static final String TASK_RETRIEVING_NEXT_N_METADATA_FROM_QUEUE = "retrieving " +
                                                                               "metadata list from queue. ";
    protected static final String TASK_DELETING_FROM_EXPIRY_TABLE = "deleting from expiry table.";
    protected static final String TASK_DELETING_MESSAGE_LIST = "deleting message list.";
    protected static final String TASK_DELETING_METADATA_FROM_QUEUE = "deleting metadata from " +
                                                                      "queue. ";
    protected static final String TASK_RETRIEVING_EXPIRED_MESSAGES = "retrieving expired messages.";
    protected static final String TASK_RETRIEVING_QUEUE_ID = "retrieving queue id for queue. ";
    protected static final String TASK_CREATING_QUEUE = "creating queue. ";

    // Andes Context Store related jdbc tasks executed
    protected static final String TASK_STORING_DURABLE_SUBSCRIPTION = "storing durable subscription";
    protected static final String TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION = "retrieving " +
                                                                             "all durable subscriptions. ";

    protected static final String TASK_REMOVING_DURABLE_SUBSCRIPTION = "removing durable " +
                                                                       "subscription. ";
    protected static final String TASK_STORING_NODE_INFORMATION = "storing node information";
    protected static final String TASK_RETRIEVING_ALL_NODE_DETAILS = "retrieving all node " +
                                                                     "information. ";
    protected static final String TASK_REMOVING_NODE_INFORMATION = "removing node information";
    protected static final String TASK_STORING_EXCHANGE_INFORMATION = "storing exchange information";
    protected static final String TASK_RETRIEVING_ALL_EXCHANGE_INFO = "retrieving all exchange " +
                                                                      "information. ";
    protected static final String TASK_IS_EXCHANGE_EXIST = "checking whether an exchange " +
                                                           "exist. ";
    protected static final String TASK_DELETING_EXCHANGE = "deleting an exchange ";
    protected static final String TASK_STORING_QUEUE_INFO = "storing queue information ";
    protected static final String TASK_RETRIEVING_ALL_QUEUE_INFO = "retrieving all queue " +
                                                                   "information. ";
    protected static final String TASK_DELETING_QUEUE_INFO = "deleting queue information. ";
    protected static final String TASK_STORING_BINDING = "storing binding information. ";
    protected static final String TASK_RETRIEVING_BINDING_INFO = "retrieving binding information.";
    protected static final String TASK_DELETING_BINDING = "deleting binding information. ";

    protected static final String TASK_CREATING_DB_TABLES = "creating database tables";

    // H2 Message Store specific create table statements

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

    // H2 Andes Context Store specific create table statements

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
    protected static final String CREATE_QUEUE_INFO_TABLE =  "CREATE TABLE IF NOT EXISTS queue_info (" +
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
}
