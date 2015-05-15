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

package org.wso2.andes.store.rdbms;

/**
 * JDBC storage related prepared statements, table names, column names and tasks are grouped
 * in this class.
 */
public class RDBMSConstants {

    // jndi lookup
    protected static final String H2_MEM_JNDI_LOOKUP_NAME = "WSO2MBInMemoryStoreDB";

    // Configuration properties
    protected static final String PROP_JNDI_LOOKUP_NAME = "dataSource";

    // Message Store tables
    protected static final String CONTENT_TABLE = "MB_CONTENT";
    protected static final String METADATA_TABLE = "MB_METADATA";
    protected static final String QUEUES_TABLE = "MB_QUEUE_MAPPING";
    protected static final String EXPIRATION_TABLE = "MB_EXPIRATION_DATA";
    protected static final String RETAINED_METADATA_TABLE = "MB_RETAINED_METADATA";
    protected static final String RETAINED_CONTENT_TABLE = "MB_RETAINED_CONTENT";

    // Message Store table columns
    protected static final String MESSAGE_ID = "MESSAGE_ID";
    protected static final String QUEUE_ID = "QUEUE_ID";
    protected static final String QUEUE_NAME = "QUEUE_NAME";
    protected static final String METADATA = "MESSAGE_METADATA";
    protected static final String MSG_OFFSET = "CONTENT_OFFSET";
    protected static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";
    protected static final String EXPIRATION_TIME = "EXPIRATION_TIME";
    protected static final String DESTINATION_QUEUE = "MESSAGE_DESTINATION";
    protected static final String TOPIC_NAME = "TOPIC_NAME";
    protected static final String TOPIC_ID = "TOPIC_ID";

    // Andes Context Store tables
    protected static final String DURABLE_SUB_TABLE = "MB_DURABLE_SUBSCRIPTION";
    protected static final String NODE_INFO_TABLE = "MB_NODE";
    protected static final String EXCHANGES_TABLE = "MB_EXCHANGE";
    protected static final String BINDINGS_TABLE = "MB_BINDING";
    protected static final String QUEUE_INFO_TABLE = "MB_QUEUE";
    protected static final String QUEUE_COUNTER_TABLE = "MB_QUEUE_COUNTER";

    // Andes Context Store table columns
    protected static final String DURABLE_SUB_ID = "SUBSCRIPTION_ID";
    protected static final String DESTINATION_IDENTIFIER = "DESTINATION_IDENTIFIER";
    protected static final String DURABLE_SUB_DATA = "SUBSCRIPTION_DATA";
    protected static final String NODE_ID = "NODE_ID";
    protected static final String NODE_INFO = "NODE_DATA";
    protected static final String EXCHANGE_NAME = "EXCHANGE_NAME";
    protected static final String EXCHANGE_DATA = "EXCHANGE_DATA";
    protected static final String BINDING_INFO = "BINDING_DETAILS";
    protected static final String BINDING_QUEUE_NAME = "QUEUE_NAME";
    protected static final String BINDING_EXCHANGE_NAME = "EXCHANGE_NAME";
    protected static final String QUEUE_DATA = "QUEUE_DATA";
    protected static final String MESSAGE_COUNT = "MESSAGE_COUNT";

    // prepared statements for Message Store
    protected static final String PS_INSERT_MESSAGE_PART =
            "INSERT INTO " + CONTENT_TABLE + "(" +
                    MESSAGE_ID + "," +
                    MSG_OFFSET + "," +
                    MESSAGE_CONTENT + ") " +
                    "VALUES (?, ?, ?)";

    protected static final String PS_DELETE_MESSAGE_PARTS =
            "DELETE " +
                    " FROM " + CONTENT_TABLE +
                    " WHERE " + MESSAGE_ID + "=?";

    protected static final String PS_RETRIEVE_MESSAGE_PART =
            "SELECT " + MESSAGE_CONTENT +
                    " FROM " + CONTENT_TABLE +
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

    protected static final String PS_INSERT_QUEUE =
            "INSERT INTO " + RDBMSConstants.QUEUES_TABLE + " (" +
                    RDBMSConstants.QUEUE_NAME + ") " +
                    " VALUES (?)";

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
                    " AND ?" +
                    " ORDER BY " + MESSAGE_ID;

    protected static final String PS_SELECT_METADATA_FROM_QUEUE =
            "SELECT " + MESSAGE_ID + "," + METADATA +
                    " FROM " + METADATA_TABLE +
                    " WHERE " + MESSAGE_ID + ">?" +
                    " AND " + QUEUE_ID + "=?" +
                    " ORDER BY " + MESSAGE_ID;

    protected static final String PS_SELECT_MESSAGE_IDS_FROM_METADATA_FOR_QUEUE =
            "SELECT " + MESSAGE_ID +
                    " FROM " + METADATA_TABLE +
                    " WHERE " + QUEUE_ID + "=?" +
                    " ORDER BY " + MESSAGE_ID ;

    protected static final String PS_DELETE_EXPIRY_DATA = "DELETE " +
            " FROM " + EXPIRATION_TABLE +
            " WHERE " + MESSAGE_ID + "=?";

    protected static final String PS_DELETE_METADATA_FROM_QUEUE =
            "DELETE " +
                    " FROM " + METADATA_TABLE +
                    " WHERE " + QUEUE_ID + "=?" +
                    " AND " + MESSAGE_ID + "=?";

    protected static final String PS_CLEAR_QUEUE_FROM_METADATA = "DELETE " +
            " FROM " + METADATA_TABLE +
            " WHERE " + QUEUE_ID + "=?";

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

    protected static final String PS_UPDATE_DURABLE_SUBSCRIPTION =
            "UPDATE " + DURABLE_SUB_TABLE +
                    " SET " + DURABLE_SUB_DATA + "=? " +
                    " WHERE " + DESTINATION_IDENTIFIER + "=? AND " +
                    DURABLE_SUB_ID + "=?";

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
                    QUEUE_NAME + "," + QUEUE_DATA + ") " +
                    " VALUES (?,?)";

    protected static final String PS_SELECT_ALL_QUEUE_INFO =
            "SELECT " + QUEUE_DATA +
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

    protected static final String PS_UPDATE_METADATA_QUEUE =
            "UPDATE " + METADATA_TABLE +
                    " SET " + QUEUE_ID + " = ? " +
                    "WHERE " + MESSAGE_ID + " = ? " +
                    "AND " + QUEUE_ID + " = ?";

    protected static final String PS_UPDATE_METADATA =
            "UPDATE " + METADATA_TABLE +
                    " SET " + QUEUE_ID + " = ?," + METADATA + " = ?" +
                    " WHERE " + MESSAGE_ID + " = ?" +
                    " AND " + QUEUE_ID + " = ?";
    /**
     * Prepared Statement to insert a new queue counter.
     */
    protected static final String PS_INSERT_QUEUE_COUNTER =
            "INSERT INTO " + QUEUE_COUNTER_TABLE + " (" +
                    QUEUE_NAME + "," + MESSAGE_COUNT + " ) " +
                    " VALUES ( ?,? )";

    /**
     * Prepared Statement to select count for a queue prepared statement
     */
    protected static final String PS_SELECT_QUEUE_COUNT =
            "SELECT " + MESSAGE_COUNT +
                    " FROM " + QUEUE_COUNTER_TABLE +
                    " WHERE " + QUEUE_NAME + "=?";

    /**
     * Prepared Statement to delete queue counter with a given queue name
     */
    protected static final String PS_DELETE_QUEUE_COUNTER =
            "DELETE FROM " + QUEUE_COUNTER_TABLE +
                    " WHERE " + QUEUE_NAME + "=?";

    /**
     * Increments the queue count by a given value in a atomic db update
     */
    protected static final String PS_INCREMENT_QUEUE_COUNT =
            "UPDATE " + QUEUE_COUNTER_TABLE +
                    " SET " + MESSAGE_COUNT + "=" + MESSAGE_COUNT + "+? " +
                    " WHERE " + QUEUE_NAME + "=?";

    /**
     * Decrement the queue count by a given value in a atomic db update
     */
    protected static final String PS_DECREMENT_QUEUE_COUNT =
            "UPDATE " + QUEUE_COUNTER_TABLE +
                    " SET " + MESSAGE_COUNT + "=" + MESSAGE_COUNT + "-? " +
                    " WHERE " + QUEUE_NAME + "=?";

    protected static final String PS_RESET_QUEUE_COUNT =
            "UPDATE " + QUEUE_COUNTER_TABLE +
                    " SET " + MESSAGE_COUNT + "= 0" +
                    " WHERE " + QUEUE_NAME + "=?";

    /**
     * Prepared statement to update retained metadata
     */
    protected static final String PS_UPDATE_RETAINED_METADATA =
            "UPDATE " + RETAINED_METADATA_TABLE +
            " SET " + MESSAGE_ID + " = ?, " + METADATA + " = ?" +
            " WHERE " + TOPIC_ID + " = ?";

    /**
     * Prepared statement to delete messages from retained content
     */
    protected static final String PS_DELETE_RETAIN_MESSAGE_PARTS =
            "DELETE" +
            " FROM " + RETAINED_CONTENT_TABLE +
            " WHERE " + MESSAGE_ID + "=?";

    /**
     * Prepared statement to insert messages to retained content
     */
    protected static final String PS_INSERT_RETAIN_MESSAGE_PART =
            "INSERT INTO " + RETAINED_CONTENT_TABLE + "(" +
            MESSAGE_ID + "," +
            MSG_OFFSET + "," +
            MESSAGE_CONTENT + ") " +
            "VALUES (?, ?, ?)";

    /**
     * Prepared statement to select all retained topics from retained metadata
     */
    protected static final String PS_SELECT_ALL_RETAINED_TOPICS =
            "SELECT " + TOPIC_NAME +
            " FROM " + RETAINED_METADATA_TABLE;

    /**
     * Prepared statement to select retained message metadata for a given topic id
     */
    protected static final String PS_SELECT_RETAINED_METADATA =
            "SELECT " + MESSAGE_ID + ", " + METADATA +
            " FROM " + RETAINED_METADATA_TABLE +
            " WHERE " + TOPIC_ID + "=?";

    /**
     * Prepared statement to select retained message content for given message id
     */
    protected static final String PS_RETRIEVE_RETAIN_MESSAGE_PART =
            "SELECT " + MSG_OFFSET + ", " + MESSAGE_CONTENT +
            " FROM " + RETAINED_CONTENT_TABLE +
            " WHERE " + MESSAGE_ID + "=?";

    /**
     * Prepared statement to select retained metadata for given topic name
     */
    protected static final String PS_SELECT_RETAINED_MESSAGE_ID =
            "SELECT " + TOPIC_ID + ", " + MESSAGE_ID +
            " FROM " + RETAINED_METADATA_TABLE +
            " WHERE " + TOPIC_NAME + "=?";

    /**
     * Prepared statement to insert retained metadata
     */
    protected static final String PS_INSERT_RETAINED_METADATA =
            "INSERT INTO " + RETAINED_METADATA_TABLE + " (" +
            TOPIC_ID + "," +
            TOPIC_NAME + "," +
            MESSAGE_ID + "," +
            METADATA + ")" +
            " VALUES ( ?,?,?,? )";

    // Message Store related jdbc tasks executed
    protected static final String TASK_STORING_MESSAGE_PARTS = "storing message parts.";
    protected static final String TASK_DELETING_MESSAGE_PARTS = "deleting message parts.";
    protected static final String TASK_RETRIEVING_MESSAGE_PARTS = "retrieving message parts.";
    protected static final String TASK_RETRIEVING_CONTENT_FOR_MESSAGES = "retrieving content for multiple messages";
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
    protected static final String TASK_RETRIEVING_NEXT_N_MESSAGE_IDS_OF_QUEUE = "retrieving " +
            "message ID list from queue. ";
    protected static final String TASK_DELETING_FROM_EXPIRY_TABLE = "deleting from expiry table.";
    protected static final String TASK_DELETING_MESSAGE_LIST = "deleting message list.";
    protected static final String TASK_DELETING_METADATA_FROM_QUEUE = "deleting metadata from " +
            "queue. ";
    protected static final String TASK_RESETTING_MESSAGE_COUNTER = "Resetting message counter for queue";
    protected static final String TASK_RETRIEVING_EXPIRED_MESSAGES = "retrieving expired messages.";
    protected static final String TASK_RETRIEVING_QUEUE_ID = "retrieving queue id for queue. ";
    protected static final String TASK_CREATING_QUEUE = "creating queue. ";

    // Message Store related retained message jdbc tasks executed
    protected static final String TASK_STORING_RETAINED_MESSAGE_PARTS = "storing retained messages.";
    protected static final String TASK_RETRIEVING_RETAINED_MESSAGE_PARTS = "retrieving retained message parts.";



    // Andes Context Store related jdbc tasks executed
    protected static final String TASK_STORING_DURABLE_SUBSCRIPTION = "storing durable subscription";
    protected static final String TASK_UPDATING_DURABLE_SUBSCRIPTION = "updating durable subscription";
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
    protected static final String TASK_UPDATING_META_DATA_QUEUE = "updating message meta data queue.";
    protected static final String TASK_UPDATING_META_DATA = "updating message meta data.";
    protected static final String TASK_ADDING_QUEUE_COUNTER = "adding counter for queue";
    protected static final String TASK_CHECK_QUEUE_COUNTER_EXIST = "checking queue counter exist";
    protected static final String TASK_RETRIEVING_QUEUE_COUNT = "retrieving queue count";
    protected static final String TASK_DELETING_QUEUE_COUNTER = "deleting queue counter";
    protected static final String TASK_INCREMENTING_QUEUE_COUNT = "incrementing queue count";
    protected static final String TASK_DECREMENTING_QUEUE_COUNT = "decrementing queue count";

    /**
     * Only public static constants are in this class. No need to instantiate.
     */
    private RDBMSConstants() {
    }
}
