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

public class JDBCConstants {

    // jndi lookup
    protected static final String H2_JNDI_LOOKUP_NAME = "jdbc/H2MessageStoreDB";
    protected static final String H2_MEM_JNDI_LOOKUP_NAME = "jdbc/InMemoryStore";

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
    protected static final String NODE_DATA = "data";
    protected static final String EXCHANGE_NAME = "name";
    protected static final String EXCHANGE_DATA = "data";
    protected static final String ROUTING_KEY = "routing_key";
    protected static final String BINDING_QUEUE_NAME = "queue_name";
    protected static final String BINDING_EXCHANGE_NAME = "exchange_name";
    protected static final String QUEUE_DATA= "data";


    // prepared statements for Message Store
    protected static final String PS_INSERT_MESSAGE_PART =
            "INSERT INTO " + JDBCConstants.MESSAGES_TABLE + "(" +
                    JDBCConstants.MESSAGE_ID + "," +
                    JDBCConstants.MSG_OFFSET + "," +
                    JDBCConstants.MESSAGE_CONTENT + ") " +
                    "VALUES (?, ?, ?)";

    protected static final String PS_DELETE_MESSAGE_PARTS =
            "DELETE " +
                    " FROM " + JDBCConstants.MESSAGES_TABLE +
                    " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

    protected static final String PS_RETRIEVE_MESSAGE_PART =
            "SELECT " + JDBCConstants
                    .MESSAGE_CONTENT +
                    " FROM " + JDBCConstants.MESSAGES_TABLE +
                    " WHERE " + JDBCConstants.MESSAGE_ID + "=?" +
                    " AND " + JDBCConstants.MSG_OFFSET + "=?";

    protected static final String PS_INSERT_METADATA =
            "INSERT INTO " + JDBCConstants.METADATA_TABLE + " (" +
                    JDBCConstants.MESSAGE_ID + "," +
                    JDBCConstants.QUEUE_ID + "," +
                    JDBCConstants.METADATA + ")" +
                    " VALUES ( ?,?,? )";

    protected static final String PS_INSERT_EXPIRY_DATA =
            "INSERT INTO " + JDBCConstants.EXPIRATION_TABLE + " (" +
                    JDBCConstants.MESSAGE_ID + "," +
                    JDBCConstants.EXPIRATION_TIME + "," +
                    JDBCConstants.DESTINATION_QUEUE + ")" +
                    " VALUES ( ?,?,? )";

    protected static final String PS_ALIAS_FOR_COUNT = "count";

    protected static final String PS_SELECT_QUEUE_MESSAGE_COUNT =
            "SELECT COUNT(" + JDBCConstants.QUEUE_ID + ") AS " + PS_ALIAS_FOR_COUNT +
                    " FROM " + JDBCConstants.METADATA_TABLE +
                    " WHERE " + JDBCConstants.QUEUE_ID + "=?";

    protected static final String PS_SELECT_METADATA =
            "SELECT " + JDBCConstants.METADATA +
                    " FROM " + JDBCConstants.METADATA_TABLE +
                    " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

    protected static final String PS_SELECT_METADATA_RANGE_FROM_QUEUE =
            "SELECT " + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.METADATA +
                    " FROM " + JDBCConstants.METADATA_TABLE +
                    " WHERE " + JDBCConstants.QUEUE_ID + "=?" +
                    " AND " + JDBCConstants.MESSAGE_ID +
                    " BETWEEN ?" +
                    " AND ?";

    protected static final String PS_SELECT_METADATA_FROM_QUEUE =
            "SELECT " + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.METADATA +
                    " FROM " + JDBCConstants.METADATA_TABLE +
                    " WHERE " + JDBCConstants.MESSAGE_ID + ">?" +
                    " AND " + JDBCConstants.QUEUE_ID + "=?";

    protected static final String PS_DELETE_METADATA =
            "DELETE " +
                    " FROM " + JDBCConstants.METADATA_TABLE +
                    " WHERE " + JDBCConstants.QUEUE_ID + "=?" +
                    " AND " + JDBCConstants.MESSAGE_ID + "=?";

    protected static final String PS_DELETE_EXPIRY_DATA = "DELETE " +
            " FROM " + JDBCConstants.EXPIRATION_TABLE +
            " WHERE " + JDBCConstants.MESSAGE_ID + "=?";

    protected static final String PS_DELETE_METADATA_FROM_QUEUE =
            "DELETE " +
                    " FROM " + JDBCConstants.METADATA_TABLE +
                    " WHERE " + JDBCConstants.QUEUE_ID + "=?" +
                    " AND " + JDBCConstants.MESSAGE_ID + "=?";

    protected static final String PS_SELECT_EXPIRED_MESSAGES =
            "SELECT " + JDBCConstants.MESSAGE_ID + "," + JDBCConstants.DESTINATION_QUEUE +
                    " FROM " + JDBCConstants.EXPIRATION_TABLE +
                    " WHERE " + JDBCConstants.EXPIRATION_TIME + "<" + System.currentTimeMillis();

    protected static final String PS_SELECT_QUEUE_ID =
            "SELECT " + JDBCConstants.QUEUE_ID +
                    " FROM " + JDBCConstants.QUEUES_TABLE +
                    " WHERE " + JDBCConstants.QUEUE_NAME + "=?";

    // prepared statements for Andes Context Store
    protected static final String PS_INSERT_DURABLE_SUBSCRIPTION =
            "INSERT INTO " + JDBCConstants.DURABLE_SUB_TABLE + " (" +
                    JDBCConstants.DESTINATION_IDENTIFIER + ", " +
                    JDBCConstants.DURABLE_SUB_ID + ", " +
                    JDBCConstants.DURABLE_SUB_DATA + ") " +
                    " VALUES (?,?,?)";

    protected static final String PS_SELECT_ALL_DURABLE_SUBSCRIPTIONS =
            "SELECT " + JDBCConstants.DESTINATION_IDENTIFIER + "," + JDBCConstants.DURABLE_SUB_DATA +
                    " FROM " + JDBCConstants.DURABLE_SUB_TABLE;

    protected static final String PS_DELETE_DURABLE_SUBSCRIPTION =
            "DELETE FROM " + JDBCConstants.DURABLE_SUB_TABLE +
                    " WHERE " + JDBCConstants.DESTINATION_IDENTIFIER + "=? " +
                    " AND " + JDBCConstants.DURABLE_SUB_ID + "=?";

    protected static final String PS_INSERT_NODE_INFO =
            "INSERT INTO " + JDBCConstants.NODE_INFO_TABLE + " ( " +
                    JDBCConstants.NODE_ID + "," +
                    JDBCConstants.NODE_DATA + ") " +
                    " VALUES (?,?)";

    protected static final String PS_SELECT_ALL_NODE_INFO =
            "SELECT * " +
                    "FROM " + JDBCConstants.NODE_INFO_TABLE;

    protected static final String PS_DELETE_NODE_INFO =
            "DELETE FROM " + JDBCConstants.NODE_INFO_TABLE +
            " WHERE " + JDBCConstants.NODE_ID + "=?";

    // Message Store related jdbc tasks executed
    protected static final String TASK_STORING_MESSAGE_PARTS = "storing message parts";
    protected static final String TASK_DELETING_MESSAGE_PARTS = "deleting message parts";
    protected static final String TASK_RETRIEVING_MESSAGE_PARTS = "retrieving message parts";
    protected static final String TASK_ADDING_METADATA_LIST = "adding metadata list";
    protected static final String TASK_ADDING_METADATA = "adding metadata";
    protected static final String TASK_ADDING_METADATA_TO_QUEUE = "adding metadata to " +
            "destination ";
    protected static final String TASK_ADDING_METADATA_LIST_TO_QUEUE = "adding metadata list to " +
            "destination ";
    protected static final String TASK_RETRIEVING_QUEUE_MSG_COUNT = "retrieving message count for" +
            " queue ";
    protected static final String TASK_RETRIEVING_METADATA = "retrieving metadata for message id ";
    protected static final String TASK_RETRIEVING_METADATA_RANGE_FROM_QUEUE = "retrieving " +
            "metadata within a range from queue ";
    protected static final String TASK_RETRIEVING_NEXT_N_METADATA_FROM_QUEUE = "retrieving " +
            "metadata list from queue";
    protected static final String TASK_DELETING_FROM_EXPIRY_TABLE = "deleting from expiry table";
    protected static final String TASK_DELETING_MESSAGE_LIST = "deleting message list";
    protected static final String TASK_DELETING_METADATA_FROM_QUEUE = "deleting metadata from " +
            "queue";
    protected static final String TASK_RETRIEVING_EXPIRED_MESSAGES = "retrieving expired messages";
    protected static final String TASK_RETRIEVING_QUEUE_ID = "retrieving queue id for queue ";
    protected static final String TASK_CREATING_QUEUE = "creating queue ";


    // Andes Context Store related jdbc tasks executed
    protected static final String TASK_STORING_DURABLE_SUBSCRIPTION = "storing durable subscription";
    protected static final String TASK_RETRIEVING_ALL_DURABLE_SUBSCRIPTION = "retrieving " +
            "all durable subscriptions";

    protected static final String TASK_REMOVING_DURABLE_SUBSCRIPTION = "removing durable " +
            "subscription. ";
    protected static final String TASK_STORING_NODE_INFORMATION = "storing node information";
    protected static final String TASK_RETRIEVING_ALL_NODE_DETAILS = "retrieving all node " +
            "information";
    protected static final String TASK_REMOVING_NODE_INFORMATION = "removing node information";
    protected static final String TASK_STORING_EXCHANGE_INFORMATION = "storing exchange information";

}
