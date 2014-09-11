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
package org.wso2.andes.messageStore;

public class JDBCConstants {

    // tables
    protected static final String MESSAGES_TABLE = "messages";
    protected static final String METADATA_TABLE = "metadata";
    protected static final String QUEUES_TABLE = "queues";
    protected static final String REF_COUNT_TABLE = "reference_counts";
    protected static final String EXPIRATION_TABLE = "expiration_data";

    // columns
    protected static final String MESSAGE_ID = "message_id";
    protected static final String QUEUE_ID = "queue_id";
    protected static final String QUEUE_NAME = "name";
    protected static final String REF_COUNT = "reference_count";
    protected static final String METADATA = "data";
    protected static final String MSG_OFFSET = "offset";
    protected static final String MESSAGE_CONTENT = "content";
    protected static final String EXPIRATION_TIME = "expiration_time";
    protected static final String DESTINATION_QUEUE = "destination";

    // jndi lookup

    protected static final String H2_JNDI_LOOKUP_NAME = "jdbc/H2MessageStoreDB";
    protected static final String H2_MEM_JNDI_LOOKUP_NAME = "jdbc/InMemoryMessageStoreDB";

    // sql prepared statements
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
                    " VALUES ( ?,?,? );";

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

    // jdbc tasks executed
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
}
