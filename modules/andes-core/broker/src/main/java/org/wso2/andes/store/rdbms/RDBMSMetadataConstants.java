/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

public class RDBMSMetadataConstants extends RDBMSConstants {

    /**
     * We need to select rows that have the DLC_QUEUE_ID = -1 indicating that the message is not moved
     * into the dead letter channel
     * Hardcoded METADATA_TABLE 1 to 4 and MB_CONTENT 1 to 4
     */
    protected String PS_INSERT_METADATA =
            "INSERT INTO " + METADATA_TABLE + " ("
                    + MESSAGE_ID + ","
                    + QUEUE_ID + ","
                    + DLC_QUEUE_ID + ","
                    + METADATA + ")"
                    + " VALUES ( ?,?,-1,? )";

    protected String PS_SELECT_QUEUE_MESSAGE_COUNT =
            "SELECT COUNT(" + QUEUE_ID + ") AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1";

    /**
     * Prepared statement to retrieve message count within a message id range for a queue.
     */
    protected String PS_SELECT_RANGED_QUEUE_MESSAGE_COUNT =
            "SELECT COUNT(" + MESSAGE_ID + ") AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " AND " + DLC_QUEUE_ID + "=-1";

    /**
     * Prepared statement to select all the queue names with the number of messages remaining in the database
     * by joining the tables MB_QUEUE_MAPPING and MB_METADATA.
     */
    protected String PS_SELECT_ALL_QUEUE_MESSAGE_COUNT =
            "SELECT " + QUEUE_NAME + ", " + PS_ALIAS_FOR_COUNT
                    + " FROM " + QUEUES_TABLE + " LEFT OUTER JOIN "
                    + "(SELECT " + QUEUE_ID + ", COUNT(" + QUEUE_ID + ") AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + DLC_QUEUE_ID + "=-1"
                    + " GROUP BY " + QUEUE_ID + " ) " + ALIAS_FOR_QUEUES
                    + " ON " + QUEUES_TABLE + "." + QUEUE_ID + "=" + ALIAS_FOR_QUEUES + "." + QUEUE_ID;

    protected String PS_SELECT_QUEUE_MESSAGE_COUNT_FROM_DLC =
            "SELECT COUNT(" + MESSAGE_ID + ")"
                    + " AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?";

    protected String PS_SELECT_MESSAGE_COUNT_IN_DLC =
            "SELECT COUNT(" + MESSAGE_ID + ")"
                    + " AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + DLC_QUEUE_ID + "=?";

    protected String PS_SELECT_METADATA =
            "SELECT " + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + "=?";

    protected String PS_SELECT_METADATA_RANGE_FROM_QUEUE =
            "SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " ORDER BY " + MESSAGE_ID;

    protected String PS_SELECT_METADATA_RANGE_FROM_QUEUE_IN_DLC =
            "SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " ORDER BY " + MESSAGE_ID;

    protected String PS_SELECT_METADATA_FROM_QUEUE =
            "SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " ORDER BY " + MESSAGE_ID;

    protected String PS_SELECT_MESSAGE_IDS_FROM_QUEUE =
            "SELECT " + MESSAGE_ID
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " ORDER BY " + MESSAGE_ID;

    protected String PS_SELECT_METADATA_IN_DLC_FOR_QUEUE =
            "SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID;

    protected String PS_SELECT_METADATA_IN_DLC =
            "SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID;

    protected String PS_SELECT_MESSAGE_IDS_FROM_METADATA_FOR_QUEUE =
            "SELECT " + MESSAGE_ID
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID ;

    protected String PS_DELETE_METADATA_FROM_QUEUE =
            "DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + "=?";

    protected String PS_DELETE_METADATA_IN_DLC =
            "DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "!= -1";

    protected String PS_DELETE_METADATA =
            "DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + "=?";


    protected String PS_CLEAR_QUEUE_FROM_METADATA =
            "DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?";

    protected String PS_CLEAR_DLC_QUEUE =
            "DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + DLC_QUEUE_ID + "=?";


    protected String PS_UPDATE_METADATA_QUEUE =
            "UPDATE " + METADATA_TABLE
                    + " SET " + QUEUE_ID + " = ?"
                    + " WHERE " + MESSAGE_ID + " = ?"
                    + " AND " + QUEUE_ID + " = ?";

    protected String PS_UPDATE_METADATA =
            "UPDATE " + METADATA_TABLE
                    + " SET " + QUEUE_ID + " = ?," + METADATA + " = ?"
                    + " WHERE " + MESSAGE_ID + " = ?"
                    + " AND " + QUEUE_ID + " = ?";

    /**
     * Prepared statement to move messages to DLC
     */
    protected String PS_MOVE_METADATA_TO_DLC =
            "UPDATE " + METADATA_TABLE
                    + " SET " + DLC_QUEUE_ID + "=?"
                    + " WHERE " + MESSAGE_ID + "=?";

    public RDBMSMetadataConstants(int tableId) {
        super();

        String derivedTableName = "MB_METADATA" + tableId;

        this.PS_CLEAR_DLC_QUEUE = this.PS_CLEAR_DLC_QUEUE.replace(METADATA_TABLE, derivedTableName);
        this.PS_CLEAR_QUEUE_FROM_METADATA = this.PS_CLEAR_QUEUE_FROM_METADATA.replace(METADATA_TABLE, derivedTableName);
        this.PS_DELETE_METADATA = this.PS_DELETE_METADATA.replace(METADATA_TABLE, derivedTableName);
        this.PS_DELETE_METADATA_FROM_QUEUE = this.PS_DELETE_METADATA_FROM_QUEUE.replace(METADATA_TABLE, derivedTableName);
        this.PS_DELETE_METADATA_IN_DLC = this.PS_DELETE_METADATA_IN_DLC.replace(METADATA_TABLE, derivedTableName);
        this.PS_INSERT_METADATA = this.PS_INSERT_METADATA.replace(METADATA_TABLE, derivedTableName);
        this.PS_MOVE_METADATA_TO_DLC = this.PS_MOVE_METADATA_TO_DLC.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_ALL_QUEUE_MESSAGE_COUNT = this.PS_SELECT_ALL_QUEUE_MESSAGE_COUNT.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_MESSAGE_COUNT_IN_DLC = this.PS_SELECT_MESSAGE_COUNT_IN_DLC.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_MESSAGE_IDS_FROM_METADATA_FOR_QUEUE = this.PS_SELECT_MESSAGE_IDS_FROM_METADATA_FOR_QUEUE.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_MESSAGE_IDS_FROM_QUEUE = this.PS_SELECT_MESSAGE_IDS_FROM_QUEUE.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_METADATA = this.PS_SELECT_METADATA.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_METADATA_FROM_QUEUE = this.PS_SELECT_METADATA_FROM_QUEUE.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_METADATA_IN_DLC = this.PS_SELECT_METADATA_IN_DLC.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_METADATA_IN_DLC_FOR_QUEUE = this.PS_SELECT_METADATA_IN_DLC_FOR_QUEUE.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_METADATA_RANGE_FROM_QUEUE = this.PS_SELECT_METADATA_RANGE_FROM_QUEUE.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_METADATA_RANGE_FROM_QUEUE_IN_DLC = this.PS_SELECT_METADATA_RANGE_FROM_QUEUE_IN_DLC.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_QUEUE_MESSAGE_COUNT = this.PS_SELECT_QUEUE_MESSAGE_COUNT.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_QUEUE_MESSAGE_COUNT_FROM_DLC = this.PS_SELECT_QUEUE_MESSAGE_COUNT_FROM_DLC.replace(METADATA_TABLE, derivedTableName);
        this.PS_SELECT_RANGED_QUEUE_MESSAGE_COUNT = this.PS_SELECT_RANGED_QUEUE_MESSAGE_COUNT.replace(METADATA_TABLE, derivedTableName);
        this.PS_UPDATE_METADATA = this.PS_UPDATE_METADATA.replace(METADATA_TABLE, derivedTableName);
        this.PS_UPDATE_METADATA_QUEUE = this.PS_UPDATE_METADATA_QUEUE.replace(METADATA_TABLE, derivedTableName);

    }
}
