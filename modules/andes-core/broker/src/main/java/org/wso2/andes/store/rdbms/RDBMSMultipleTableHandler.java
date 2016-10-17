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

import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;


/**
 * JDBC storage related prepared statements, table names, column names and tasks
 * that changes according to the number of tables that are grouped.
 */

public class RDBMSMultipleTableHandler {

    private static final String CONTENT_TABLE = "MB_CONTENT";
    private static final String MESSAGE_ID = "MESSAGE_ID";
    private static final String MSG_OFFSET = "CONTENT_OFFSET";
    private static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";

    private static final String METADATA_TABLE = "MB_METADATA";
    private static final String QUEUE_ID = "QUEUE_ID";
    private static final String DLC_QUEUE_ID = "DLC_QUEUE_ID";
    private static final String METADATA = "MESSAGE_METADATA";
    private static final String PS_ALIAS_FOR_COUNT = "count";
    protected static final String QUEUE_NAME = RDBMSConstants.QUEUE_NAME;
    private static final String QUEUES_TABLE = "MB_QUEUE_MAPPING";
    private static final String DESTINATION_QUEUE = "MESSAGE_DESTINATION";
    private static final String EXPIRATION_TABLE = "MB_EXPIRATION_DATA";
    private static final String EXPIRATION_TIME = "EXPIRATION_TIME";


    private static final String ALIAS_FOR_QUEUES = "QUEUE_COUNT";

    // Get number of tables from the configurations. Default value is 5.
    private Integer numberOfTables = AndesConfigurationManager.readValue(
            AndesConfiguration.PERFORMANCE_TUNING_NUMBER_OF_TABLES);

    // Insert queries
    private String[] psInsertMessagePart = new String[numberOfTables];
    private String[] psInsertMetadata = new String[numberOfTables];

    // Retrieve queries
    private String[] psRetrieveMessagePart = new String[numberOfTables];

    // Select queries
    private String[] psSelectQueueMessageCount = new String[numberOfTables];
    private String[] psSelectRangedQueueMessageCount = new String[numberOfTables];
    private String[] psSelectAllQueueMessageCount = new String[numberOfTables];
    private String[] psSelectQueueMessageCountFromDlc = new String[numberOfTables];
    private String[] psSelectMessageCountInDlc = new String[numberOfTables];
    private String[] psSelectMetadata = new String[numberOfTables];
    private String[] psSelectMetadataRangeFromQueue = new String[numberOfTables];
    private String[] psSelectMetadataRangeFromQueueInDlc = new String[numberOfTables];
    private String[] psSelectMetadataFromQueue = new String[numberOfTables];
    private String[] psSelectMessageIdsFromQueue = new String[numberOfTables];
    private String[] psSelectMetadataInDlcForQueue = new String[numberOfTables];
    private String[] psSelectMetadataInDlc = new String[numberOfTables];
    private String[] psSelectMessageIdsFromMetadataForQueue = new String[numberOfTables];
    private String[] psSelectExpiredMessages = new String[numberOfTables];
    private String[] psSelectExpiredMessagesFromDlc = new String[numberOfTables];
    private String[] psSelectContentPart = new String[numberOfTables];

    // Delete, Clear queries
    private String[] psDeleteMetadataFromQueue = new String[numberOfTables];
    private String[] psDeleteMetadataInDlc = new String[numberOfTables];
    private String[] psDeleteMetadata = new String[numberOfTables];
    private String[] psClearQueueFromMetadata = new String[numberOfTables];
    private String[] psClearDlcQueue = new String[numberOfTables];

    // Update queries
    private String[] psUpdateMetadataQueue = new String[numberOfTables];
    private String[] psUpdateMetadata = new String[numberOfTables];
    private String[] psMoveMetadataToDlc = new String[numberOfTables];
    private String[] psUpdateDlcStatusInExpiryTable = new String[numberOfTables];

    /**
     * Constructor that initialize all the prepared statements, that will change according to the number of tables.
     */
    public RDBMSMultipleTableHandler() {

        for (int tableCount = 0; tableCount < numberOfTables; tableCount++) {

            psInsertMessagePart[tableCount] = ("INSERT INTO "
                    + CONTENT_TABLE + tableCount + " ( " + MESSAGE_ID + " , "
                    + MSG_OFFSET + " , " + MESSAGE_CONTENT + ")" + " VALUES (?, ?, ?)");

            psRetrieveMessagePart[tableCount] = ("SELECT " + MESSAGE_CONTENT
                    + " FROM " + CONTENT_TABLE + tableCount
                    + " WHERE " + MESSAGE_ID + "=?"
                    + " AND " + MSG_OFFSET + "=?");

            psInsertMetadata[tableCount] = ("INSERT INTO "
                    + METADATA_TABLE + tableCount + " ( " + MESSAGE_ID + " , "
                    + QUEUE_ID + " , " + DLC_QUEUE_ID + " , " + METADATA + " ) " + " VALUES ( ?,?,-1,? )");

            psSelectQueueMessageCount[tableCount] = ("SELECT COUNT(" + QUEUE_ID + ") AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1");

            psSelectRangedQueueMessageCount[tableCount] = ("SELECT COUNT(" + MESSAGE_ID + ") AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " AND " + DLC_QUEUE_ID + "=-1");

            psSelectAllQueueMessageCount[tableCount] = ("SELECT " + QUEUE_NAME + ", " + PS_ALIAS_FOR_COUNT
                    + " FROM " + QUEUES_TABLE + " LEFT OUTER JOIN "
                    + "(SELECT " + QUEUE_ID + ", COUNT(" + QUEUE_ID + ") AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + DLC_QUEUE_ID + "=-1"
                    + " GROUP BY " + QUEUE_ID + " ) " + ALIAS_FOR_QUEUES
                    + " ON " + QUEUES_TABLE + "." + QUEUE_ID + "=" + ALIAS_FOR_QUEUES + "." + QUEUE_ID);

            psSelectQueueMessageCountFromDlc[tableCount] = ("SELECT COUNT(" + MESSAGE_ID + ")"
                    + " AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?");

            psSelectMessageCountInDlc[tableCount] = ("SELECT COUNT(" + MESSAGE_ID + ")"
                    + " AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + DLC_QUEUE_ID + "=?");

            psSelectMetadata[tableCount] = ("SELECT " + METADATA
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + MESSAGE_ID + "=?");

            psSelectMetadataRangeFromQueue[tableCount] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataRangeFromQueueInDlc[tableCount] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataFromQueue[tableCount] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMessageIdsFromQueue[tableCount] = ("SELECT " + MESSAGE_ID
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataInDlcForQueue[tableCount] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataInDlc[tableCount] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMessageIdsFromMetadataForQueue[tableCount] = ("SELECT " + MESSAGE_ID
                    + " FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID);

            psDeleteMetadataFromQueue[tableCount] = ("DELETE  FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + "=?");

            psDeleteMetadataInDlc[tableCount] = ("DELETE  FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + MESSAGE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "!= -1");

            psDeleteMetadata[tableCount] = ("DELETE  FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + MESSAGE_ID + "=?");

            psClearQueueFromMetadata[tableCount] = ("DELETE  FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + QUEUE_ID + "=?");

            psClearDlcQueue[tableCount] = ("DELETE  FROM " + METADATA_TABLE + tableCount
                    + " WHERE " + DLC_QUEUE_ID + "=?");

            psUpdateMetadataQueue[tableCount] = ("UPDATE " + METADATA_TABLE + tableCount
                    + " SET " + QUEUE_ID + " = ?"
                    + " WHERE " + MESSAGE_ID + " = ?"
                    + " AND " + QUEUE_ID + " = ?");

            psUpdateMetadata[tableCount] = ("UPDATE " + METADATA_TABLE + tableCount
                    + " SET " + QUEUE_ID + " = ?," + METADATA + " = ?"
                    + " WHERE " + MESSAGE_ID + " = ?"
                    + " AND " + QUEUE_ID + " = ?");

            psMoveMetadataToDlc[tableCount] = ("UPDATE " + METADATA_TABLE + tableCount
                    + " SET " + DLC_QUEUE_ID + "=?"
                    + " WHERE " + MESSAGE_ID + "=?");


            psSelectContentPart[tableCount] = ( "SELECT " + MESSAGE_CONTENT + ", " + MESSAGE_ID + ", " + MSG_OFFSET +
                            " FROM " + CONTENT_TABLE + tableCount +
                            " WHERE " + MESSAGE_ID + " IN (");

            //MB_EXPIRATION_DATA table is ON DELETE CASCADE to MB_METADATA table.
            //So when MB_METADATA# deletes it automatically deletes MB_EXPIRATION_DATA#
            //So need to improvise MB_EXPIRATION_DATA table as to number of tables.
            psSelectExpiredMessages[tableCount] = ( "SELECT " + MESSAGE_ID + "," + DESTINATION_QUEUE
                    + " FROM " + EXPIRATION_TABLE + tableCount
                    + " WHERE " + EXPIRATION_TIME + "<?"
                    + " AND " + MESSAGE_ID + ">=?"
                    + " AND " + DESTINATION_QUEUE + "=?");

            psUpdateDlcStatusInExpiryTable[tableCount] = ("UPDATE " + EXPIRATION_TABLE + tableCount
                    + " SET " + DLC_QUEUE_ID + "=?"
                    + " WHERE " + MESSAGE_ID + "=?");

            psSelectExpiredMessagesFromDlc[tableCount] = ("SELECT " + MESSAGE_ID
                    + " FROM " + EXPIRATION_TABLE + tableCount
                    + " WHERE " + EXPIRATION_TIME + "<?"
                    + " AND " + DLC_QUEUE_ID + " != -1");

        }
    }

    /**
     * get tableID according to the queueID.
     * eg: By default number of tables = 5. If queueID= 1, TableID (for that queue) = 1 % 5 = 1
     * TableIDs are given according to the Round-robin manner.
     *
     * @param queueId that pass.
     * @return tableID relevant to the queueID given.
     */
    private int queueIdToTableId(int queueId) {
        int tableID;
        tableID = queueId % numberOfTables;
        return tableID;
    }

    //Get the String query arrays according to the table id given.
    public String getPsInsertMessagePart(int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psInsertMessagePart[tableID];
    }

    public String getPsInsertMetadata(int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psInsertMetadata[tableID];
    }

    public String getPsSelectContentPart (int queueID) {
        int tableID = queueIdToTableId(queueID);
        String psSelect = psSelectContentPart[tableID];
        return psSelect;
    }

    public String getPsRetrieveMessagePart(int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psRetrieveMessagePart[tableID];
    }

    public String getPsSelectQueueMessageCount(int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectQueueMessageCount[tableID];
    }

    public String getPsSelectRangedQueueMessageCount(int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectRangedQueueMessageCount[tableID];
    }

    public String getPsSelectAllQueueMessageCount (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectAllQueueMessageCount[tableID];
    }

    public String getPsSelectQueueMessageCountFromDlc (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectQueueMessageCountFromDlc[tableID];
    }

    public String getPsSelectMessageCountInDlc (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectMessageCountInDlc[tableID];
    }

    public String getPsSelectMetadata (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectMetadata[tableID];
    }

    public String getPsSelectMetadataRangeFromQueue (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectMetadataRangeFromQueue[tableID];
    }

    public String getPsSelectMetadataRangeFromQueueInDlc (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectMetadataRangeFromQueueInDlc[tableID];
    }

    public String getPsSelectMetadataFromQueue (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectMetadataFromQueue[tableID];
    }

    public String getPsSelectMessageIdsFromQueue (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectMessageIdsFromQueue[tableID];
    }

    public String getPsSelectMetadataInDlcForQueue (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectMetadataInDlcForQueue[tableID];
    }

    public String getPsSelectMetadataInDlc (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectMetadataInDlc[tableID];
    }

    public String getPsSelectMessageIdsFromMetadataForQueue (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectMessageIdsFromMetadataForQueue[tableID];
    }

    public String getPsDeleteMetadataFromQueue (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psDeleteMetadataFromQueue[tableID];
    }

    public String getPsDeleteMetadataInDlc (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psDeleteMetadataInDlc[tableID];
    }

    public String getPsDeleteMetadata (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psDeleteMetadata[tableID];
    }

    public String getPsClearQueueFromMetadata (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psClearQueueFromMetadata[tableID];
    }

    public String getPsClearDlcQueue (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psClearDlcQueue[tableID];
    }

    public String getPsUpdateMetadataQueue(int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psUpdateMetadataQueue[tableID];
    }

    public String getPsUpdateMetadata (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psUpdateMetadata[tableID];
    }

    public String getPsMoveMetadataToDlc (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psMoveMetadataToDlc[tableID];
    }

    public String getPsSelectExpiredMessages (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectExpiredMessages[tableID];
    }

    public String getPsUpdateDlcStatusInExpiryTable (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psUpdateDlcStatusInExpiryTable[tableID];
    }

    public String getPsSelectExpiredMessagesFromDlc (int queueID) {
        int tableID = queueIdToTableId(queueID);
        return psSelectExpiredMessagesFromDlc[tableID];
    }
}
