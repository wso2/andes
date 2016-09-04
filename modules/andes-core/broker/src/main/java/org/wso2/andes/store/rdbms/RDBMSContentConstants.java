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

public class RDBMSContentConstants extends RDBMSConstants{

    protected String PS_INSERT_MESSAGE_PART =
            "INSERT INTO " + CONTENT_TABLE + "("
                    + MESSAGE_ID + ","
                    + MSG_OFFSET + ","
                    + MESSAGE_CONTENT + ") VALUES (?, ?, ?)";

    protected String PS_RETRIEVE_MESSAGE_PART =
            "SELECT " + MESSAGE_CONTENT
                    + " FROM " + CONTENT_TABLE
                    + " WHERE " + MESSAGE_ID + "=?"
                    + " AND " + MSG_OFFSET + "=?";

    /**
     * Partially created prepared statement to retrieve content of multiple messages using IN operator
     * this will be completed on the fly when the request comes
     */
    protected String PS_SELECT_CONTENT_PART =
            "SELECT " + MESSAGE_CONTENT + ", " + MESSAGE_ID + ", " + MSG_OFFSET +
                    " FROM " + CONTENT_TABLE +
                    " WHERE " + MESSAGE_ID + " IN (";

    public RDBMSContentConstants(int tableId) {

        String derivedTableName = "MB_CONTENT" + tableId;

        this.PS_INSERT_MESSAGE_PART = this.PS_INSERT_MESSAGE_PART.replace(CONTENT_TABLE, derivedTableName);
        this.PS_RETRIEVE_MESSAGE_PART = this.PS_RETRIEVE_MESSAGE_PART.replace(CONTENT_TABLE, derivedTableName);
        this.PS_SELECT_CONTENT_PART = this.PS_SELECT_CONTENT_PART.replace(CONTENT_TABLE, derivedTableName);
    }


}
