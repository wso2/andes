/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/

package org.wso2.andes.store.file;

import org.apache.log4j.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteBatch;
import org.wso2.andes.kernel.DurableStoreConnection;

import java.io.IOException;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

/**
 * Contains utilility methods required for  {@link FileMessageStoreImpl}
 */

public class FileStoreUtils {

    private static final Logger log = Logger.getLogger(FileStoreUtils.class);

    /**
     * Inserts a test record
     *
     * @param testString a string value
     * @param testTime   a time value
     * @return true if the test was successful.
     */
    public boolean testInsert(DurableStoreConnection connection, String testString, long testTime) {
        boolean canInsert;

        DB brokerStore = (DB) connection.getConnection();
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            batch.put(bytes(testString), bytes(Long.toString(testTime)));
            brokerStore.write(batch);
            canInsert = true;
        } catch (DBException e) {
            canInsert = false;
        } finally {
            closeWriteBatch(batch);
        }

        return canInsert;
    }

    /**
     * Reads a test record
     *
     * @param testString a string value
     * @param testTime   a time value
     * @return true if the test was successful.
     */
    public boolean testRead(DurableStoreConnection connection, String testString, long testTime) {
        boolean canRead = false;

        DB brokerStore = (DB) connection.getConnection();

        try {
            if (testTime == Long.parseLong(asString(brokerStore.get(bytes(testString))))) {
                canRead = true;
            }
        } catch (DBException e) {
            canRead = false;
        }

        return canRead;
    }

    /**
     * Delete a test record
     *
     * @param testString a string value
     * @return true if the test was successful.
     */
    public boolean testDelete(DurableStoreConnection connection, String testString) {

        boolean canDelete;

        DB brokerStore = (DB) connection.getConnection();
        WriteBatch batch = brokerStore.createWriteBatch();

        try {
            batch.delete(bytes(testString));
            brokerStore.write(batch);
            canDelete = true;
        } catch (DBException e) {
            canDelete = false;
        } finally {
            closeWriteBatch(batch);
        }

        return canDelete;
    }

    /**
     * Close the given write batch
     *
     * @param batch write batch which needs to be closed
     */
    private void closeWriteBatch(WriteBatch batch) {
        try {
            batch.close();
        } catch (IOException e) {
            log.error("Write batch closing failed.", e);
        }
    }
}
