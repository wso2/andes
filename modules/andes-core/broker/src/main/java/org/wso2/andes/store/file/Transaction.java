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
import org.iq80.leveldb.WriteBatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Use to apply changes in batches, ignore the whole batch if one operation fails
 */
public class Transaction {

    private static final Logger log = Logger.getLogger(Transaction.class);

    /**
     * WriteBatch instance which accumulates get, delete operations to commit
     */
    private WriteBatch batch;

    /**
     * Map which stores the updated values of keys which were changed during the transaction
     */
    private Map<String, byte[]> updates = new HashMap<>();


    /**
     * Instantiate write batch
     *
     * @param db LevelDB store instance which creates the write batch.
     */
    public Transaction(DB db) {
        this.batch = db.createWriteBatch();
    }

    /**
     * Add put operation to the write batch
     *
     * @param key   name of the key, related to the operation
     * @param value value of the key
     */
    public void put(byte[] key, byte[] value) {
        this.batch.put(key, value);
    }

    /**
     * Add delete operation to the write batch
     *
     * @param key name of the key, related to the operation
     */
    public void delete(byte[] key) {
        this.batch.delete(key);
    }

    /**
     * Insert keys which will be updated during the transaction to the map
     *
     * @param key   name of the key which will be updated
     * @param value updated value of the key
     */
    public void setKey(String key, byte[] value) {
        updates.put(key, value);
        batch.put(key.getBytes(), value);
    }

    /**
     * Get the updated value of a key which is stored in the map
     *
     * @param key name of the key
     */
    public byte[] getKey(String key) {
        return updates.get(key);
    }

    /**
     * Write batch to the db
     *
     * @param db LevelDB store instance which changes will be written
     */
    public void commit(DB db) {
        db.write(this.batch);
    }

    /**
     * Close the write batch
     */
    public void close() {
        try {
            this.batch.close();
        } catch (IOException e) {
            log.error("Error occured while closing ", e);
        }
    }
}
