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
import org.iq80.leveldb.Options;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;

import java.io.File;
import java.io.IOException;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import static org.wso2.andes.store.file.FileStoreConstants.BLOCK_SIZE;
import static org.wso2.andes.store.file.FileStoreConstants.CACHE_SIZE;
import static org.wso2.andes.store.file.FileStoreConstants.MAX_OPEN_FILES;
import static org.wso2.andes.store.file.FileStoreConstants.PATH;
import static org.wso2.andes.store.file.FileStoreConstants.WRITE_BUFFER_SIZE;
import static org.wso2.andes.store.file.FileStoreConstants.MB;

/**
 * File connection class. Initiate file based store.
 */
public class FileStoreConnection extends DurableStoreConnection {
    private static final Logger logger = Logger.getLogger(FileStoreConnection.class);
    private DB brokerStore;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(ConfigurationProperties connectionProperties) throws AndesException {
        super.initialize(connectionProperties);

        try {
            Options options = new Options();

            options.blockSize(Integer.parseInt(connectionProperties.getProperty(BLOCK_SIZE)));
            options.cacheSize(Integer.parseInt(connectionProperties.getProperty(CACHE_SIZE)) * MB);
            options.maxOpenFiles(Integer.parseInt(connectionProperties.getProperty(MAX_OPEN_FILES)) * MB);
            options.writeBufferSize(Integer.parseInt(connectionProperties.getProperty(WRITE_BUFFER_SIZE))
                    * MB);
            options.createIfMissing(true);

            this.brokerStore = factory.open(new File(connectionProperties.getProperty(PATH)), options);
        } catch (IOException e) {
            logger.error("LevelDB store connection initiation failed.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            if (brokerStore != null) {
                brokerStore.close();
            }
        } catch (IOException e) {
            logger.error("File message broker store closing failed", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getConnection() {
        return this.brokerStore;
    }
}
