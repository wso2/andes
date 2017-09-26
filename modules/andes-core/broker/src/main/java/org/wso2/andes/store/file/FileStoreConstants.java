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

/**
 * Holds the constant values related to the file store
 */
public class FileStoreConstants {

    public static final String CONNECTOR = "::";

    // LevelDB store configuration
    public static final String BLOCK_SIZE = "blockSize";
    public static final String CACHE_SIZE = "cacheSize";
    public static final String MAX_OPEN_FILES = "maxOpenFiles";
    public static final String WRITE_BUFFER_SIZE = "writeBufferSize";
    public static final String PATH = "path";
    // Bytes per 1MB
    public static final int MB = 1024 * 1024;

    // Message related identifiers
    public static final String MESSAGE = "MESSAGE";
    public static final String MESSAGE_METADATA = "MESSAGE_METADATA";
    public static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";
    public static final String EXPIRATION_TIME = "EXPIRATION_TIME";

    // Destination related identifiers
    public static final String DESTINATION = "DESTINATION";
    public static final String DESTINATION_ID = "DESTINATION_ID";
    public static final String DESTINATION_NAME = "DESTINATION_NAME";
    public static final String LAST_DESTINATION_ID = "LAST_DESTINATION_ID";
    public static final String MESSAGE_COUNT = "MESSAGE_COUNT";

    // Dead letter channel related identifiers
    public static final String DLC = "deadletterchannel";
    public static final String DLC_QUEUE_ID = "DLC_QUEUE_ID";
    public static final String DEFAULT_DLC_QUEUE_ID = "-1";
}
