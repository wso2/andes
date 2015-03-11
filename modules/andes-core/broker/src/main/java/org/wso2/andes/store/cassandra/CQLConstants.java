/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store.cassandra;


/**
 * Constants related CQL based store implementation.
 * @see CQLBasedMessageStoreImpl and {@link CQLBasedAndesContextStoreImpl}
 */
public class CQLConstants {

    /**
     * Connection property to get the jndi lookup name (value) of the data source
     */
    public static final String PROP_JNDI_LOOKUP_NAME = "dataSource";
    
    
    /** Message Store tables */
    protected static final String CONTENT_TABLE = "MB_CONTENT";
    protected static final String METADATA_TABLE = "MB_METADATA";
    
    protected static final String MSG_STORE_STATUS_TABLE = "MB_MSG_STORE_STATUS";
  
    /** Message Store table columns */
    protected static final String MESSAGE_ID = "MESSAGE_ID";

    protected static final String QUEUE_NAME = "QUEUE_NAME";
    protected static final String METADATA = "MESSAGE_METADATA";
    protected static final String MESSAGE_OFFSET = "CONTENT_OFFSET";
    protected static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";

    protected static final String TIME_STAMP = "TIME_STAMP";

    protected static final String NODE_ID = "NODE_ID";

    
    
    //CQL batch has a limitation of number of entries we need to ensure that this limit will not exceed
    protected static final int MAX_MESSAGE_BATCH_SIZE = 1000;

    // Message expiration feature moved to MB 3.1.0
    /*
    protected static final String EXPIRATION_TABLE = "MB_EXPIRATION_DATA";
    protected static final String EXPIRATION_TIME = "EXPIRATION_TIME";
    protected static final String DESTINATION_QUEUE = "MESSAGE_DESTINATION";
    protected static final String EXPIRATION_TIME_RANGE = "TIME_RANGE";
     */
}
