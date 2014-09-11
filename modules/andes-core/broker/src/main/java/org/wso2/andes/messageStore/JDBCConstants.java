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
    public static final String MESSAGES_TABLE = "messages";
    public static final String METADATA_TABLE = "metadata";
    public static final String QUEUES_TABLE = "queues";
    public static final String REF_COUNT_TABLE = "reference_counts";
    public static final String EXPIRATION_TABLE = "expiration_data";

    // columns
    public static final String MESSAGE_ID = "message_id";
    public static final String QUEUE_ID = "queue_id";
    public static final String QUEUE_NAME = "name";
    public static final String REF_COUNT = "reference_count";
    public static final String METADATA = "data";
    public static final String MSG_OFFSET = "offset";
    public static final String MESSAGE_CONTENT = "content";
    public static final String EXPIRATION_TIME = "expiration_time";
    public static final String DESTINATION_QUEUE = "destination";

}
