/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.matrics;

/**
 * Will maintain the list of metrics
 */
public class MetricsConstants {

    /*DB OPERATIONS*/

    /**
     * Add message content to the message store
     */
    public static final String ADD_MESSAGE_PART = "db_store_message_part";
    
    /**
     * Add message meta data list to the database
     */
    public static final String ADD_META_DATA_LIST = "db_store_mata_data_list";
    /**
     * Add message meta data to database
     */
    public static final String ADD_META_DATA = "db_store_mata_data_list";
    
    /**
     * Add meta data to queue list
     */
    public static final String ADD_META_DATA_TO_QUEUE_LIST = "db_store_mata_data_queue_list";
    /**
     * Add meta data to queue
     */
    public static final String ADD_META_DATA_TO_QUEUE = "db_store_mata_data_queue";
    /**
     * Add meta data as a batch
     */
    public static final String ADD_META_DATA_TO_BATCH = "db_store_mata_data_batch";
    /**
     * Get message content as batch
     */
    public static final String GET_CONTENT_BATCH = "db_get_content_as_batch";
    /**
     * Get message meta data
     */
    public static final String GET_META_DATA = "db_get_mata_data";
    /**
     * Get message meta data list
     */
    public static final String GET_META_DATA_LIST = "db_get_mata_data_list";
    /**
     * Get message content
     */
    public static final String GET_CONTENT = "db_get_content";
    /**
     * Get next message meta data from queue
     */
    public static final String GET_NEXT_MESSAGE_METADATA_FROM_QUEUE = "db_get_next_mata_data";
    /**
     * Delete message part
     */
    public static final String DELETE_MESSAGE_PART = "db_delete_message_part";
    /**
     * Delete message meta data from queue
     */
    public static final String DELETE_MESSAGE_META_DATA_FROM_QUEUE = "db_delete_message_meat_data_from_queue";
    /**
     * Update meta data
     */
    public static final String UPDATE_META_DATA_INFORMATION = "db_update_meta_info";

    /*Buffer Values*/

    /**
     * At a given time the number of messages in the inbound disruptor ring
     */
    public static final String DISRUPTOR_INBOUND_RING = "buffer_in_bound_ring_size";
    /**
     * At a given time the number of messages which have being acknowledged in the inbound ring
     */
    public static final String DISRUPTOR_MESSAGE_ACK = "buffer_message_ack_count";
    /**
     * At a given time the number of messages in the outbound ring
     */
    public static final String DISRUPTOR_OUTBOUND_RING = "buffer_out_bound_ring";


}
