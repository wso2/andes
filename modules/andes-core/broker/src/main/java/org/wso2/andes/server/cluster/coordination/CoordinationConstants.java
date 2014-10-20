/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.andes.server.cluster.coordination;

/**
 * This class contains all constants used in Hazelcast coordination.
 */
public final class CoordinationConstants {
    /**
     * Hazelcast distributed topic name to send subscription changed notifications to cluster nodes.
     */
    public static String HAZELCAST_SUBSCRIPTION_CHANGED_NOTIFIER_TOPIC_NAME = "SUBSCRIPTION_CHANGED";

    /**
     * Hazelcast distributed topic name to send queue purged notifications to cluster nodes.
     */
    public static String HAZELCAST_QUEUE_CHANGED_NOTIFIER_TOPIC_NAME = "QUEUE_CHANGED";

    /**
     * Hazelcast distributed topic name to send exchange notifications to cluster nodes
     */
    public static String HAZELCAST_EXCHANGE_CHANGED_NOTIFIER_TOPIC_NAME = "EXCHANGE_CHANGED";

    /**
     * Hazelcast distributed topic name to send binding notifications to cluster nodes
     */
    public static String HAZELCAST_BINDING_CHANGED_NOTIFIER_TOPIC_NAME = "BINDING_CHANGED";

    /**
     * Hazelcast distributed topic name to send coordinator change notifications to cluster nodes
     */
    public static String HAZELCAST_COORDINATOR_CHANGED_NOTIFIER_TOPIC_NAME = "COORDINATOR_CHANGED";

    /**
     * Prefix to generate node ID
     */
    public static String NODE_NAME_PREFIX = "NODE";

    /**
     * Hazelcast ID generator name to generate an unique ID for each node.
     */
    public static String HAZELCAST_ID_GENERATOR_NAME = "UNIQUE_ID_TO_GENERATE_MESSAGE_ID";

    /**
     * Name of the distributed map to keep track of non-empty slots which are unassigned from
     * other nodes
     */
    public static String UNASSIGNED_SLOT_MAP_NAME = "unassignedSlotsMap";

    /**
     *Name of the distributed map to store message ID list against queue name
     */
    public static String SLOT_ID_MAP_NAME = "slotIdMap";

    /**
     *Name of the distributed map to store last assigned message ID against queue name
     */
    public static String LAST_ASSIGNED_ID_MAP_NAME = "lastAssignedIDMap";

    /**
     * Name of the distributed map to store list of assigned slots against queue name
     */
    public static String SLOT_ASSIGNMENT_MAP_NAME = "slotAssignmentMap";

    public static String THRIFT_SERVER_DETAILS_MAP_NAME = "thriftServerDetailsMap";


}
