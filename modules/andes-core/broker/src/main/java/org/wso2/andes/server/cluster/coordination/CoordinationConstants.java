/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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


public final class CoordinationConstants {

    public static String QUEUE_WORKER_COORDINATION_PARENT = "/queue_workers_parent";

    public static String QUEUE_WORKER_NODE = "/queue_worker_node";

    public static String QUEUE_FAIL_OVER_HANDLING_PARENT = "/queue_fail_over_handling_parent";

    public static String QUEUE_FAIL_OVER_HANDLING_NODE = "/queue_fail_over_handling_node";

    public static String QUEUE_RESOURCE_LOCK_PARENT = "/queue_resource_lock_parent";

    public static String QUEUE_RESOURCE_LOCK_NODE = "/queue_resource_lock_node";

    public static final String SUBSCRIPTION_COORDINATION_PARENT = "/subscription_coordination_parent";
    
    public static final String TOPIC_SUBSCRIPTION_COORDINATION_PARENT = "/topic_subscription_coordination_parent";

    public static final String NODE_CHANGED_PREFIX = "NODE:";

    public static final String QUEUES_CHANGED_PREFIX = "QUEUE:";

    public static String NODE_SEPARATOR = "/";

    public static String SUBSCRIPTION_CHANGED_NOTIFIER_TOPIC_NAME = "SUBSCRIPTION";

    public static String QUEUE_CHANGED_NOTIFIER_TOPIC_NAME = "QUEUE";

    public static String NODE_NAME_PREFIX = "NODE_";

    public static String UNIQUE_ID_FOR_NODE = "UNIQUE_ID";

}
