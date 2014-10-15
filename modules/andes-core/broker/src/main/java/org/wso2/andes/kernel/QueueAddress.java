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
package org.wso2.andes.kernel;

public class QueueAddress {

    public enum QueueType {

        QUEUE_NODE_QUEUE ("NodeQueue"),
        TOPIC_NODE_QUEUE ("TopicNodeQueue"),
        GLOBAL_QUEUE ("GlobalQueue"),
        DESTINATION_QUEUE ("DestinationQueue");

        private final String value;

        private QueueType(final String value) {
            this.value = value;
        }

        public String getType(){
            return value;
        }

    }

    public QueueType queueType;
    public String queueName;

    public QueueAddress(QueueType queueType, String queueName) {
        this.queueType = queueType;
        this.queueName = queueName;
    }
}
