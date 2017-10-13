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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.andes.configuration.models;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for data access classes (persistance)
 */
@Configuration(description = "Depending on the database type selected in deployment.yaml, you must enable the\n"
        + "relevant Data access classes here. Currently WSO2 MB Supports RDBMS(any RDBMS store).")
public class PersistenceConfig {
    @Element(description = "RDBMS MB Store Configuration.")
    private MessageStoreConfig messageStore = new MessageStoreConfig();

    @Element(description = "RDBMS MB ContextStore Configuration.")
    private ContextStoreConfig contextStore = new ContextStoreConfig();

    @Element(description = "Message caching related configuration.")
    private CacheConfig cache = new CacheConfig();

    @Element(description = "This class decides how unique IDs are generated for the MB node. This id generator is\n"
            + "expected to be thread safe and a implementation of interface\n"
            + "org.wso2.andes.server.cluster.coordination.MessageIdGenerator\n"
            + "NOTE: This is NOT used in MB to generate message IDs.")
    private String idGenerator = "org.wso2.andes.server.cluster.coordination.TimeStampBasedMessageIdGenerator";

    @Element(description = "This is the Task interval (in SECONDS) to check whether communication\n"
            + "is healthy between message store (/Database) and this server instance.")
    private int storeHealthCheckInterval = 10;

    public MessageStoreConfig getMessageStore() {
        return messageStore;
    }

    public ContextStoreConfig getContextStore() {
        return contextStore;
    }

    public CacheConfig getCache() {
        return cache;
    }

    public String getIdGenerator() {
        return idGenerator;
    }

    public int getStoreHealthCheckInterval() {
        return storeHealthCheckInterval;
    }
}