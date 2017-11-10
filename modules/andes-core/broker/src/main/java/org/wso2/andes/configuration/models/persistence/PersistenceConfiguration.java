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
package org.wso2.andes.configuration.models.persistence;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for persistence related configs
 */
@Configuration(description = "Persistence related configs")
public class PersistenceConfiguration {

    @Element(description = "This class decides how unique IDs are generated for the MB node. This id generator is\n"
            + "expected to be thread safe and a implementation of interface\n"
            + "org.wso2.andes.server.cluster.coordination.MessageIdGenerator")
    private String idGenerator = "org.wso2.andes.server.cluster.coordination.TimeStampBasedMessageIdGenerator";

    @Element(description = "This is the Task interval (in SECONDS) to check whether communication \n"
            + "is healthy between message store (/Database) and this server instance")
    private int storeHealthCheckInterval = 10;

    @Element(description = "Message store related configuration")
    private MessageStoreConfiguration messageStore = new MessageStoreConfiguration();

    @Element(description = "Context store related configuration")
    private ContextStoreConfiguration contextStore = new ContextStoreConfiguration();

    @Element(description = "Cache related configuration")
    private CacheConfiguration cache = new CacheConfiguration();

    public String getIdGenerator() {
        return idGenerator;
    }

    public int getStoreHealthCheckInterval() {
        return storeHealthCheckInterval;
    }

    public MessageStoreConfiguration getMessageStore() {
        return messageStore;
    }

    public ContextStoreConfiguration getContextStore() {
        return contextStore;
    }

    public CacheConfiguration getCache() {
        return cache;
    }
}
