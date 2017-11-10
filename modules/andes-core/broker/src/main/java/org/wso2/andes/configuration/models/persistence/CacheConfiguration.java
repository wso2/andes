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
 * Configuration model for persistence related configs.
 */
@Configuration(description = "Cache related configs")
public class CacheConfiguration {

    @Element(description = "Size of the messages cache in MBs. Setting '0' will disable the cache")
    private int size = 256;

    @Element(description = "Expected concurrency for the cache (4 is guava default)")
    private int concurrencyLevel = 4;

    @Element(description = "Number of seconds cache will keep messages after they are added (unless they are consumed "
            + "and deleted)")
    private int expirySeconds = 2;

    @Element(description = "Reference type used to hold messages in memory. \n"
            + " weak - Using java weak references ( - results higher cache misses) \n"
            + "strong - ordinary references ( - higher cache hits, but not good if server \n"
            + "is going to run with limited heap size + under severe load).")
    private String valueReferenceType = "strong";

    @Element(description = "Prints cache statistics in 2 minute intervals in carbon log ( and console)")
    private boolean printStats = false;

    public int getSize() {
        return size;
    }

    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }

    public int getExpirySeconds() {
        return expirySeconds;
    }

    public String getValueReferenceType() {
        return valueReferenceType;
    }

    public boolean isPrintStats() {
        return printStats;
    }

}
