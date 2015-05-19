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

package org.wso2.andes.store.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import org.wso2.andes.configuration.util.ConfigurationProperties;

import static org.wso2.andes.store.cassandra.HectorConstants.*;

/**
 * Configurations related to Cassandra are parsed through this class.
 * Works as convenient class to parse configurations and read properties from it
 */
public class CassandraConfig {

    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;
    private String replicationFactor;
    private String strategyClass;
    private Integer gcGraceSeconds;
    private String keyspace;

    /**
     * Parse configuration properties and updates the internal properties which can be accessed through getters
     * provided.
      * @param configurationProperties ConfigurationProperties
     */
    void parse(ConfigurationProperties configurationProperties) {
        replicationFactor = configurationProperties.getProperty(PROP_REPLICATION_FACTOR);
        if (getReplicationFactor().isEmpty()) {
            replicationFactor = DEFAULT_REPLICATION_FACTOR;
        }

        strategyClass = configurationProperties.getProperty(PROP_STRATEGY_CLASS);
        if (getStrategyClass().isEmpty()) {
            strategyClass = DEFAULT_STRATEGY_CLASS;
        }

        String consistencyAsString = configurationProperties.getProperty(PROP_READ_CONSISTENCY);
        if (consistencyAsString.isEmpty()) {
            readConsistencyLevel = ConsistencyLevel.ONE;
        } else {
            readConsistencyLevel = ConsistencyLevel.valueOf(consistencyAsString);
        }

        consistencyAsString = configurationProperties.getProperty(PROP_WRITE_CONSISTENCY);
        if (consistencyAsString.isEmpty()) {
            writeConsistencyLevel = ConsistencyLevel.ONE;
        } else {
            writeConsistencyLevel = ConsistencyLevel.valueOf(consistencyAsString);
        }

        String graceSecondsAsString = configurationProperties.getProperty(PROP_GC_GRACE_SECONDS);
        if (graceSecondsAsString.isEmpty()) {
            gcGraceSeconds = Integer.valueOf(DEFAULT_GC_GRACE_SECONDS);
        } else {
            gcGraceSeconds = Integer.valueOf(graceSecondsAsString);
        }

        keyspace = configurationProperties.getProperty(PROP_KEYSPACE);
        if (getKeyspace().isEmpty()) {
            keyspace = DEFAULT_KEYSPACE;
        }
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    public String getReplicationFactor() {
        return replicationFactor;
    }

    public String getStrategyClass() {
        return strategyClass;
    }

    public Integer getGcGraceSeconds() {
        return gcGraceSeconds;
    }

    public String getKeyspace() {
        return keyspace;
    }
}
