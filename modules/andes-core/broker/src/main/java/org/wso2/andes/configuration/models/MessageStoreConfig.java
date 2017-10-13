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

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration model for Message Store persistence configs
 */
@Configuration(description = "RDBMS MB Store Configuration")
public class MessageStoreConfig {
    private Map property = new HashMap();

    private String _class = "org.wso2.andes.store.rdbms.RDBMSMessageStoreImpl";

    public MessageStoreConfig () {
        property.put("dataSource", "WSO2MBStoreDB");
        property.put("storeUnavailableSQLStateClasses", 8);
        property.put("integrityViolationSQLStateClasses", "23,27,44");
        property.put("dataErrorSQLStateClasses", "21,22");
        property.put("transactionRollbackSQLStateClasses", 40);
    }

    public Map getProperty() {
        return property;
    }

    public String getClassConfig() {
        return _class;
    }
}