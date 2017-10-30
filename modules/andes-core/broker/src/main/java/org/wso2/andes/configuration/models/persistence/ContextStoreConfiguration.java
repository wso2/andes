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
@Configuration(description = "Context store related configs")
public class ContextStoreConfiguration {

    @Element(description = "Context store impl class name")
    private String className = "org.wso2.andes.store.rdbms.RDBMSAndesContextStoreImpl";

    @Element(description = "Datasource name of context store")
    private String dataSource = "WSO2MBStoreDB";

    @Element(description = "Property of context store")
    private String storeUnavailableSQLStateClasses = "08";

    @Element(description = "Property of context store")
    private String integrityViolationSQLStateClasses = "23,27,44";

    @Element(description = "Property of context store")
    private String dataErrorSQLStateClasses = "21,22";

    @Element(description = "Property of context store")
    private String transactionRollbackSQLStateClasses = "40";

    public String getClassName() {
        return className;
    }

    public String getDataSource() {
        return dataSource;
    }

    public String getStoreUnavailableSQLStateClasses() {
        return storeUnavailableSQLStateClasses;
    }

    public String getIntegrityViolationSQLStateClasses() {
        return integrityViolationSQLStateClasses;
    }

    public String getDataErrorSQLStateClasses() {
        return dataErrorSQLStateClasses;
    }

    public String getTransactionRollbackSQLStateClasses() {
        return transactionRollbackSQLStateClasses;
    }
}
