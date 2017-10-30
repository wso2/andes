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
package org.wso2.andes.configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.models.BrokerConfiguration;
import org.wso2.carbon.config.ConfigProviderFactory;
import org.wso2.carbon.config.ConfigurationException;
import org.wso2.carbon.config.provider.ConfigProvider;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Broker configuration provider.
 */
public class BrokerConfigurationService {
    private static Log log = LogFactory.getLog(BrokerConfigurationService.class);
    /**
     * path name to conf folder in wso2 runtime
     */
    private static String CONF_FOLDER = "conf";
    /**
     * path name to runtime folder
     */
    private static String RUNTIME = "broker";
    /**
     *file name of global config yaml
     */
    private static String CONFIG_NAME = "deployment.yaml";
    /**
     *  path name to carbon home in wso2 runtime
     */
    private static String CARBON_HOME = "carbon.home";
    /**
     * Broker configuration service to access broker config provider
     */
    private static BrokerConfigurationService brokerConfigurationService = new BrokerConfigurationService();
    /**
     *  Broker configuration provider
     */
    private BrokerConfiguration brokerConfiguration;

    // Get the config file location
    Path deploymentConfigPath = Paths.get(System.getProperty(CARBON_HOME), CONF_FOLDER, RUNTIME,
            CONFIG_NAME);

    private BrokerConfigurationService() {
        // Get configuration provider
        try {
            ConfigProvider configProvider = ConfigProviderFactory.getConfigProvider(deploymentConfigPath);
            brokerConfiguration = configProvider.getConfigurationObject(BrokerConfiguration.class);
        } catch (ConfigurationException e) {
            log.warn("Broker configuration could not be loaded. Using the default configurations.", e);
            brokerConfiguration = new BrokerConfiguration();
        }
    }

    /**
     * Get the broker configuration service instance.
     * @return BrokerConfigurationService
     */
    public static BrokerConfigurationService getInstance() {
        return brokerConfigurationService;
    }

    /**
     * Get the Broker Configuration instance.
     * @return BrokerConfiguration
     */
    public BrokerConfiguration getBrokerConfiguration() {
        return brokerConfiguration;
    }
}