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
package org.wso2.andes.server.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.wso2.andes.server.configuration.plugins.ConfigurationPlugin;
import org.wso2.andes.server.configuration.plugins.ConfigurationPluginFactory;
import org.wso2.andes.server.registry.ApplicationRegistry;

public class ConfigurationManager
{
    public List<ConfigurationPlugin> getConfigurationPlugins(String configurationElement, Configuration configuration) throws ConfigurationException
    {
        List<ConfigurationPlugin> plugins = new ArrayList<ConfigurationPlugin>();
        Map<List<String>, ConfigurationPluginFactory> factories =
            ApplicationRegistry.getInstance().getPluginManager().getConfigurationPlugins();

        for (Entry<List<String>, ConfigurationPluginFactory> entry : factories.entrySet())
        {
            if (entry.getKey().contains(configurationElement))
            {
                ConfigurationPluginFactory factory = entry.getValue();
                plugins.add(factory.newInstance(configurationElement, configuration));
            }
        }
        
        return plugins;
    }
}
