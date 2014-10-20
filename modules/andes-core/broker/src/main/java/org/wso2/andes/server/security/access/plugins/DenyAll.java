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
package org.wso2.andes.server.security.access.plugins;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.wso2.andes.server.configuration.plugins.ConfigurationPlugin;
import org.wso2.andes.server.configuration.plugins.ConfigurationPluginFactory;
import org.wso2.andes.server.security.Result;
import org.wso2.andes.server.security.SecurityPluginFactory;

/** Always Deny. */
public class DenyAll extends BasicPlugin
{
    public static class DenyAllConfiguration extends ConfigurationPlugin {
        public static final ConfigurationPluginFactory FACTORY = new ConfigurationPluginFactory()
        {
            public List<String> getParentPaths()
            {
                return Arrays.asList("security.deny-all", "virtualhosts.virtualhost.security.deny-all");
            }

            public ConfigurationPlugin newInstance(String path, Configuration config) throws ConfigurationException
            {
                ConfigurationPlugin instance = new DenyAllConfiguration();
                instance.setConfiguration(path, config);
                return instance;
            }
        };
        
        public String[] getElementsProcessed()
        {
            return new String[] { "" };
        }

        public void validateConfiguration() throws ConfigurationException
        {
            if (!_configuration.isEmpty())
            {
                throw new ConfigurationException("deny-all section takes no elements.");
            }
        }

    }
    
    public static final SecurityPluginFactory<DenyAll> FACTORY = new SecurityPluginFactory<DenyAll>()
    {
        public DenyAll newInstance(ConfigurationPlugin config) throws ConfigurationException
        {
            DenyAllConfiguration configuration = config.getConfiguration(DenyAllConfiguration.class.getName());

            // If there is no configuration for this plugin then don't load it.
            if (configuration == null)
            {
                return null;
            }

            DenyAll plugin = new DenyAll();
            plugin.configure(configuration);
            return plugin;
        }

        public String getPluginName()
        {
            return DenyAll.class.getName();
        }

        public Class<DenyAll> getPluginClass()
        {
            return DenyAll.class;
        }
    };
	
    @Override
	public Result getDefault()
	{
		return Result.DENIED;
    }

}
