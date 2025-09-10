/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.server.security.access.plugins;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.CombinedConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.configuration2.tree.OverrideCombiner;
import org.apache.commons.configuration2.XMLConfiguration;
import org.wso2.andes.configuration.qpid.plugins.ConfigurationPlugin;
import org.wso2.andes.configuration.qpid.plugins.ConfigurationPluginFactory;
import org.apache.commons.configuration2.io.FileHandler;
import org.wso2.andes.server.security.Result;
import org.wso2.andes.server.security.access.config.FirewallRule;

public class FirewallConfiguration extends ConfigurationPlugin
{
    CombinedConfiguration _finalConfig;

    public static final ConfigurationPluginFactory FACTORY = new ConfigurationPluginFactory()
    {
        public ConfigurationPlugin newInstance(String path, Configuration config) throws ConfigurationException
        {
            ConfigurationPlugin instance = new FirewallConfiguration();
            instance.setConfiguration(path, config);
            return instance;
        }

        public List<String> getParentPaths()
        {
            return Arrays.asList("security.firewall", "virtualhosts.virtualhost.security.firewall");
        }
    };

    public String[] getElementsProcessed()
    {
        return new String[] { "" };
    }

    public Configuration getConfiguration()
    {
        return _finalConfig;
    }

    public Result getDefaultAction()
    {
        String defaultAction = _configuration.getString("[@default-action]");
        if (defaultAction == null)
        {
            return Result.ABSTAIN;
        }
        else if (defaultAction.equalsIgnoreCase(FirewallRule.ALLOW))
        {
            return Result.ALLOWED;
        }
        else
        {
            return Result.DENIED;
        }
    }



    @Override
    public void validateConfiguration() throws ConfigurationException
    {
        // Start with the main configuration you already have
        CombinedConfiguration finalConfig = new CombinedConfiguration(new OverrideCombiner());
        finalConfig.addConfiguration(_configuration); // _configuration is your XMLConfiguration (or Configuration)

        // Load and add child XML files referenced as <xml fileName="..."/>
        // In 2.x you can use a typed getter; if you prefer, List<Object> + cast also works.
        List<String> subFiles = _configuration.getList(String.class, "xml[@fileName]");
        for (String path : subFiles) {
            XMLConfiguration child = new XMLConfiguration();
            FileHandler fh = new FileHandler(child);

            // Optional: if your paths are relative to the parent file's directory,
            // set the same base path on the handler (uncomment if you know the base path).
            // fh.setBasePath(_configuration.getFileLocator().getBasePath());

            fh.load(path);                // load child XML
            finalConfig.addConfiguration(child);  // add to the combined config
        }

        _finalConfig = finalConfig;

        // Validate: either rules have an access attribute, or a root default-action is defined
        boolean hasRuleAccess = !finalConfig.getList("rule[@access]").isEmpty();
        boolean hasDefault = _configuration.getString("[@default-action]") != null;

        if (!hasRuleAccess && !hasDefault) {
            throw new ConfigurationException("No rules or default-action found in firewall configuration.");
        }
    }

}
