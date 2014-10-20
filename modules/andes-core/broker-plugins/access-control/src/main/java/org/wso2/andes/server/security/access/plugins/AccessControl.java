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

import javax.security.auth.Subject;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.wso2.andes.server.configuration.plugins.ConfigurationPlugin;
import org.wso2.andes.server.security.AbstractPlugin;
import org.wso2.andes.server.security.Result;
import org.wso2.andes.server.security.SecurityManager;
import org.wso2.andes.server.security.SecurityPluginFactory;
import org.wso2.andes.server.security.access.ObjectProperties;
import org.wso2.andes.server.security.access.ObjectType;
import org.wso2.andes.server.security.access.Operation;
import org.wso2.andes.server.security.access.config.RuleSet;

/**
 * This access control plugin implements version two plain text access control.
 */
public class AccessControl extends AbstractPlugin
{
    public static final Logger _logger = Logger.getLogger(AccessControl.class);
    
    private RuleSet _ruleSet;
    
    public static final SecurityPluginFactory<AccessControl> FACTORY = new SecurityPluginFactory<AccessControl>()
    {
        public Class<AccessControl> getPluginClass()
        {
            return AccessControl.class;
        }

        public String getPluginName()
        {
            return AccessControl.class.getName();
        }

        public AccessControl newInstance(ConfigurationPlugin config) throws ConfigurationException
        {
            AccessControlConfiguration configuration = config.getConfiguration(AccessControlConfiguration.class.getName());

            // If there is no configuration for this plugin then don't load it.
            if (configuration == null)
            {
                return null;
            }

            AccessControl plugin = new AccessControl();
            plugin.configure(configuration);
            return plugin;
        }
    };

    public Result getDefault()
    {
        return _ruleSet.getDefault();
    }

    /**
     * Object instance access authorisation.
     *
	 * Delegate to the {@link #authorise(Operation, ObjectType, ObjectProperties)} method, with
     * the operation set to ACCESS and no object properties.
	 */
    public Result access(ObjectType objectType, Object instance)
    {
        return authorise(Operation.ACCESS, objectType, ObjectProperties.EMPTY);
    }
    
    /**
     * Check if an operation is authorised by asking the  configuration object about the access
     * control rules granted to the current thread's {@link Subject}. If there is no current
     * user the plugin will abstain.
     */
    public Result authorise(Operation operation, ObjectType objectType, ObjectProperties properties)
    {
        final Subject subject = SecurityManager.getThreadSubject();
        // Abstain if there is no subject/principal associated with this thread
        if (subject == null  || subject.getPrincipals().size() == 0)
        {
            return Result.ABSTAIN;
        }
 
        return  _ruleSet.check(subject, operation, objectType, properties);
    }

    public void configure(ConfigurationPlugin config)
    {
        super.configure(config);

        AccessControlConfiguration accessConfig = (AccessControlConfiguration) _config;

        _ruleSet = accessConfig.getRuleSet();
    }
}
