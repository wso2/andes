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
package org.wso2.andes.server.security;

import org.apache.log4j.Logger;
import org.wso2.andes.server.configuration.plugins.ConfigurationPlugin;
import org.wso2.andes.server.security.access.ObjectProperties;
import org.wso2.andes.server.security.access.ObjectType;
import org.wso2.andes.server.security.access.Operation;

/**
 * This is intended as the parent for all simple plugins.
 */
public abstract class AbstractPlugin implements SecurityPlugin
{
	protected final Logger _logger = Logger.getLogger(getClass());
    
    protected ConfigurationPlugin _config;
	
	public Result getDefault()
	{
		return Result.ABSTAIN;
	}
    
    public abstract Result access(ObjectType object, Object instance);

    public abstract Result authorise(Operation operation, ObjectType object, ObjectProperties properties);

    public void configure(ConfigurationPlugin config)
    {
        _config = config;
    }

}
