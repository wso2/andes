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

import org.wso2.andes.server.security.AbstractPlugin;
import org.wso2.andes.server.security.Result;
import org.wso2.andes.server.security.SecurityPlugin;
import org.wso2.andes.server.security.access.ObjectProperties;
import org.wso2.andes.server.security.access.ObjectType;
import org.wso2.andes.server.security.access.Operation;

/**
 * This {@link SecurityPlugin} simply abstains from all authorisation requests and ignores configuration.
 */
public abstract class BasicPlugin extends AbstractPlugin
{
    public Result access(ObjectType objectType, Object instance)
    {
        return getDefault();
    }
    
    public Result authorise(Operation operation, ObjectType objectType, ObjectProperties properties)
    {
        return getDefault();
    }
}
