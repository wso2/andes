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
package org.wso2.andes.server.plugins;

import org.apache.log4j.Logger;
import org.wso2.andes.server.configuration.ServerConfiguration;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class Activator implements BundleActivator
{
    private static final Logger _logger = Logger.getLogger(Activator.class);
    
    private BundleContext _context = null;
    
    public void start(BundleContext ctx) throws Exception
    {
        _context = ctx;
        _logger.info("Registering bundle: " + _context.getBundle().getSymbolicName());
         ctx.registerService(ServerConfiguration.class.getName(), ApplicationRegistry.getInstance().getConfiguration(), null);
    }

    public void stop(BundleContext ctx) throws Exception
    {
        _logger.info("Stopping bundle: " + _context.getBundle().getSymbolicName());
        _context = null;
    }

    public BundleContext getContext()
    {
        return _context;
    }
}
