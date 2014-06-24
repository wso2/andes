/*
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
 */
package org.wso2.andes.server.plugins;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.felix.framework.Felix;
import org.apache.felix.framework.util.StringMap;
import org.apache.log4j.Logger;
import org.wso2.andes.common.Closeable;
import org.wso2.andes.server.configuration.TopicConfiguration;
import org.wso2.andes.server.configuration.plugins.ConfigurationPluginFactory;
import org.wso2.andes.server.configuration.plugins.SlowConsumerDetectionConfiguration.SlowConsumerDetectionConfigurationFactory;
import org.wso2.andes.server.configuration.plugins.SlowConsumerDetectionPolicyConfiguration.SlowConsumerDetectionPolicyConfigurationFactory;
import org.wso2.andes.server.configuration.plugins.SlowConsumerDetectionQueueConfiguration.SlowConsumerDetectionQueueConfigurationFactory;
import org.wso2.andes.server.exchange.ExchangeType;
import org.wso2.andes.server.security.SecurityManager;
import org.wso2.andes.server.security.SecurityPluginFactory;
import org.wso2.andes.server.security.access.plugins.AllowAll;
import org.wso2.andes.server.security.access.plugins.DenyAll;
import org.wso2.andes.server.security.access.plugins.LegacyAccess;
import org.wso2.andes.server.security.auth.manager.AuthenticationManagerPluginFactory;
import org.wso2.andes.server.security.auth.manager.PrincipalDatabaseAuthenticationManager;
import org.wso2.andes.server.virtualhost.plugins.SlowConsumerDetection;
import org.wso2.andes.server.virtualhost.plugins.VirtualHostPluginFactory;
import org.wso2.andes.server.virtualhost.plugins.policies.TopicDeletePolicy;
import org.wso2.andes.policies.SlowConsumerPolicyPluginFactory;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.launch.Framework;
import org.osgi.util.tracker.ServiceTracker;

import java.io.File;
import java.util.*;

import static org.apache.felix.framework.util.FelixConstants.SYSTEMBUNDLE_ACTIVATORS_PROP;
import static org.apache.felix.main.AutoProcessor.*;
import static org.osgi.framework.Constants.*;

/**
 * Provides access to pluggable elements, such as exchanges
 */
@SuppressWarnings("unchecked")
public class PluginManager implements Closeable
{
    private static final Logger _logger = Logger.getLogger(PluginManager.class);

    private static final int FELIX_STOP_TIMEOUT = 30000;
    private static final String QPID_VER_SUFFIX = "version=0.13,";
    private static BundleContext _bundleContext = null;

    private Framework _felix;

    private ServiceTracker _exchangeTracker = null;
    private ServiceTracker _securityTracker = null;
    private ServiceTracker _configTracker = null;
    private ServiceTracker _virtualHostTracker = null;
    private ServiceTracker _policyTracker = null;
    private ServiceTracker _authenticationManagerTracker = null;

    private Activator _activator;

    private Map<String, SecurityPluginFactory> _securityPlugins = new HashMap<String, SecurityPluginFactory>();
    private Map<List<String>, ConfigurationPluginFactory> _configPlugins = new IdentityHashMap<List<String>, ConfigurationPluginFactory>();
    private Map<String, VirtualHostPluginFactory> _vhostPlugins = new HashMap<String, VirtualHostPluginFactory>();
    private Map<String, SlowConsumerPolicyPluginFactory> _policyPlugins = new HashMap<String, SlowConsumerPolicyPluginFactory>();
    private Map<String, AuthenticationManagerPluginFactory<? extends Plugin>> _authenticationManagerPlugins = new HashMap<String, AuthenticationManagerPluginFactory<? extends Plugin>>();

    public PluginManager(String pluginPath, String cachePath) throws Exception
    {
        // Store all non-OSGi plugins
        // A little gross that we have to add them here, but not all the plugins are OSGIfied
        for (SecurityPluginFactory<?> pluginFactory : Arrays.asList(
                AllowAll.FACTORY, DenyAll.FACTORY, LegacyAccess.FACTORY))
        {
            _securityPlugins.put(pluginFactory.getPluginName(), pluginFactory);
        }
        for (ConfigurationPluginFactory configFactory : Arrays.asList(
                TopicConfiguration.FACTORY,
                SecurityManager.SecurityConfiguration.FACTORY,
                AllowAll.AllowAllConfiguration.FACTORY,
                DenyAll.DenyAllConfiguration.FACTORY,
                LegacyAccess.LegacyAccessConfiguration.FACTORY,
                new SlowConsumerDetectionConfigurationFactory(),
                new SlowConsumerDetectionPolicyConfigurationFactory(),
                new SlowConsumerDetectionQueueConfigurationFactory(),
                PrincipalDatabaseAuthenticationManager.PrincipalDatabaseAuthenticationManagerConfiguration.FACTORY))
        {
            _configPlugins.put(configFactory.getParentPaths(), configFactory);
        }
        for (SlowConsumerPolicyPluginFactory pluginFactory : Arrays.asList(
                new TopicDeletePolicy.TopicDeletePolicyFactory()))
        {
            _policyPlugins.put(pluginFactory.getPluginName(), pluginFactory);
        }
        for (VirtualHostPluginFactory pluginFactory : Arrays.asList(
                new SlowConsumerDetection.SlowConsumerFactory()))
        {
            _vhostPlugins.put(pluginFactory.getClass().getName(), pluginFactory);
        }

        for (AuthenticationManagerPluginFactory<? extends Plugin> pluginFactory : Arrays.asList(
                PrincipalDatabaseAuthenticationManager.FACTORY))
        {
            _authenticationManagerPlugins.put(pluginFactory.getPluginName(), pluginFactory);
        }
        if (_bundleContext == null) // Not running inside an OSGi environment.So start Felix.
        {
        // Check the plugin directory path is set and exist
        if (pluginPath == null)
        {
            return;
        }
        File pluginDir = new File(pluginPath);
        if (!pluginDir.exists())
        {
            return;
        }

        // Setup OSGi configuration propery map
        StringMap configMap = new StringMap(false);

        // Add the bundle provided service interface package and the core OSGi
        // packages to be exported from the class path via the system bundle.
        configMap.put(FRAMEWORK_SYSTEMPACKAGES,
                "org.osgi.framework; version=1.3.0," +
                "org.osgi.service.packageadmin; version=1.2.0," +
                "org.osgi.service.startlevel; version=1.0.0," +
                "org.osgi.service.url; version=1.0.0," +
                "org.osgi.util.tracker; version=1.0.0," +
                "org.wso2.andes.junit.extensions.util; " + QPID_VER_SUFFIX +
                "org.wso2.andes; " + QPID_VER_SUFFIX +
                "org.wso2.andes.common; " + QPID_VER_SUFFIX +
                "org.wso2.andes.exchange; " + QPID_VER_SUFFIX +
                "org.wso2.andes.framing; " + QPID_VER_SUFFIX +
                "org.wso2.andes.management.common.mbeans.annotations; " + QPID_VER_SUFFIX +
                "org.wso2.andes.protocol; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.binding; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.configuration; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.configuration.plugins; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.configuration.management; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.exchange; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.logging; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.logging.actors; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.logging.subjects; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.management; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.persistent; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.plugins; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.protocol; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.queue; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.registry; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.security; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.security.access; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.security.access.plugins; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.virtualhost; " + QPID_VER_SUFFIX +
                "org.wso2.andes.server.virtualhost.plugins; " + QPID_VER_SUFFIX +
                "org.wso2.andes.util; " + QPID_VER_SUFFIX +
                "org.apache.commons.configuration; version=1.0.0," +
                "org.apache.commons.lang; version=1.0.0," +
                "org.apache.commons.lang.builder; version=1.0.0," +
                "org.apache.commons.logging; version=1.0.0," +
                "org.apache.log4j; version=1.2.12," +
                "javax.management.openmbean; version=1.0.0," +
                "javax.management; version=1.0.0," +
                "javax.security.auth; version=1.0.0"
            );

        
        
        // No automatic shutdown hook
        configMap.put("felix.shutdown.hook", "false");
        
        // Add system activator
        List<BundleActivator> activators = new ArrayList<BundleActivator>();
        _activator = new Activator();
        activators.add(_activator);
        configMap.put(SYSTEMBUNDLE_ACTIVATORS_PROP, activators);

        if (cachePath != null)
        {
            File cacheDir = new File(cachePath);
            if (!cacheDir.exists() && cacheDir.canWrite())
            {
                _logger.info("Creating plugin cache directory: " + cachePath);
                cacheDir.mkdir();
            }
            
            // Set plugin cache directory and empty it
            _logger.info("Cache bundles in directory " + cachePath);
            configMap.put(FRAMEWORK_STORAGE, cachePath);
        }
        configMap.put(FRAMEWORK_STORAGE_CLEAN, FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);
        
        // Set directory with plugins to auto-deploy
        _logger.info("Auto deploying bundles from directory " + pluginPath);
        configMap.put(AUTO_DEPLOY_DIR_PROPERY, pluginPath);
        configMap.put(AUTO_DEPLOY_ACTION_PROPERY, AUTO_DEPLOY_INSTALL_VALUE + "," + AUTO_DEPLOY_START_VALUE);        
        
        // Start plugin manager and trackers
        _felix = new Felix(configMap);
        try
        {
            _logger.info("Starting plugin manager...");
            _felix.init();
	        process(configMap, _felix.getBundleContext());
            _felix.start();
            _logger.info("Started plugin manager");
        }
        catch (BundleException e)
        {
            throw new ConfigurationException("Could not start plugin manager: " + e.getMessage(), e);
        }
        // Initialize bundle context
            _bundleContext = _activator.getContext();
        }
        // TODO save trackers in a map, keyed by class name

        _exchangeTracker = new ServiceTracker(_bundleContext, ExchangeType.class.getName(), null);
        _exchangeTracker.open();

        _securityTracker = new ServiceTracker(_bundleContext, SecurityPluginFactory.class.getName(), null);
        _securityTracker.open();

        _configTracker = new ServiceTracker(_bundleContext, ConfigurationPluginFactory.class.getName(), null);
        _configTracker.open();

        _virtualHostTracker = new ServiceTracker(_bundleContext, VirtualHostPluginFactory.class.getName(), null);
        _virtualHostTracker.open();

        _policyTracker = new ServiceTracker(_bundleContext, SlowConsumerPolicyPluginFactory.class.getName(), null);
        _policyTracker.open();

        _authenticationManagerTracker = new ServiceTracker(_bundleContext, AuthenticationManagerPluginFactory.class.getName(), null);
        _authenticationManagerTracker.open();

        _logger.info("Opened service trackers");
    }

     public static void setBundleContext(BundleContext ctx)
    {
        _bundleContext = ctx;
    }

    private static <T> Map<String, T> getServices(ServiceTracker tracker)
    {   
        Map<String, T> services = new HashMap<String, T>();
        
        if ((tracker != null) && (tracker.getServices() != null))
        {
            for (Object service : tracker.getServices())
            {
                if (service instanceof PluginFactory<?>)
                {
                    services.put(((PluginFactory<?>) service).getPluginName(), (T) service);
                }
                else
                {
                    services.put(service.getClass().getName(), (T) service);
                }
            }
        }

        return services;
    }

    public static <T> Map<String, T> getServices(ServiceTracker tracker, Map<String, T> plugins)
    {   
        Map<String, T> services = getServices(tracker);
        services.putAll(plugins);
        return services;
    }

    public Map<List<String>, ConfigurationPluginFactory> getConfigurationPlugins()
    {   
        Map<List<String>, ConfigurationPluginFactory> services = new IdentityHashMap<List<String>, ConfigurationPluginFactory>();
        
        if (_configTracker != null && _configTracker.getServices() != null)
        {
            for (Object service : _configTracker.getServices())
            {
                ConfigurationPluginFactory factory = (ConfigurationPluginFactory) service;
                services.put(factory.getParentPaths(), factory);
            }
        }
        
        services.putAll(_configPlugins);

        return services;
    }

    public Map<String, VirtualHostPluginFactory> getVirtualHostPlugins()
    {   
        return getServices(_virtualHostTracker, _vhostPlugins);
    }

    public Map<String, SlowConsumerPolicyPluginFactory> getSlowConsumerPlugins()
    {   
        return getServices(_policyTracker, _policyPlugins);
    }

    public Map<String, ExchangeType<?>> getExchanges()
    {
        return getServices(_exchangeTracker);
    }
    
    public Map<String, SecurityPluginFactory> getSecurityPlugins()
    {
        return getServices(_securityTracker, _securityPlugins);
    }

    public Map<String, AuthenticationManagerPluginFactory<? extends Plugin>> getAuthenticationManagerPlugins()
    {
        return getServices(_authenticationManagerTracker, _authenticationManagerPlugins);
    }

    public void close()
    {
        if (_felix != null)
        {
            try
            {
                // Close all bundle trackers
                _exchangeTracker.close();
                _securityTracker.close();
                _configTracker.close();
                _virtualHostTracker.close();
                _policyTracker.close();
                _authenticationManagerTracker.close();
            }
            finally
            {
                _logger.info("Stopping plugin manager");
                try
                {
                    // FIXME should be stopAndWait() but hangs VM, need upgrade in felix
                    _felix.stop();
                }
                catch (BundleException e)
                {
                    // Ignore
                }

                try
                {
                    _felix.waitForStop(FELIX_STOP_TIMEOUT);
                }
                catch (InterruptedException e)
                {
                    // Ignore
                }
                _logger.info("Stopped plugin manager");
            }
        }
    }
}
