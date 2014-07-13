/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.wso2.andes.server.configuration;

import org.apache.commons.configuration.*;
import org.apache.log4j.Logger;
import org.wso2.andes.server.configuration.management.ConfigurationManagementMBean;
import org.wso2.andes.server.configuration.plugins.ConfigurationPlugin;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostRegistry;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;

import static org.wso2.andes.transport.ConnectionSettings.WILDCARD_ADDRESS;

public class ServerConfiguration extends ConfigurationPlugin implements SignalHandler
{
    protected static final Logger _logger = Logger.getLogger(ServerConfiguration.class);

    // Default Configuration values
    public static final int DEFAULT_BUFFER_SIZE = 262144;
    public static final int DEFAULT_SOCKET_BUFFER_SIZE = 32768;
    public static final String DEFAULT_STATUS_UPDATES = "on";
    public static final String SECURITY_CONFIG_RELOADED = "SECURITY CONFIGURATION RELOADED";

    public static final int DEFAULT_FRAME_SIZE = 65536;
    public static final int DEFAULT_PORT = 5672;
    public static final int DEFAULT_SSL_PORT = 8672;
    public static final long DEFAULT_HOUSEKEEPING_PERIOD = 30000L;
    public static final int DEFAULT_JMXPORT = 8999;

    public static final String QPID_HOME = "QPID_HOME";
    public static final String QPID_WORK = "QPID_WORK";
    public static final String LIB_DIR = "lib";
    public static final String PLUGIN_DIR = "plugins";
    public static final String CACHE_DIR = "cache";

    private Map<String, VirtualHostConfiguration> _virtualHosts = new HashMap<String, VirtualHostConfiguration>();

    private File _configFile;
    private File _vhostsFile;

    private Logger _log = Logger.getLogger(this.getClass());

    private ConfigurationManagementMBean _mbean;

    // Map of environment variables to config items
    private static final Map<String, String> envVarMap = new HashMap<String, String>();

    // Configuration values to be read from the configuration file
    //todo Move all properties to static values to ensure system testing can be performed.
    public static final String MGMT_CUSTOM_REGISTRY_SOCKET = "management.custom-registry-socket";
    public static final String STATUS_UPDATES = "status-updates";
    public static final String ADVANCED_LOCALE = "advanced.locale";

    public static final int DEFAULT_JMS_EXPIRATION_CHECK_INTERVAL = 10000;
    public static final boolean DEFAULT_SAVE_EXPIRED_TO_DLC = false;
    public static final int DEFAULT_EXPIRATION_MESSAGE_BATCH_SIZE =1000;

    {
        envVarMap.put("QPID_ENABLEDIRECTBUFFERS", "advanced.enableDirectBuffers");
        envVarMap.put("QPID_SSLPORT", "connector.ssl.port");
        envVarMap.put("QPID_WRITEBIASED", "advanced.useWriteBiasedPool");
        envVarMap.put("QPID_JMXPORT", "management.jmxport");
        envVarMap.put("QPID_FRAMESIZE", "advanced.framesize");
        envVarMap.put("QPID_MSGAUTH", "security.msg-auth");
        envVarMap.put("QPID_AUTOREGISTER", "auto_register");
        envVarMap.put("QPID_MANAGEMENTENABLED", "management.enabled");
        envVarMap.put("QPID_HEARTBEATDELAY", "heartbeat.delay");
        envVarMap.put("QPID_HEARTBEATTIMEOUTFACTOR", "heartbeat.timeoutFactor");
        envVarMap.put("QPID_MAXIMUMMESSAGEAGE", "maximumMessageAge");
        envVarMap.put("QPID_MAXIMUMMESSAGECOUNT", "maximumMessageCount");
        envVarMap.put("QPID_MAXIMUMQUEUEDEPTH", "maximumQueueDepth");
        envVarMap.put("QPID_MAXIMUMMESSAGESIZE", "maximumMessageSize");
        envVarMap.put("QPID_MAXIMUMCHANNELCOUNT", "maximumChannelCount");
        envVarMap.put("QPID_MINIMUMALERTREPEATGAP", "minimumAlertRepeatGap");
        envVarMap.put("QPID_QUEUECAPACITY", "capacity");
        envVarMap.put("QPID_FLOWRESUMECAPACITY", "flowResumeCapacity");
        envVarMap.put("QPID_SOCKETRECEIVEBUFFER", "connector.socketReceiveBuffer");
        envVarMap.put("QPID_SOCKETWRITEBUFFER", "connector.socketWriteBuffer");
        envVarMap.put("QPID_TCPNODELAY", "connector.tcpNoDelay");
        envVarMap.put("QPID_ENABLEPOOLEDALLOCATOR", "advanced.enablePooledAllocator");
        envVarMap.put("QPID_STATUS-UPDATES", "status-updates");
        envVarMap.put("JMS_EXPIRATION_CHECK_INTERVAL","advanced.expiration.checkInterval");
        envVarMap.put("SAVE_EXPIRED_TO_DLC","advanced.dlc.sendExpiredMessagesToDLC");
        envVarMap.put("EXPIRATION_MESSAGE_BATCH_SIZE","advanced.expiration.messageBatchSize");
    }

    /**
     * Loads the given file and sets up the HUP signal handler.
     *
     * This will load the file and present the root level properties but will
     * not perform any virtualhost configuration.
     * <p>
     * To perform this {@link #initialise()} must be called.
     * <p>
     * This has been made a two step process to allow the Plugin Manager and
     * Configuration Manager to be initialised in the Application Registry.
     * <p>
     * If using this ServerConfiguration via an ApplicationRegistry there is no
     * need to explictly call {@link #initialise()} as this is done via the
     * {@link ApplicationRegistry#initialise()} method.
     *
     * @param configurationURL
     * @throws org.apache.commons.configuration.ConfigurationException
     */
    public ServerConfiguration(File configurationURL) throws ConfigurationException
    {
        this(parseConfig(configurationURL));
        _configFile = configurationURL;
        try
        {
            Signal sig = new sun.misc.Signal("HUP");
            sun.misc.Signal.handle(sig, this);
        }
        catch (Exception e)
        {
            _logger.info("Signal HUP not supported for OS: " + System.getProperty("os.name"));
            // We're on something that doesn't handle SIGHUP, how sad, Windows.
        }
    }

    /**
     * Wraps the given Commons Configuration as a ServerConfiguration.
     *
     * Mainly used during testing and in locations where configuration is not
     * desired but the interface requires configuration.
     * <p>
     * If the given configuration has VirtualHost configuration then
     * {@link #initialise()} must be called to perform the required setup.
     * <p>
     * This has been made a two step process to allow the Plugin Manager and
     * Configuration Manager to be initialised in the Application Registry.
     * <p>
     * If using this ServerConfiguration via an ApplicationRegistry there is no 
     * need to explictly call {@link #initialise()} as this is done via the
     * {@link ApplicationRegistry#initialise()} method.
     *
     * @param conf
     */
    public ServerConfiguration(Configuration conf)
    {
        _configuration = conf;        
    }

    /**
     * Processes this configuration and setups any VirtualHosts defined in the
     * configuration.
     *
     * This has been separated from the constructor to allow the PluginManager
     * time to be created and provide plugins to the ConfigurationManager for
     * processing here.
     * <p>
     * Called by {@link ApplicationRegistry#initialise()}.
     * <p>
     * NOTE: A DEFAULT ApplicationRegistry must exist when using this method
     * or a new ApplicationRegistry will be created. 
     *
     * @throws ConfigurationException
     */
    public void initialise() throws ConfigurationException
    {	
        setConfiguration("", _configuration);
        setupVirtualHosts(_configuration);
    }

    public String[] getElementsProcessed()
    {
        return new String[] { "" };
    }

    @Override
    public void validateConfiguration() throws ConfigurationException
    {
        // Support for security.jmx.access was removed when JMX access rights were incorporated into the main ACL.
        // This ensure that users remove the element from their configuration file.
        
        if (getListValue("security.jmx.access").size() > 0)
        {
            String message = "Validation error : security/jmx/access is no longer a supported element within the configuration xml." 
                    + (_configFile == null ? "" : " Configuration file : " + _configFile);
            throw new ConfigurationException(message);
        }

        if (getListValue("security.jmx.principal-database").size() > 0)
        {
            String message = "Validation error : security/jmx/principal-database is no longer a supported element within the configuration xml."
                    + (_configFile == null ? "" : " Configuration file : " + _configFile);
            throw new ConfigurationException(message);
        }

        if (getListValue("security.principal-databases.principal-database(0).class").size() > 0)
        {
            String message = "Validation error : security/principal-databases is no longer supported within the configuration xml." 
                    + (_configFile == null ? "" : " Configuration file : " + _configFile);
            throw new ConfigurationException(message);
        }
    }

    /*
     * Modified to enforce virtualhosts configuration in external file or main file, but not
     * both, as a fix for QPID-2360 and QPID-2361.
     */
    @SuppressWarnings("unchecked")
    protected void setupVirtualHosts(Configuration conf) throws ConfigurationException
    {
        List<String> vhostFiles = conf.getList("virtualhosts");
        Configuration vhostConfig = conf.subset("virtualhosts");

        // Only one configuration mechanism allowed
        if (!vhostFiles.isEmpty() && !vhostConfig.subset("virtualhost").isEmpty())
        {
            throw new ConfigurationException("Only one of external or embedded virtualhosts configuration allowed.");
        }

        // We can only have one vhosts XML file included
        if (vhostFiles.size() > 1)
        {
            throw new ConfigurationException("Only one external virtualhosts configuration file allowed, multiple filenames found.");
        }

        // Virtualhost configuration object
        Configuration vhostConfiguration = new HierarchicalConfiguration();

        // Load from embedded configuration if possible
        if (!vhostConfig.subset("virtualhost").isEmpty())
        {
            vhostConfiguration = vhostConfig;
        }
        else
        {
	    	// Load from the external configuration if possible
	    	for (String fileName : vhostFiles)
	        {
	            // Open the vhosts XML file and copy values from it to our config
                _vhostsFile = new File(fileName);
                if (!_vhostsFile.exists())
                {
                    throw new ConfigurationException("Virtualhosts file does not exist");
                }
	        	vhostConfiguration = parseConfig(new File(fileName));

                // save the default virtualhost name
                String defaultVirtualHost = vhostConfiguration.getString("default");
                _configuration.setProperty("virtualhosts.default", defaultVirtualHost);
            }
        }

        // Now extract the virtual host names from the configuration object
        List hosts = vhostConfiguration.getList("virtualhost.name");
        for (int j = 0; j < hosts.size(); j++)
        {
            String name = (String) hosts.get(j);

            // Add the virtual hosts to the server configuration
            VirtualHostConfiguration virtualhost = new VirtualHostConfiguration(name,
                    vhostConfiguration.subset("virtualhost." + name));
            _virtualHosts.put(virtualhost.getName(), virtualhost);
        }
    }

    private static void substituteEnvironmentVariables(Configuration conf)
    {
        for (Entry<String, String> var : envVarMap.entrySet())
        {
            String val = System.getenv(var.getKey());
            if (val != null)
            {
                conf.setProperty(var.getValue(), val);
            }
        }
    }

    private static Configuration parseConfig(File file) throws ConfigurationException
    {
        ConfigurationFactory factory = new ConfigurationFactory();
        factory.setConfigurationFileName(file.getAbsolutePath());
        Configuration conf = factory.getConfiguration();

        Iterator<?> keys = conf.getKeys();
        if (!keys.hasNext())
        {
            keys = null;
            conf = flatConfig(file);
        }

        substituteEnvironmentVariables(conf);

        return conf;
    }

    /**
     * Check the configuration file to see if status updates are enabled.
     *
     * @return true if status updates are enabled
     */
    public boolean getStatusUpdatesEnabled()
    {
        // Retrieve the setting from configuration but default to on.
        String value = getStringValue(STATUS_UPDATES, DEFAULT_STATUS_UPDATES);

        return value.equalsIgnoreCase("on");
    }

    /**
     * The currently defined {@see Locale} for this broker
     *
     * @return the configuration defined locale
     */
    public Locale getLocale()
    {
        String localeString = getStringValue(ADVANCED_LOCALE);
        // Expecting locale of format langauge_country_variant

        // If the configuration does not have a defined locale use the JVM default
        if (localeString == null)
        {
            return Locale.getDefault();
        }

        String[] parts = localeString.split("_");

        Locale locale;
        switch (parts.length)
        {
            case 1:
                locale = new Locale(localeString);
                break;
            case 2:
                locale = new Locale(parts[0], parts[1]);
                break;
            default:
                StringBuilder variant = new StringBuilder(parts[2]);
                // If we have a variant such as the Java doc suggests for Spanish
                // Traditional_WIN we may end up with more than 3 parts on a
                // split with '_'. So we should recombine the variant.
                if (parts.length > 3)
                {
                    for (int index = 3; index < parts.length; index++)
                    {
                        variant.append('_').append(parts[index]);
                    }
                }

                locale = new Locale(parts[0], parts[1], variant.toString());
        }

        return locale;
    }

    public int getMessageReadCacheSize() {
        int cacheSize =  getIntValue("clustering.tuning.messageReadCacheSize",500);
        return cacheSize;
    }



    // Our configuration class needs to make the interpolate method
    // public so it can be called below from the config method.
    public static class MyConfiguration extends CompositeConfiguration
    {
        public String interpolate(String obj)
        {
            return super.interpolate(obj);
        }
    }

    public int getNumberOfMaximumDeliveryCount() {
        int numberOfMaximumDeliveryCount =  getIntValue("clustering.tuning.maximumNumberOfMessageDeliveryAttempts",10);
        return numberOfMaximumDeliveryCount;
    }

    public final static Configuration flatConfig(File file) throws ConfigurationException
    {
        // We have to override the interpolate methods so that
        // interpolation takes place accross the entirety of the
        // composite configuration. Without doing this each
        // configuration object only interpolates variables defined
        // inside itself.
        final MyConfiguration conf = new MyConfiguration();
        conf.addConfiguration(new SystemConfiguration()
        {
            protected String interpolate(String o)
            {
                return conf.interpolate(o);
            }
        });
        conf.addConfiguration(new XMLConfiguration(file)
        {
            protected String interpolate(String o)
            {
                return conf.interpolate(o);
            }
        });
        return conf;
    }

    public String getConfigurationURL()
    {
        return _configFile == null ? "" : _configFile.getAbsolutePath();
    }

    public void handle(Signal arg0)
    {
        try
        {
            reparseConfigFileSecuritySections();
        }
        catch (ConfigurationException e)
        {
             _logger.error("Could not reload configuration file security sections", e);
        }
    }

    public void reparseConfigFileSecuritySections() throws ConfigurationException
    {
        if (_configFile != null)
        {
            Configuration newConfig = parseConfig(_configFile);
            setConfiguration("", newConfig);
            ApplicationRegistry.getInstance().getSecurityManager().configureHostPlugins(this);
			
            // Reload virtualhosts from correct location
            Configuration newVhosts;
            if (_vhostsFile == null)
            {
                newVhosts = newConfig.subset("virtualhosts");
            }
            else
            {
                newVhosts = parseConfig(_vhostsFile);
            }

            VirtualHostRegistry vhostRegistry = ApplicationRegistry.getInstance().getVirtualHostRegistry();
            for (String hostName : _virtualHosts.keySet())
            {
                VirtualHost vhost = vhostRegistry.getVirtualHost(hostName);
                Configuration vhostConfig = newVhosts.subset("virtualhost." + hostName);
                vhost.getConfiguration().setConfiguration("virtualhosts.virtualhost", vhostConfig); // XXX
                vhost.getSecurityManager().configureGlobalPlugins(this);
                vhost.getSecurityManager().configureHostPlugins(vhost.getConfiguration());
            }

            _logger.warn(SECURITY_CONFIG_RELOADED);
        }
    }
    
    public String getQpidWork()
    {
        return System.getProperty(QPID_WORK, System.getProperty("java.io.tmpdir"));
    }
    
    public String getQpidHome()
    {
        return System.getProperty(QPID_HOME);
    }

    public void setJMXManagementPort(int mport)
    {
        getConfig().setProperty("management.jmxport", mport);
    }

    public int getJMXManagementPort()
    {
        return getIntValue("management.jmxport", DEFAULT_JMXPORT);
    }

    public boolean getUseCustomRMISocketFactory()
    {
        return getBooleanValue(MGMT_CUSTOM_REGISTRY_SOCKET, true);
    }

    public void setUseCustomRMISocketFactory(boolean bool)
    {
        getConfig().setProperty(MGMT_CUSTOM_REGISTRY_SOCKET, bool);
    }

    public boolean getPlatformMbeanserver()
    {
        return getBooleanValue("management.platform-mbeanserver", true);
    }

    public String[] getVirtualHosts()
    {
        return _virtualHosts.keySet().toArray(new String[_virtualHosts.size()]);
    }
    
    public String getPluginDirectory()
    {
        return getStringValue("plugin-directory");
    }
    
    public String getCacheDirectory()
    {
        return getStringValue("cache-directory");
    }

    public VirtualHostConfiguration getVirtualHostConfig(String name)
    {
        return _virtualHosts.get(name);
    }

    public void setVirtualHostConfig(VirtualHostConfiguration config)
    {
        _virtualHosts.put(config.getName(), config);
    }

    public int getFrameSize()
    {
        return getIntValue("advanced.framesize", DEFAULT_FRAME_SIZE);
    }

    public boolean getSynchedClocks()
    {
        return getBooleanValue("advanced.synced-clocks");
    }

    public boolean getMsgAuth()
    {
        return getBooleanValue("security.msg-auth");
    }

    public String getManagementKeyStorePath()
    {
        return getStringValue("management.ssl.keyStorePath");
    }

    public boolean getManagementSSLEnabled()
    {
        return getBooleanValue("management.ssl.enabled", true);
    }

    public String getManagementKeyStorePassword()
    {
        return getStringValue("management.ssl.keyStorePassword");
    }

    public boolean getQueueAutoRegister()
    {
        return getBooleanValue("queue.auto_register", true);
    }

    public boolean getViewMessageCounts()
    {
        return getBooleanValue("queue.viewMessageCounts", false);
    }

    public boolean  getClusteringEnabled()
    {
        return getBooleanValue("clustering.enabled", false);
    }

    public boolean getIsExternalCassandraServerRequired() {

        return  getBooleanValue("clustering.externalCassandraServerRequired", false);
    }

    public String getZookeeperConnection()
    {
        return getStringValue("clustering.coordination.ZooKeeperConnection","127.0.0.1:2180");
    }

    public String getReferenceTime() {
        return getStringValue("clustering.coordination.ReferenceTime","2012-02-29 08:08:08");
    }


    public boolean isOnceInOrderSupportEnabled() {
        return getBooleanValue("clustering.OnceInOrderSupportEnabled",false);
    }

    public int getGlobalQueueCount(){
        return getIntValue("clustering.GlobalQueueCount",10);
    }

    public int getGlobalQueueWorkerMessageBatchSize() {
        return getIntValue("clustering.tuning.messageBatchSizes.globalQueueWorkerMessageBatchSize", 700);
    }

    public int getContentPublisherMessageBatchSize() {
        return getIntValue("clustering.tuning.messageBatchSizes.contentPublisherMessageBatchSize", 200);
    }

    public int getMetadataPublisherMessageBatchSize() {
        return getIntValue("clustering.tuning.messageBatchSizes.medataDatePublisherMessageBatchSize", 200);
    }


    public int getMessageBatchSizeForSubscribersQueues() {
        return getIntValue("clustering.tuning.messageBatchSizes.messageBatchSizeForSubscribersQueues", 20);
    }

    public int getDefaultMessageBatchSizeForSubscribers() {
        return getIntValue("clustering.tuning.messageBatchSizes.messageBatchSizeForSubscribers.default",50);
    }

    public int getMaxMessageBatchSizeForSubscribers() {
        return getIntValue("clustering.tuning.messageBatchSizes.messageBatchSizeForSubscribers.max",300);
    }

    public int getMinMessageBatchSizeForSubscribers() {
        return getIntValue("clustering.tuning.messageBatchSizes.messageBatchSizeForSubscribers.min", 20);
    }


    public int getMaxNumberOfUnackedMessages() {
        return getIntValue("clustering.tuning.messageBatchSizes.maxNumberOfUnackedMessages",1000);
    }

    public int getMaxNumberOfReadButUndeliveredMessages() {
        return getIntValue("clustering.tuning.messageBatchSizes.maxNumberOfReadButUndeliveredMessages",1000);
    }

    public int getFlusherPoolSize() {
        return getIntValue("clustering.tuning.threading.flusherPoolSize",10);
    }

    public int getAndesInternalParallelThreadPoolSize() {
        return getIntValue("clustering.tuning.threading.andesInternalParallelThreadPoolSize",50);
    }

    public int getAndesExecutorServicePoolSize() {
        return getIntValue("clustering.tuning.threading.andesExecutorServicePoolSize",50);
    }

    public int getSubscriptionPoolSize() {
        return getIntValue("clustering.tuning.threading.subscriptionPoolSize",20);
    }

    public double getFlowControlGlobalMemoryThresholdRatio() {
        return getDoubleValue("clustering.tuning.flowControl.memoryBased.globalMemoryThresholdRatio", 1);
    }

    public double getGlobalMemoryRecoveryThresholdRatio() {
        return getDoubleValue("clustering.tuning.flowControl.memoryBased.globalMemoryRecoveryThresholdRatio", 1);
    }

    public long getMemoryCheckInterval() {
        return getLongValue("clustering.tuning.flowControl.memoryBased.memoryCheckInterval", 20000L);
    }

    public int getFlowControlPerConnectionMessageThreshold() {
        return getIntValue("clustering.tuning.flowControl.connectionBased.perConnectionMessageThreshold", 2000);
    }

    public int getInternalSequentialThreadPoolSize() {
        return getIntValue("clustering.tuning.threading.internalSequentialThreadPoolSize", 5);
    }

    public int getPublisherPoolSize() {
        return getIntValue("clustering.tuning.threading.publisherPoolSize",50);
    }

    public int getMaxAckWaitTime() {
        return getIntValue("clustering.tuning.waitTimes.maxAckWaitTime",300);
    }


    public int getQueueWorkerInterval() {
        return getIntValue("clustering.tuning.waitTimes.queueWorkerInterval",500);
    }

    public int getPubSubMessageRemovalTaskInterval() {

        return getIntValue("clustering.tuning.waitTimes.pubSubMessageRemovalTaskInterval",5000);
    }

    public int getContentRemovalTaskInterval() {
        return getIntValue("clustering.tuning.waitTimes.contentRemovalTaskInterval",4000);
    }


    public int getContentRemovalTimeDifference() {
        return getIntValue("clustering.tuning.waitTimes.contentRemovalTimeDifference",120);
    }

    public int getVirtualHostSyncTaskInterval() {
        return getIntValue("clustering.tuning.waitTimes.virtualHostSyncTaskInterval",3600);
    }

    public int getQueueMsgDeliveryCurserResetTimeInterval() {
        return getIntValue("clustering.tuning.waitTimes.queueMsgDeliveryCurserResetTimeInterval",60000);
    }

    public boolean getManagementEnabled()
    {
        return getBooleanValue("management.enabled", true);
    }

    public void setManagementEnabled(boolean enabled)
    {
        getConfig().setProperty("management.enabled", enabled);
    }

    public int getHeartBeatDelay()
    {
        return getIntValue("heartbeat.delay", 0);
    }

    public double getHeartBeatTimeout()
    {
        return getDoubleValue("heartbeat.timeoutFactor", 2.0);
    }

    public int getDeliveryPoolSize()
    {
        return getIntValue("delivery.poolsize");
    }

    public long getMaximumMessageAge()
    {
        return getLongValue("maximumMessageAge");
    }

    public long getMaximumMessageCount()
    {
        return getLongValue("maximumMessageCount");
    }

    public long getMaximumQueueDepth()
    {
        return getLongValue("maximumQueueDepth");
    }

    public long getMaximumMessageSize()
    {
        return getLongValue("maximumMessageSize");
    }

    public long getMinimumAlertRepeatGap()
    {
        return getLongValue("minimumAlertRepeatGap");
    }

    public long getCapacity()
    {
        return getLongValue("capacity");
    }

    public long getFlowResumeCapacity()
    {
        return getLongValue("flowResumeCapacity", getCapacity());
    }

    public int getConnectorProcessors()
    {
        return getIntValue("connector.processors", 4);
    }

    public List getPorts()
    {
        return getListValue("connector.port", Collections.<Integer>singletonList(DEFAULT_PORT));
    }

    public List getPortExclude010()
    {
        return getListValue("connector.non010port");
    }

    public List getPortExclude091()
    {
        return getListValue("connector.non091port");
    }

    public List getPortExclude09()
    {
        return getListValue("connector.non09port");
    }

    public List getPortExclude08()
    {
        return getListValue("connector.non08port");
    }

    public String getBind()
    {
        return getStringValue("connector.bind", WILDCARD_ADDRESS);
    }

    public int getReceiveBufferSize()
    {
        return getIntValue("connector.socketReceiveBuffer", DEFAULT_SOCKET_BUFFER_SIZE);
    }

    public int getWriteBufferSize()
    {
        return getIntValue("connector.socketWriteBuffer", DEFAULT_SOCKET_BUFFER_SIZE);
    }

    public boolean getTcpNoDelay()
    {
        return getBooleanValue("connector.tcpNoDelay", true);
    }

    public boolean getEnableExecutorPool()
    {
        return getBooleanValue("advanced.filterchain[@enableExecutorPool]");
    }

    public boolean getEnableSSL()
    {
        return getBooleanValue("connector.ssl.enabled");
    }

    public boolean getSSLOnly()
    {
        return getBooleanValue("connector.ssl.sslOnly");
    }

    public List getSSLPorts()
    {
        return getListValue("connector.ssl.port", Collections.<Integer>singletonList(DEFAULT_SSL_PORT));
    }

    public String getKeystorePath()
    {
        return getStringValue("connector.ssl.keystorePath", "repository/resources/security/wso2carbon.jks");
    }

    public String getKeystorePassword()
    {
        return getStringValue("connector.ssl.keystorePassword", "wso2carbon");
    }

    public String getCertType()
    {
        return getStringValue("connector.ssl.certType", "SunX509");
    }

    public boolean getUseBiasedWrites()
    {
        return getBooleanValue("advanced.useWriteBiasedPool");
    }

    public String getDefaultVirtualHost()
    {
        return getStringValue("virtualhosts.default");
    }

    public void setDefaultVirtualHost(String vhost)
    {
         getConfig().setProperty("virtualhosts.default", vhost);
    }    

    public void setHousekeepingExpiredMessageCheckPeriod(long value)
    {
        getConfig().setProperty("housekeeping.expiredMessageCheckPeriod", value);
    }

    public long getHousekeepingCheckPeriod()
    {
        return getLongValue("housekeeping.checkPeriod",
                                   getLongValue("housekeeping.expiredMessageCheckPeriod",
                                                       DEFAULT_HOUSEKEEPING_PERIOD));
    }

    public long getStatisticsSamplePeriod()
    {
        return getConfig().getLong("statistics.sample.period", 5000L);
    }

    public boolean isStatisticsGenerationBrokerEnabled()
    {
        return getConfig().getBoolean("statistics.generation.broker", false);
    }

    public boolean isStatisticsGenerationVirtualhostsEnabled()
    {
        return getConfig().getBoolean("statistics.generation.virtualhosts", false);
    }

    public boolean isStatisticsGenerationConnectionsEnabled()
    {
        return getConfig().getBoolean("statistics.generation.connections", false);
    }

    public long getStatisticsReportingPeriod()
    {
        return getConfig().getLong("statistics.reporting.period", 0L);
    }

    public boolean isStatisticsReportResetEnabled()
    {
        return getConfig().getBoolean("statistics.reporting.reset", false);
    }

    public int getMaxChannelCount()
    {
        return getIntValue("maximumChannelCount", 256);
    }

    public int getMessageBatchSizeForBrowserSubscriptions(){
        return getIntValue("clustering.tuning.messageBatchSizes.messageBatchSizeForBrowserSubscriptions",200);
    }

     public boolean isInMemoryModeEnabled() {
        return getBooleanValue("InMemoryMode", false);
    }
     
     public int getMaxAckWaitTimeForBatch() {
         return getIntValue("clustering.tuning.waitTimes.maxAckWaitTimeForBatch",2);
     }

    public String getMessageIdGeneratorClass() {
        return getStringValue("broker.store.idGenerator", "org.wso2.andes.server.cluster.coordination.TimeStampBasedMessageIdGenerator");
    }

    /**
     * configuration property to specify time interval between checking expired messages within the broker. (milliseconds)
     * @return int
     */
    public int getJMSExpirationCheckInterval() {
        return getIntValue("advanced.expiration.checkInterval",DEFAULT_JMS_EXPIRATION_CHECK_INTERVAL);
    }

    /**
     * configuration property to decide whether to save expired messages to DLC or not
     * @return boolean
     */
    public boolean getSaveExpiredToDLC() {
        return getBooleanValue("advanced.dlc.sendExpiredMessagesToDLC", DEFAULT_SAVE_EXPIRED_TO_DLC);
    }

    public int getExpirationMessageBatchSize() {
        return getIntValue("advanced.expiration.messageBatchSize", DEFAULT_EXPIRATION_MESSAGE_BATCH_SIZE);
    }
}

// TODO : Disruptor clean the configs