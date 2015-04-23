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
package org.wso2.andes.server;

import org.apache.commons.logging.Log;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.QpidLog4JConfigurator;
import org.wso2.andes.AMQException;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesKernelBoot;
import org.wso2.andes.configuration.qpid.ServerConfiguration;
import org.wso2.andes.configuration.qpid.ServerNetworkTransportConfiguration;
import org.wso2.andes.configuration.qpid.management.ConfigurationManagementMBean;
import org.wso2.andes.server.information.management.ServerInformationMBean;
import org.wso2.andes.server.logging.SystemOutMessageLogger;
import org.wso2.andes.server.logging.actors.BrokerActor;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.actors.GenericActor;
import org.wso2.andes.server.logging.management.LoggingManagementMBean;
import org.wso2.andes.server.logging.messages.BrokerMessages;
import org.wso2.andes.server.protocol.AMQProtocolEngineFactory;
import org.wso2.andes.server.protocol.AmqpProtocolVersion;
import org.wso2.andes.server.protocol.MultiVersionProtocolEngineFactory;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.registry.ConfigurationFileApplicationRegistry;
import org.wso2.andes.server.transport.QpidAcceptor;
import org.wso2.andes.ssl.SSLContextFactory;
import org.wso2.andes.transport.NetworkTransportConfiguration;
import org.wso2.andes.transport.flow.control.EventDispatcher;
import org.wso2.andes.transport.flow.control.EventDispatcherFactory;
import org.wso2.andes.transport.flow.control.MemoryMonitor;
import org.wso2.andes.transport.flow.control.MemoryMonitorNotificationFilter;
import org.wso2.andes.transport.network.IncomingNetworkTransport;
import org.wso2.andes.transport.network.Transport;
import org.wso2.andes.transport.network.mina.MinaNetworkTransport;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.wso2.andes.transport.ConnectionSettings.WILDCARD_ADDRESS;

public class Broker
{
    private static final int IPV4_ADDRESS_LENGTH = 4;
    private static final char IPV4_LITERAL_SEPARATOR = '.';
    private static final String ANDES_CONFIG="andesConfig";
    private static Log log =
            org.apache.commons.logging.LogFactory.getLog(Broker.class);

    private static EventDispatcher dispatcher = EventDispatcherFactory.createEventDispatcher();


    protected static class InitException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        InitException(String msg, Throwable cause)
        {
            super(msg, cause);
        }
    }

    public void shutdown()
    {
        Andes.getInstance().shutDown();
    }

    public void startup() throws Exception
    {
        startup(new BrokerOptions());
    }

    public void startup(BrokerOptions options) throws Exception
    {
        try
        {
            CurrentActor.set(new BrokerActor(new SystemOutMessageLogger()));
            startupImpl(options);
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    /**
     * Starts the TCP listener for handling AMQP messages.
     * @param config
     * @param options
     * @param serverConfig
     * @throws AndesException
     * @throws JMException
     * @throws UnknownHostException
     */
    private void startAMQPListener(ApplicationRegistry config, BrokerOptions options, ServerConfiguration serverConfig) throws AndesException, JMException, UnknownHostException {

        if (AndesConfigurationManager.<Boolean>readValue(AndesConfiguration.TRANSPORTS_AMQP_ENABLED)) {
            ConfigurationManagementMBean configMBean = new ConfigurationManagementMBean();
            configMBean.register();

            ServerInformationMBean sysInfoMBean = new ServerInformationMBean(config);
            sysInfoMBean.register();

            Set<Integer> ports = new HashSet<Integer>(options.getPorts());
            if(ports.isEmpty())
            {
                parsePortList(ports, serverConfig.getPorts());
            }

            Set<Integer> sslPorts = new HashSet<Integer>(options.getSSLPorts());
            if(sslPorts.isEmpty())
            {
                parsePortList(sslPorts, serverConfig.getSSLPorts());
            }

            Set<Integer> exclude_0_10 = new HashSet<Integer>(options.getExcludedPorts(ProtocolExclusion.v0_10));
            if(exclude_0_10.isEmpty())
            {
                parsePortList(exclude_0_10, serverConfig.getPortExclude010());
            }

            Set<Integer> exclude_0_9_1 = new HashSet<Integer>(options.getExcludedPorts(ProtocolExclusion.v0_9_1));
            if(exclude_0_9_1.isEmpty())
            {
                parsePortList(exclude_0_9_1, serverConfig.getPortExclude091());
            }

            Set<Integer> exclude_0_9 = new HashSet<Integer>(options.getExcludedPorts(ProtocolExclusion.v0_9));
            if(exclude_0_9.isEmpty())
            {
                parsePortList(exclude_0_9, serverConfig.getPortExclude09());
            }

            Set<Integer> exclude_0_8 = new HashSet<Integer>(options.getExcludedPorts(ProtocolExclusion.v0_8));
            if(exclude_0_8.isEmpty())
            {
                parsePortList(exclude_0_8, serverConfig.getPortExclude08());
            }

            String bindAddr = options.getBind();
            if (bindAddr == null)
            {
                bindAddr = serverConfig.getBind();
            }

            InetAddress bindAddress = null;
            if (bindAddr.equals(WILDCARD_ADDRESS))
            {
                bindAddress = new InetSocketAddress(0).getAddress();
            }
            else
            {
                bindAddress = InetAddress.getByAddress(parseIP(bindAddr));
            }
            String hostName = bindAddress.getCanonicalHostName();

            if (!serverConfig.getSSLOnly())
            {
                for(int port : ports)
                {
                    Set<AmqpProtocolVersion> supported = EnumSet.allOf(AmqpProtocolVersion.class);

                    if(exclude_0_10.contains(port))
                    {
                        supported.remove(AmqpProtocolVersion.v0_10);
                    }

                    if(exclude_0_9_1.contains(port))
                    {
                        supported.remove(AmqpProtocolVersion.v0_9_1);
                    }
                    if(exclude_0_9.contains(port))
                    {
                        supported.remove(AmqpProtocolVersion.v0_9);
                    }
                    if(exclude_0_8.contains(port))
                    {
                        supported.remove(AmqpProtocolVersion.v0_8);
                    }

                    NetworkTransportConfiguration settings =
                            new ServerNetworkTransportConfiguration(serverConfig, port, bindAddress.getHostName(), Transport.TCP);

                    IncomingNetworkTransport transport = Transport.getIncomingTransportInstance();
                    MultiVersionProtocolEngineFactory protocolEngineFactory =
                            new MultiVersionProtocolEngineFactory(hostName, supported);

                    transport.accept(settings, protocolEngineFactory, null);
                    ApplicationRegistry.getInstance().addAcceptor(new InetSocketAddress(bindAddress, port),
                            new QpidAcceptor(transport,"TCP"));
                    CurrentActor.get().message(BrokerMessages.LISTENING("TCP", port));

                }
            }

            if (serverConfig.getEnableSSL())
            {
                String keystorePath = serverConfig.getKeystorePath();
                String keystorePassword = serverConfig.getKeystorePassword();
                String certType = serverConfig.getCertType();
                SSLContextFactory sslFactory =
                        new SSLContextFactory(keystorePath, keystorePassword, certType);

                for(int sslPort : sslPorts)
                {
                    NetworkTransportConfiguration settings =
                            new ServerNetworkTransportConfiguration(serverConfig, sslPort, bindAddress.getHostName(), Transport.TCP);

                    IncomingNetworkTransport transport = new MinaNetworkTransport();

                    transport.accept(settings, new AMQProtocolEngineFactory(), sslFactory);

                    ApplicationRegistry.getInstance().addAcceptor(new InetSocketAddress(bindAddress, sslPort),
                            new QpidAcceptor(transport,"TCP"));
                    CurrentActor.get().message(BrokerMessages.LISTENING("TCP/SSL", sslPort));
                }
            }

            CurrentActor.get().message(BrokerMessages.READY());
        } else {
            log.warn("AMQP Transport is disabled as per configuration.");
        }
    }

    private void startupImpl(final BrokerOptions options) throws Exception
    {
        final String qpidHome = options.getQpidHome();
        File configFile = null;

        configFile = getConfigFile(options.getConfigFile(),
                                BrokerOptions.DEFAULT_ANDES_CONFIG_FILE, qpidHome, true);

        log.info("Starting Qpid using configuration : " + configFile.getAbsolutePath());

        /*File logConfigFile = getConfigFile(options.getLogConfigFile(),
                                    BrokerOptions.DEFAULT_LOG_CONFIG_FILE, qpidHome, false);

        configureLogging(logConfigFile, options.getLogWatchFrequency());*/

        ConfigurationFileApplicationRegistry config = new ConfigurationFileApplicationRegistry(configFile);
        ServerConfiguration serverConfig = config.getConfiguration();
        updateManagementPort(serverConfig, options.getJmxPort());

         /* Registering the memory threshold ratio configured in the qpid-config.xml */
        Double memoryThresholdRatio = AndesConfigurationManager.readValue
                (AndesConfiguration.FLOW_CONTROL_MEMORY_BASED_GLOBAL_MEMORY_THRESHOLD_RATIO);
        this.registerFlowControlMemoryThreshold(memoryThresholdRatio);

        /* Registering the memory monitor */
        Double recoveryThresholdRatio = AndesConfigurationManager.readValue
                (AndesConfiguration.FLOW_CONTROL_MEMORY_BASED_GLOBAL_MEMORY_RECOVERY_THRESHOLD_RATIO);
        Long memoryCheckInterval = AndesConfigurationManager.readValue
                (AndesConfiguration.FLOW_CONTROL_MEMORY_BASED_MEMORY_CHECK_INTERVAL);
        this.registerMemoryMonitor(recoveryThresholdRatio, memoryCheckInterval);

        ApplicationRegistry.initialise(config);

        // We have already loaded the BrokerMessages class by this point so we
        // need to refresh the locale setting incase we had a different value in
        // the configuration.
        BrokerMessages.reload();

        // AR.initialise() sets and removes its own actor so we now need to set the actor
        // for the remainder of the startup, and the default actor if the stack is empty
        CurrentActor.set(new BrokerActor(config.getCompositeStartupMessageLogger()));
        CurrentActor.setDefault(new BrokerActor(config.getRootMessageLogger()));
        GenericActor.setDefaultMessageLogger(config.getRootMessageLogger());

        try
        {

            startAMQPListener(config,options,serverConfig);

            /**
             * Boot andes kernel
             */
            AndesKernelBoot.bootAndesKernel();
        }
        finally
        {
            // Startup is complete so remove the AR initialised Startup actor
            CurrentActor.remove();
        }
    }


    private File getConfigFile(final String fileName,
                               final String defaultFileName,
                               final String qpidHome, boolean throwOnFileNotFound) throws InitException
    {
        File configFile = null;
        if (fileName != null)
        {
            configFile = new File(fileName);
        }
        else
        {
            configFile = new File(qpidHome, defaultFileName);
        }

        if (!configFile.exists() && throwOnFileNotFound)
        {
            String error = "File " + fileName + " could not be found. Check the file exists and is readable.";

            if (qpidHome == null)
            {
                error = error + "\nNote: " + BrokerOptions.ANDES_HOME + " is not set.";
            }

            throw new InitException(error, null);
        }

        return configFile;
    }

    public static void parsePortList(Set<Integer> output, List<?> ports) throws InitException
    {
        if(ports != null)
        {
            for(Object o : ports)
            {
                try
                {
                    output.add(Integer.parseInt(String.valueOf(o)));
                }
                catch (NumberFormatException e)
                {
                    throw new InitException("Invalid port: " + o, e);
                }
            }
        }
    }

    /**
     * Update the configuration data with the management port.
     * @param configuration
     * @param managementPort The string from the command line
     */
    private void updateManagementPort(ServerConfiguration configuration, Integer managementPort)
    {
        if (managementPort != null)
        {
            try
            {
                configuration.setJMXManagementPort(managementPort);
            }
            catch (NumberFormatException e)
            {
                throw new InitException("Invalid management port: " + managementPort, null);
            }
        }
    }

    private byte[] parseIP(String address) throws AndesException
    {
        char[] literalBuffer = address.toCharArray();
        int byteCount = 0;
        int currByte = 0;
        byte[] ip = new byte[IPV4_ADDRESS_LENGTH];
        for (int i = 0; i < literalBuffer.length; i++)
        {
            char currChar = literalBuffer[i];
            if ((currChar >= '0') && (currChar <= '9'))
            {
                currByte = (currByte * 10) + (Character.digit(currChar, 10) & 0xFF);
            }

            if (currChar == IPV4_LITERAL_SEPARATOR || (i + 1 == literalBuffer.length))
            {
                ip[byteCount++] = (byte) currByte;
                currByte = 0;
            }
        }

        if (byteCount != 4)
        {
            throw new AndesException("Invalid IP address: " + address);
        }
        return ip;
    }

    private void configureLogging(File logConfigFile, long logWatchTime) throws InitException, IOException
    {
        if (logConfigFile.exists() && logConfigFile.canRead())
        {
            CurrentActor.get().message(BrokerMessages.LOG_CONFIG(logConfigFile.getAbsolutePath()));

            if (logWatchTime > 0)
            {
                System.out.println("log file " + logConfigFile.getAbsolutePath() + " will be checked for changes every "
                        + logWatchTime + " seconds");
                // log4j expects the watch interval in milliseconds
                try
                {
                    QpidLog4JConfigurator.configureAndWatch(logConfigFile.getPath(), logWatchTime * 1000);
                }
                catch (Exception e)
                {
                    throw new InitException(e.getMessage(),e);
                }
            }
            else
            {
                try
                {
                    QpidLog4JConfigurator.configure(logConfigFile.getPath());
                }
                catch (Exception e)
                {
                    throw new InitException(e.getMessage(),e);
                }
            }
        }
        else
        {
            System.err.println("Logging configuration error: unable to read file " + logConfigFile.getAbsolutePath());
            System.err.println("Using the fallback internal log4j.properties configuration");

            InputStream propsFile = this.getClass().getResourceAsStream("/log4j.properties");
            if(propsFile == null)
            {
                throw new IOException("Unable to load the fallback internal log4j.properties configuration file");
            }
            else
            {
                try
                {
                    Properties fallbackProps = new Properties();
                    fallbackProps.load(propsFile);
                    PropertyConfigurator.configure(fallbackProps);
                }
                finally
                {
                    propsFile.close();
                }
            }
        }
    }

    private void configureLoggingManagementMBean(File logConfigFile, int logWatchTime) throws Exception
    {
        LoggingManagementMBean blm = new LoggingManagementMBean(logConfigFile.getPath(),logWatchTime);

        blm.register();
    }

    /**
     * Registers memory threshold upon all available managed memory pools
     *
     * @param threshold Memory threshold value
     */
    private void registerFlowControlMemoryThreshold(double threshold) throws AMQException {
        if (threshold > 1 || threshold < 0) {
            throw new AMQException("Global memory threshold ratio should be in between 0 and 1");
        }

        if (threshold == 1) {
            log.debug("Global memory threshold ratio is set to 1. Memory based flow controlling " +
                    "is disabled");
        }

        List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
        for (MemoryPoolMXBean poolMXBean : pools) {
            if (MemoryType.HEAP.equals(poolMXBean.getType())) {
                if (!poolMXBean.isUsageThresholdSupported()) {
                    if (log.isDebugEnabled()) {
                        log.debug("UsageThreshold is not supported by the MemoryPool MXBean. " +
                                "Continuing without setting the UsageThreshold");
                    }
                    continue;
                }
                long thresholdInBytes = (long)Math.floor(poolMXBean.getUsage().getMax() * threshold);
                poolMXBean.setUsageThreshold(thresholdInBytes);
                poolMXBean.setCollectionUsageThreshold(thresholdInBytes);
            }
        }

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        NotificationEmitter emitter = (NotificationEmitter) memoryMXBean;
        emitter.addNotificationListener(dispatcher, null, null);
    }

    private void registerMemoryMonitor(
            double recoveryThresholdRatio, long memoryCheckInterval) throws AMQException {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        MemoryMonitor monitor = new MemoryMonitor(recoveryThresholdRatio, memoryCheckInterval);
        try {
            ObjectName name = new ObjectName("org.wso2.andes.transport.flow.control:type=MemoryMonitorMBean");
            mbs.registerMBean(monitor, name);

            NotificationFilter filter = new MemoryMonitorNotificationFilter();
            mbs.addNotificationListener(name, getEventDispatcher(), filter, name);
            monitor.start();
        } catch (Exception e) {
            String msg = "Error occurred while registering MemoryMonitorMBean";
            log.error(msg, e);
            throw new AMQException(msg, e);
        }
    }

    public static EventDispatcher getEventDispatcher() {
        return dispatcher;
    }

}
