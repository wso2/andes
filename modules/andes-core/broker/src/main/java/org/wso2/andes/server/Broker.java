/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.configuration.modules.JKSStore;
import org.wso2.andes.configuration.qpid.ServerConfiguration;
import org.wso2.andes.configuration.qpid.ServerNetworkTransportConfiguration;
import org.wso2.andes.configuration.qpid.management.ConfigurationManagementMBean;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesKernelBoot;
import org.wso2.andes.server.information.management.ServerInformationMBean;
import org.wso2.andes.server.logging.SystemOutMessageLogger;
import org.wso2.andes.server.logging.actors.BrokerActor;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.actors.GenericActor;
import org.wso2.andes.server.logging.messages.BrokerMessages;
import org.wso2.andes.server.protocol.AMQProtocolEngineFactory;
import org.wso2.andes.server.protocol.AmqpProtocolVersion;
import org.wso2.andes.server.protocol.MultiVersionProtocolEngineFactory;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.registry.ConfigurationFileApplicationRegistry;
import org.wso2.andes.server.transport.QpidAcceptor;
import org.wso2.andes.ssl.SSLContextFactory;
import org.wso2.andes.transport.NetworkTransportConfiguration;
import org.wso2.andes.transport.network.IncomingNetworkTransport;
import org.wso2.andes.transport.network.Transport;
import org.wso2.andes.transport.network.mina.MinaNetworkTransport;

import javax.management.JMException;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.wso2.andes.transport.ConnectionSettings.WILDCARD_ADDRESS;

/**
 * The following class contains the startup and shutting down implementation of Andes Broker.
 */
public class Broker
{
    private static Log log = org.apache.commons.logging.LogFactory.getLog(Broker.class);

    /**
     * Exception class for initializing failure
     */
    protected static class InitException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        InitException(String msg, Throwable cause)
        {
            super(msg, cause);
        }
    }

    /**
     * Shutdowns Andes broker.
     *
     * @throws AndesException
     */
    public void shutdown() throws AndesException {
        Andes.getInstance().shutDown();
    }

    /**
     * Starts up Andes broker
     *
     * @throws AndesException
     */
    public void startup() throws AndesException {
        startup(new BrokerOptions());
    }

    /**
     * Starts up Andes broker with options(configurations).
     *
     * @param options The broker configurations.
     * @throws AndesException
     */
    public void startup(BrokerOptions options) throws AndesException {
        CurrentActor.set(new BrokerActor(new SystemOutMessageLogger()));
        startupImpl(options);
        CurrentActor.remove();
    }

    /**
     * Starts the TCP listener for handling AMQP messages.
     *
     * @param config       The configuration for application registry.
     * @param options      Broker options
     * @param serverConfig Server configuration
     * @throws AndesException
     */
    private void startAMQPListener(ApplicationRegistry config, BrokerOptions options, ServerConfiguration
            serverConfig) throws AndesException {
        try {
            if (AndesConfigurationManager.<Boolean>readValue(AndesConfiguration.TRANSPORTS_AMQP_ENABLED)) {
                ConfigurationManagementMBean configMBean = new ConfigurationManagementMBean();
                configMBean.register();

                ServerInformationMBean sysInfoMBean = new ServerInformationMBean(config);
                sysInfoMBean.register();

                Set<Integer> ports = new HashSet<Integer>(options.getPorts());
                if (ports.isEmpty()) {
                    parsePortList(ports, serverConfig.getPorts());
                }

                Set<Integer> sslPorts = new HashSet<Integer>(options.getSSLPorts());
                if (sslPorts.isEmpty()) {
                    parsePortList(sslPorts, serverConfig.getSSLPorts());
                }

                Set<Integer> exclude_0_10 = new HashSet<Integer>(options.getExcludedPorts(ProtocolExclusion.v0_10));
                if (exclude_0_10.isEmpty()) {
                    parsePortList(exclude_0_10, serverConfig.getPortExclude010());
                }

                Set<Integer> exclude_0_9_1 = new HashSet<Integer>(options.getExcludedPorts(ProtocolExclusion.v0_9_1));
                if (exclude_0_9_1.isEmpty()) {
                    parsePortList(exclude_0_9_1, serverConfig.getPortExclude091());
                }

                Set<Integer> exclude_0_9 = new HashSet<Integer>(options.getExcludedPorts(ProtocolExclusion.v0_9));
                if (exclude_0_9.isEmpty()) {
                    parsePortList(exclude_0_9, serverConfig.getPortExclude09());
                }

                Set<Integer> exclude_0_8 = new HashSet<Integer>(options.getExcludedPorts(ProtocolExclusion.v0_8));
                if (exclude_0_8.isEmpty()) {
                    parsePortList(exclude_0_8, serverConfig.getPortExclude08());
                }

                String bindAddressFromBrokerOptions = options.getBind();
                if (null == bindAddressFromBrokerOptions) {
                    bindAddressFromBrokerOptions = serverConfig.getBind();
                }

                InetAddress bindAddressForHostname;
                if (WILDCARD_ADDRESS.equals(bindAddressFromBrokerOptions)) {
                    bindAddressForHostname = new InetSocketAddress(0).getAddress();
                } else {
                    bindAddressForHostname = InetAddress.getByName(bindAddressFromBrokerOptions);
                }
                String hostName = bindAddressForHostname.getCanonicalHostName();

                if (!serverConfig.getSSLOnly()) {
                    for (int port : ports) {
                        Set<AmqpProtocolVersion> supported = EnumSet.allOf(AmqpProtocolVersion.class);

                        if (exclude_0_10.contains(port)) {
                            supported.remove(AmqpProtocolVersion.v0_10);
                        }

                        if (exclude_0_9_1.contains(port)) {
                            supported.remove(AmqpProtocolVersion.v0_9_1);
                        }
                        if (exclude_0_9.contains(port)) {
                            supported.remove(AmqpProtocolVersion.v0_9);
                        }
                        if (exclude_0_8.contains(port)) {
                            supported.remove(AmqpProtocolVersion.v0_8);
                        }

                        NetworkTransportConfiguration settings =
                                new ServerNetworkTransportConfiguration(serverConfig, port,
                                                                        bindAddressFromBrokerOptions, Transport.TCP);

                        IncomingNetworkTransport transport = Transport.getIncomingTransportInstance();
                        MultiVersionProtocolEngineFactory protocolEngineFactory =
                                new MultiVersionProtocolEngineFactory(hostName, supported);

                        transport.accept(settings, protocolEngineFactory, null);
                        ApplicationRegistry.getInstance().addAcceptor(new InetSocketAddress(bindAddressForHostname, port),
                                new QpidAcceptor(transport,"TCP"));
                        CurrentActor.get().message(BrokerMessages.LISTENING("TCP", port));

                    }
                }

                if (serverConfig.getEnableSSL()) {
                    JKSStore keyStore = AndesConfigurationManager
                            .readValue(AndesConfiguration.TRANSPORTS_AMQP_SSL_CONNECTION_KEYSTORE);
                    JKSStore trustStore = AndesConfigurationManager
                            .readValue(AndesConfiguration.TRANSPORTS_AMQP_SSL_CONNECTION_TRUSTSTORE);

                    String certType = serverConfig.getCertType();
                    SSLContextFactory sslFactory =
                            new SSLContextFactory(trustStore.getStoreLocation(), trustStore.getPassword(), certType,
                                                  keyStore.getStoreLocation(), keyStore.getPassword(), certType);

                    for(int sslPort : sslPorts)
                    {
                        NetworkTransportConfiguration settings =
                                new ServerNetworkTransportConfiguration(serverConfig, sslPort,
                                                                        bindAddressFromBrokerOptions, Transport.TCP);

                        IncomingNetworkTransport transport = new MinaNetworkTransport();

                        transport.accept(settings, new AMQProtocolEngineFactory(), sslFactory);

                        ApplicationRegistry.getInstance().addAcceptor(new InetSocketAddress(bindAddressForHostname, sslPort),
                                                                                    new QpidAcceptor(transport,"TCP"));
                        CurrentActor.get().message(BrokerMessages.LISTENING("TCP/SSL", sslPort));
                    }
                }

                CurrentActor.get().message(BrokerMessages.READY());
            } else {
                log.warn("AMQP Transport is disabled as per configuration.");
            }
        } catch (JMException e) {
            throw new AndesException("Unable to register an MBean", e);
        } catch (UnknownHostException e) {
            throw new AndesException("Unable to get bind address", e);
        }
    }

    /**
     * Andes broker startup implementation.
     *
     * @param options The broker options for configurations.
     * @throws AndesException
     */
    private void startupImpl(final BrokerOptions options) throws AndesException {

        boolean isActorSet = false;
        try {

            final String qpidHome = options.getQpidHome();
            File configFile;

            configFile = getConfigFile(options.getConfigFile(),
                    BrokerOptions.DEFAULT_ANDES_CONFIG_FILE, qpidHome, true);

            log.info("Starting Qpid using configuration : " + configFile.getAbsolutePath());

            /*File logConfigFile = getConfigFile(options.getLogConfigFile(),
                                    BrokerOptions.DEFAULT_LOG_CONFIG_FILE, qpidHome, false);

            configureLogging(logConfigFile, options.getLogWatchFrequency());*/

            ConfigurationFileApplicationRegistry config = new ConfigurationFileApplicationRegistry(configFile);
            ServerConfiguration serverConfig = config.getConfiguration();
            updateManagementPort(serverConfig, options.getJmxPort());

            ApplicationRegistry.initialise(config);

            // We have already loaded the BrokerMessages class by this point so we
            // need to refresh the locale setting in-case we had a different value in
            // the configuration.
            BrokerMessages.reload();

            // AR.initialise() sets and removes its own actor so we now need to set the actor
            // for the remainder of the startup, and the default actor if the stack is empty
            CurrentActor.set(new BrokerActor(config.getCompositeStartupMessageLogger()));
            CurrentActor.setDefault(new BrokerActor(config.getRootMessageLogger()));
            GenericActor.setDefaultMessageLogger(config.getRootMessageLogger());

            isActorSet = true;

            startAMQPListener(config, options, serverConfig);

            /**
             * Boot andes kernel
             */
            AndesKernelBoot.bootAndesKernel();

            AMQPUtils.DEFAULT_CONTENT_CHUNK_SIZE = AndesConfigurationManager.readValue(
                    AndesConfiguration.PERFORMANCE_TUNING_MAX_CONTENT_CHUNK_SIZE);
        } catch (ConfigurationException ce) {
            throw new AndesException("Unable to create configuration files based application registry", ce);
        } catch (AMQException amqe) {
            throw new AndesException("Unable to register a memory configuration", amqe);
        } catch (Exception e) {
            throw new AndesException("Unable to initialise application registry", e);
        } finally {
            if (isActorSet) {
                // Startup is complete so remove the AR initialised Startup actor
                CurrentActor.remove();
            }
        }
    }

    /**
     * Gets the configuration file.
     *
     * @param fileName            The file name for the configuration file.
     * @param defaultFileName     The default configuration file name.
     * @param qpidHome            The qpid home path.
     * @param throwOnFileNotFound Throws error if configuration file is not found.
     * @return The configuration file.
     * @throws InitException
     */
    private File getConfigFile(final String fileName,
                               final String defaultFileName,
                               final String qpidHome, boolean throwOnFileNotFound) throws InitException {
        File configFile;
        if (null != fileName) {
            configFile = new File(fileName);
        } else {
            configFile = new File(qpidHome, defaultFileName);
        }

        if (!configFile.exists() && throwOnFileNotFound) {
            String error = "File " + fileName + " could not be found. Check the file exists and is readable.";

            if (null == qpidHome) {
                error = error + "\nNote: " + BrokerOptions.ANDES_HOME + " is not set.";
            }

            throw new InitException(error, null);
        }

        return configFile;
    }

    /**
     * Parsing a port list to an integer set.
     *
     * @param output The integer set.
     * @param ports  The list of ports.
     * @throws InitException
     */
    public static void parsePortList(Set<Integer> output, List<?> ports) throws InitException {
        if (null != ports) {
            for (Object port : ports) {
                try {
                    output.add(Integer.parseInt(String.valueOf(port)));
                } catch (NumberFormatException e) {
                    throw new InitException("Invalid port: " + port, e);
                }
            }
        }
    }

    /**
     * Update the configuration data with the management port.
     *
     * @param configuration  The server configuration.
     * @param managementPort The string from the command line
     */
    private void updateManagementPort(ServerConfiguration configuration, Integer managementPort) {
        if (null != managementPort) {
            try {
                configuration.setJMXManagementPort(managementPort);
            } catch (NumberFormatException e) {
                throw new InitException("Invalid management port: " + managementPort, null);
            }
        }
    }

}
