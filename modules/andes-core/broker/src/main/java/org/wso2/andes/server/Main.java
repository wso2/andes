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

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.mina.util.SessionLog;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.server.Broker.InitException;
import org.wso2.andes.server.registry.ApplicationRegistry;


/**
 * Main entry point for AMQPD.
 */
public class Main {
    private final Options options = new Options();
    private CommandLine commandLine;

    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        //if the -Dlog4j.configuration property has not been set, enable the init override
        //to stop Log4J wondering off and picking up the first log4j.xml/properties file it
        //finds from the classpath when we get the first Loggers
        if (System.getProperty("log4j.configuration") == null) {
            System.setProperty("log4j.defaultInitOverride", "true");
        }

        new Main(args);
    }

    public Main(final String[] args) {
        setOptions(options);
        if (parseCommandline(args)) {
            try {
                execute();
            } catch (Exception e) {
                logger.error("Exception during startup. Triggering shutdown ", e);
                shutdown(1);
            }
        }
    }

    protected boolean parseCommandline(final String[] args) {
        try {
            commandLine = new PosixParser().parse(options, args);

            return true;
        } catch (ParseException e) {
            logger.error("Error while parsing command line arguments ", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Qpid", options, true);

            return false;
        }
    }

    protected void setOptions(final Options options) {
        Option help = new Option("h", "help", false, "print this message");
        Option version = new Option("v", "version", false, "print the version information and exit");
        Option configFile =
                OptionBuilder.withArgName("file").hasArg().withDescription("use given configuration file").withLongOpt("config")
                        .create("c");
        Option port =
                OptionBuilder.withArgName("port").hasArg()
                        .withDescription("listen on the specified port. Overrides any value in the config file")
                        .withLongOpt("port").create("p");

        Option exclude0_10 =
                OptionBuilder.withArgName("exclude-0-10").hasArg()
                        .withDescription("when listening on the specified port do not accept AMQP0-10 connections. The specified port must be one specified on the command line")
                        .withLongOpt("exclude-0-10").create();

        Option exclude0_9_1 =
                OptionBuilder.withArgName("exclude-0-9-1").hasArg()
                        .withDescription("when listening on the specified port do not accept AMQP0-9-1 connections. The specified port must be one specified on the command line")
                        .withLongOpt("exclude-0-9-1").create();


        Option exclude0_9 =
                OptionBuilder.withArgName("exclude-0-9").hasArg()
                        .withDescription("when listening on the specified port do not accept AMQP0-9 connections. The specified port must be one specified on the command line")
                        .withLongOpt("exclude-0-9").create();


        Option exclude0_8 =
                OptionBuilder.withArgName("exclude-0-8").hasArg()
                        .withDescription("when listening on the specified port do not accept AMQP0-8 connections. The specified port must be one specified on the command line")
                        .withLongOpt("exclude-0-8").create();


        Option mport =
                OptionBuilder.withArgName("mport").hasArg()
                        .withDescription("listen on the specified management port. Overrides any value in the config file")
                        .withLongOpt("mport").create("m");


        Option bind =
                OptionBuilder.withArgName("bind").hasArg()
                        .withDescription("bind to the specified address. Overrides any value in the config file")
                        .withLongOpt("bind").create(BrokerOptions.BIND);
        Option logconfig =
                OptionBuilder.withArgName("logconfig").hasArg()
                        .withDescription("use the specified log4j xml configuration file. By "
                                + "default looks for a file named " + BrokerOptions.DEFAULT_LOG_CONFIG_FILE
                                + " in the same directory as the configuration file").withLongOpt("logconfig").create(BrokerOptions.LOG_CONFIG);
        Option logwatchconfig =
                OptionBuilder.withArgName("logwatch").hasArg()
                        .withDescription("monitor the log file configuration file for changes. Units are seconds. "
                                + "Zero means do not check for changes.").withLongOpt("logwatch").create(BrokerOptions.WATCH);

        Option sslport =
                OptionBuilder.withArgName("sslport").hasArg()
                        .withDescription("SSL port. Overrides any value in the config file")
                        .withLongOpt("sslport").create(BrokerOptions.SSL_PORTS);


        options.addOption(help);
        options.addOption(version);
        options.addOption(configFile);
        options.addOption(logconfig);
        options.addOption(logwatchconfig);
        options.addOption(port);
        options.addOption(exclude0_10);
        options.addOption(exclude0_9_1);
        options.addOption(exclude0_9);
        options.addOption(exclude0_8);
        options.addOption(mport);
        options.addOption(bind);
        options.addOption(sslport);
    }

    protected void execute() throws Exception {
        BrokerOptions options = new BrokerOptions();
        String configFile = commandLine.getOptionValue(BrokerOptions.CONFIG);
        if (configFile != null) {
            options.setConfigFile(configFile);
        }

        String logWatchConfig = commandLine.getOptionValue(BrokerOptions.WATCH);
        if (logWatchConfig != null) {
            options.setLogWatchFrequency(Integer.parseInt(logWatchConfig));
        }

        String logConfig = commandLine.getOptionValue(BrokerOptions.LOG_CONFIG);
        if (logConfig != null) {
            options.setLogConfigFile(logConfig);
        }

        String jmxPort = commandLine.getOptionValue(BrokerOptions.MANAGEMENT);
        if (jmxPort != null) {
            options.setJmxPort(Integer.parseInt(jmxPort));
        }

        String bindAddr = commandLine.getOptionValue(BrokerOptions.BIND);
        if (bindAddr != null) {
            options.setBind(bindAddr);
        }

        String[] portStr = commandLine.getOptionValues(BrokerOptions.PORTS);
        if (portStr != null) {
            parsePortArray(options, portStr, false);
            for (ProtocolExclusion pe : ProtocolExclusion.values()) {
                parsePortArray(options, commandLine.getOptionValues(pe.getExcludeName()), pe);
            }
        }

        String[] sslPortStr = commandLine.getOptionValues(BrokerOptions.SSL_PORTS);
        if (sslPortStr != null) {
            parsePortArray(options, sslPortStr, true);
            for (ProtocolExclusion pe : ProtocolExclusion.values()) {
                parsePortArray(options, commandLine.getOptionValues(pe.getExcludeName()), pe);
            }
        }

        startBroker(options);
    }

    protected void startBroker(final BrokerOptions options) throws Exception {
        Broker broker = new Broker();
        //TODO Fix this properly
        broker.startup(options);
    }

    protected void shutdown(final int status) {
        ApplicationRegistry.remove();
        System.exit(status);
        //todo need to add the ability to gracefully shutdown the MQTT server
    }

    private static void parsePortArray(final BrokerOptions options, final Object[] ports,
                                       final boolean ssl) throws InitException {
        if (ports != null) {
            for (Object port : ports) {
                try {
                    if (ssl) {
                        options.addSSLPort(Integer.parseInt(String.valueOf(port)));
                    } else {
                        options.addPort(Integer.parseInt(String.valueOf(port)));
                    }
                } catch (NumberFormatException e) {
                    throw new InitException("Invalid port: " + port, e);
                }
            }
        }
    }

    private static void parsePortArray(final BrokerOptions options, final Object[] ports,
                                       final ProtocolExclusion excludedProtocol) throws InitException {
        if (ports != null) {
            for (Object port : ports) {
                try {
                    options.addExcludedPort(excludedProtocol,Integer.parseInt(String.valueOf(port)));
                } catch (NumberFormatException e) {
                    throw new InitException("Invalid port for exclusion: " + port, e);
                }
            }
        }
    }
}
