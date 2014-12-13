package org.dna.mqtt.moquette.server;

import org.dna.mqtt.moquette.messaging.spi.impl.SimpleMessaging;
import org.dna.mqtt.moquette.server.netty.NettyAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
/**
 * Launch a  configured version of the server.
 * @author andrea
 */
public class Server {

    public static final int DEFAULT_MQTT_PORT = 1833;
    private static final Logger log = LoggerFactory.getLogger(Server.class);
    
    public static final String STORAGE_FILE_PATH = System.getProperty("user.home") + 
            File.separator + "moquette_store.hawtdb";

    private ServerAcceptor m_acceptor;
    SimpleMessaging messaging;

    public void startServer(int port) throws IOException, AndesException {
        Properties configProps = loadConfigurations();
        configProps.put("port",Integer.toString(port));
        serverInit(configProps);
    }
    
    public void startServer() throws IOException, AndesException {
        Properties configProps = loadConfigurations();
        serverInit(configProps);
    }

    /**
     * Load configurations related to MQTT from Andes configuration files.
     *
     * @return Property collection
     * @throws AndesException
     */
    private Properties loadConfigurations() throws AndesException {

        Properties mqttProperties = new Properties();

        mqttProperties.put("port",
                AndesConfigurationManager.getInstance().readConfigurationValue(AndesConfiguration
                        .TRANSPORTS_MQTT_PORT));

        mqttProperties.put("sslPort",
                AndesConfigurationManager.getInstance().readConfigurationValue(AndesConfiguration
                        .TRANSPORTS_MQTT_SSL_PORT));

        mqttProperties.put("host",AndesConfigurationManager.getInstance().readConfigurationValue
                (AndesConfiguration.TRANSPORTS_BIND_ADDRESS));

        return mqttProperties;
    }

    private void serverInit(Properties configProps) throws IOException {
        messaging = SimpleMessaging.getInstance();
        messaging.init(configProps);

        m_acceptor = new NettyAcceptor();
        m_acceptor.initialize(messaging, configProps);
    }

    public void stopServer() {
        log.info("MQTT Server is stopping...");
        messaging.stop();
        m_acceptor.close();
        log.info("MQTT Server has stopped.");
    }
}
