package org.dna.mqtt.moquette.server;

import org.dna.mqtt.moquette.messaging.spi.impl.SimpleMessaging;
import org.dna.mqtt.moquette.server.netty.NettyAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;
/**
 * Launch a  configured version of the server.
 * @author andrea
 */
public class Server {

    public static final int DEFAULT_MQTT_PORT = 1833;
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    
    public static final String STORAGE_FILE_PATH = System.getProperty("user.home") + 
            File.separator + "moquette_store.hawtdb";

    private ServerAcceptor m_acceptor;
    SimpleMessaging messaging;
    
/*    public static void main(String[] args) throws IOException {
        
        final Server server = new Server();
        server.startServer();
        System.out.println("Server started, version 0.5");
        //Bind  a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.stopServer();
            }
        });
        
    }*/

    public void startServer(int port) throws IOException {
        Properties configProps = loadConfigurations();
        configProps.put("port",Integer.toString(port));
        serverInit(configProps);
    }
    
    public void startServer() throws IOException {
        Properties configProps = loadConfigurations();
        serverInit(configProps);
    }

    private Properties loadConfigurations() {
        ConfigurationParser confParser = new ConfigurationParser();
        try {
            String configPath = System.getProperty("moquette.path", "");
            confParser.parse(new File(configPath + "mqtt/moquette.conf"));
        } catch (ParseException pex) {
            LOG.warn("An error occured in parsing configuration, fallback on default configuration", pex);
        }

        return confParser.getProperties();
    }

    private void serverInit(Properties configProps) throws IOException {
        messaging = SimpleMessaging.getInstance();
        messaging.init(configProps);

        m_acceptor = new NettyAcceptor();
        m_acceptor.initialize(messaging, configProps);
    }

    public void stopServer() {
        System.out.println("Server stopping...");
        messaging.stop();
        m_acceptor.close();
        System.out.println("Server stopped");
    }
}
