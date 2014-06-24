package org.dna.mqtt.moquette.server;

import org.dna.mqtt.moquette.messaging.spi.IMessaging;

import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author andrea
 */
public interface ServerAcceptor {
    
    void initialize(IMessaging messaging, Properties props) throws IOException;
    
    void close();
}
