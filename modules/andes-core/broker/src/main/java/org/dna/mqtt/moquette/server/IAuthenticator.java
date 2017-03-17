package org.dna.mqtt.moquette.server;

/**
 * username and password checker
 *
 * @author andrea
 */
public interface IAuthenticator {

    AuthenticationInfo checkValid(String username, String password);
}
