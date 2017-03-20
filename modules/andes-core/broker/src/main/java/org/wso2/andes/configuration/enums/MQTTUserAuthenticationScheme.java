package org.wso2.andes.configuration.enums;


/**
 * Represents a configuration options instructing ( the server that )  mqtt users should always
 * supply credentials while establishing connections.
 */

public enum MQTTUserAuthenticationScheme {

    /**
     * Adhere's MQTT spec 3.1 where authentication is optional.
     */
    OPTIONAL,
    /**
     * Enforces every connection made should send credentials
     */
    REQUIRED

}
