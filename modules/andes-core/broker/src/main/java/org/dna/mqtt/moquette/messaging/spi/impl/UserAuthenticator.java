package org.dna.mqtt.moquette.messaging.spi.impl;

import org.apache.commons.lang.StringUtils;
import org.dna.mqtt.moquette.server.IAuthenticator;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserAuthenticator implements IAuthenticator {
    
    private Map<String, String> m_identities = new HashMap<String, String>();
    
    UserAuthenticator() throws AndesException {

        List<String> list = AndesConfigurationManager.getInstance().readPropertyList
                (AndesConfiguration.LIST_TRANSPORTS_MQTT_USERNAMES);

        for (int i =1; i<list.size(); i++) {
            String userName = AndesConfigurationManager.getInstance().readPropertyOfChildByIndex
                    (AndesConfiguration.TRANSPORTS_MQTT_USERNAME, i);
            String password = AndesConfigurationManager.getInstance().readPropertyOfChildByIndex
                    (AndesConfiguration.TRANSPORTS_MQTT_PASSWORD, i);

            m_identities.put(userName,password);
        }
    }
    
    public boolean checkValid(String username, String password) {
        String foundPwq = m_identities.get(username);
        return !StringUtils.isBlank(foundPwq) && foundPwq.equals(password);
    }
    
}
