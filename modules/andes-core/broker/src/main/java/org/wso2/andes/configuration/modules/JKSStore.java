/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.configuration.modules;

import org.apache.commons.configuration.ConfigurationException;
import org.wso2.andes.configuration.AndesConfigurationManager;

import java.io.File;

/**
 * Common class used to maintain and parse JKS stores specified in broker.xml.
 * <p/>
 * This is an example for modularizing configurations for re-usability. Since JKS stores are used for both AMQP and
 * MQTT, the following config block is used repeatedly within the broker.xml :
 * <p/>
 * <keyStore>
 * <location>repository/resources/security/wso2carbon.jks</location>
 * <password>wso2carbon</password>
 * </keyStore>
 * <p/>
 * So this class is used to parse this block into a common data structure.
 * Refer usages of the class in AndesConfiguration for more information.
 */
public class JKSStore {

    /**
     * Default values
     */
    private final String DEFAULT_STORE_LOCATION = "repository" + File.separator + "resources" + File.separator +
            "security" + File.separator + "wso2carbon.jks";
    private final String DEFAULT_STORE_PASSWORD = "wso2carbon";

    /**
     * Relative xpaths which are appended to the input root XPath at constructor.
     */
    private final String relativeXPathForLocation = "/location";
    private final String relativeXPathForPassword = "/password";

    /**
     * Physical location of the JKS store, relative to PRODUCT_HOME
     */
    private String storeLocation;

    /**
     * password of the JKS store.
     */
    private String password;

    public String getStoreLocation() {
        return storeLocation;
    }

    public String getPassword() {
        return password;
    }

    public JKSStore(String rootXPath) throws ConfigurationException {

        String locationXPath = rootXPath + relativeXPathForLocation;
        String passwordXPath = rootXPath + relativeXPathForPassword;

        // After deriving the full xpaths, the AndesConfigurationManager is used to extract the values for each
        // property.
        storeLocation = AndesConfigurationManager.deriveValidConfigurationValue(locationXPath, String.class,
                DEFAULT_STORE_LOCATION);
        password = AndesConfigurationManager.deriveValidConfigurationValue(passwordXPath, String.class,
                DEFAULT_STORE_PASSWORD);
    }
}
