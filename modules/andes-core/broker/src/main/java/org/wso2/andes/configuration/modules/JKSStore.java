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
 */
public class JKSStore {

    private final String DEFAULT_STORE_LOCATION = "repository" + File.separator + "resources" + File.separator +
            "security" + File.separator + "wso2carbon.jks";
    private final String DEFAULT_STORE_PASSWORD = "wso2carbon";

    private final String relativeXPathForLocation = "/location";
    private final String relativeXPathForPassword = "/password";

    private String storeLocation;
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

        storeLocation = AndesConfigurationManager.deriveValidConfigurationValue(locationXPath, String.class,
                DEFAULT_STORE_LOCATION);
        password = AndesConfigurationManager.deriveValidConfigurationValue(passwordXPath, String.class,
                DEFAULT_STORE_PASSWORD);
    }
}
