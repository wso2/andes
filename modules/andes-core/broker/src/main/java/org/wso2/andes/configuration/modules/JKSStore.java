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
import org.apache.commons.lang.StringUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;

import java.io.File;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * Common class used to maintain and parse JKS stores specified in broker.xml.
 * <p/>
 * This is an example for modularizing configurations for re-usability.
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
     * Common path fragment for all key/trust stores.
     */
    private static final String JKS_BASE_PATH = "repository" + File.separator + "resources" + File.separator +
                                         "security" + File.separator;
    /**
     * Default store password
     */
    private final String DEFAULT_STORE_PASSWORD = "wso2carbon";

    /**
     * Relative xpaths which are appended to the input root XPath at constructor.
     */
    private final String relativeXPathForLocation = "/location";
    private final String relativeXPathForPassword = "/password";
    private final String relativeXPathForStoreAlgorithm = "/certType";
    
    
    /**
     * Physical location of the JKS store, relative to PRODUCT_HOME
     */
    private String storeLocation;

    /**
     * password of the JKS store.
     */
    private String password;

    /**
     * Algorithm ( / certificate type) used for the store. (e.g. SunX509)
     */
    private String storeAlgorithm;
    
    public String getStoreLocation() {
        return storeLocation;
    }

    public String getPassword() {
        return password;
    }

    public String getStoreAlgorithm() {
        return storeAlgorithm;
    }

    public void setStoreAlgorithm(String storeAlgorithm) {
        this.storeAlgorithm = storeAlgorithm;
    }
    
    public JKSStore(String rootXPath) throws ConfigurationException {

        String locationXPath = rootXPath + relativeXPathForLocation;
        String passwordXPath = rootXPath + relativeXPathForPassword;
        String storeAlgorithmXPath = rootXPath + relativeXPathForStoreAlgorithm;
        
        String defaultStoreLocation = null;
        String defaultStoreAlgorithm = null;
        
        if (StringUtils.containsIgnoreCase(rootXPath,"trustStore")) {
            defaultStoreLocation = JKS_BASE_PATH + "wso2carbon.jks";
            defaultStoreAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        } else {
            defaultStoreAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            defaultStoreLocation = JKS_BASE_PATH + "client-truststore.jks";
        }
        
        // After deriving the full xpaths, the AndesConfigurationManager is used to extract the values for each
        // property.
        storeLocation = AndesConfigurationManager.deriveValidConfigurationValue(locationXPath, String.class,
                defaultStoreLocation);
        password = AndesConfigurationManager.deriveValidConfigurationValue(passwordXPath, String.class,
                DEFAULT_STORE_PASSWORD);
        storeAlgorithm = AndesConfigurationManager.deriveValidConfigurationValue(storeAlgorithmXPath, String.class,
                                                                           defaultStoreAlgorithm);
    }
}
