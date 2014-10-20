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

package org.wso2.andes.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to store property value pairs related to configurations
 *
 */
public class ConfigurationProperties {

    /**
     * Internal map to store property values
     */
    private Map<String, String> propertyValueMap;

    public ConfigurationProperties() {
        propertyValueMap = new HashMap<String, String>();
    }

    /**
     * Adds a property
     * @param property property name
     * @param value value of property
     */
    public void addProperty(String property, String value) {
        propertyValueMap.put(property, value);
    }

    /**
     * Returns a value for the given property
     * @param propertyName property name
     * @return value for the property, empty string for invalid property
     */
    public String getProperty(String propertyName) {
        String value = propertyValueMap.get(propertyName);
        if(value == null) {
            value = "";
        }
        return value;
    }

}
