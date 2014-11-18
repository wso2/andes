/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.andes.configuration;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.configuration.util.ConfigurationProperty;
import org.wso2.andes.kernel.AndesException;
import org.wso2.carbon.utils.ServerConstants;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * This class acts as a singleton access point for all config parameters used within MB.
 */
public class AndesConfigurationManager {

    private static Log log = LogFactory.getLog(AndesConfigurationManager.class);

    private static volatile AndesConfigurationManager instance;

    public static final String GENERIC_CONFIGURATION_PARSE_ERROR = "Error occurred when trying " +
            "to parse configuration value : ";

    /**
     * Main file path of configuration files into which all other andes-specific config files (if any)
     * should be linked.
     */
    private static final String ROOT_CONFIG_FILE_PATH = System.getProperty(ServerConstants.CARBON_HOME) +
            "/repository/conf/";

    private static final String ROOT_CONFIG_FILE_NAME = "broker.xml";

    private final CompositeConfiguration compositeConfiguration;

    private AndesConfigurationManager() throws AndesException, ConfigurationException, UnknownHostException {

        compositeConfiguration = new CompositeConfiguration();

        log.info("Main andes configuration located at : " + ROOT_CONFIG_FILE_PATH + ROOT_CONFIG_FILE_NAME);

        XMLConfiguration rootConfiguration = new XMLConfiguration(ROOT_CONFIG_FILE_PATH + ROOT_CONFIG_FILE_NAME);
        rootConfiguration.setExpressionEngine(new XPathExpressionEngine());

        compositeConfiguration.addConfiguration(rootConfiguration);

        // Load and combine other configurations linked to broker.xml
        List<String> linkedConfigurations = compositeConfiguration.getList("links/link");

        for (String linkedConfigurationPath : linkedConfigurations) {

            log.info("Linked configuration file path : " + ROOT_CONFIG_FILE_PATH +
                    linkedConfigurationPath);

            XMLConfiguration linkedConfiguration = new XMLConfiguration(ROOT_CONFIG_FILE_PATH +
                    linkedConfigurationPath);
            linkedConfiguration.setExpressionEngine(new XPathExpressionEngine());
            compositeConfiguration.addConfiguration(linkedConfiguration);
        }

        compositeConfiguration.setDelimiterParsingDisabled(true); // If we don't do this,
        // we can't add a new configuration by code.

        // derive certain special properties that are not simply specified in
        // the configuration files.
        addDerivedProperties();
    }

    public static AndesConfigurationManager getInstance() throws AndesException {
        if (instance == null) {
            synchronized (AndesConfigurationManager.class) {
                try {
                    instance = new AndesConfigurationManager();
                } catch (ConfigurationException e) {
                    throw new AndesException("Error occurred when trying to construct configurations from " + "files" +
                            ".", e);
                    // The error is not propagated upwards here, because every point in code that
                    // accesses configurations will need to handle it. code duplication.)
                    // Since we have default values to use in a failure,
                    // logging the error here is sufficient.
                } catch (UnknownHostException e) {
                    throw new AndesException("Error occurred when trying to derive the bind address for messaging.", e);
                }
            }
        }
        return instance;
    }

    /**
     * The sole method exposed to everyone accessing configurations. We can use the relevant
     * enums (e.g.- config.enums.BrokerConfiguration) to pass the required property and
     * its meta information.
     *
     * @param configurationProperty relevant enum value (e.g.- config.enums.AndesConfiguration)
     * @param <T>                   Expected data type of the property
     * @return Value of config in the expected data type.
     * @throws org.wso2.andes.kernel.AndesException
     *
     */
    public <T> T readConfigurationValue(ConfigurationProperty configurationProperty) throws AndesException {

        String valueInFile = compositeConfiguration.getString(configurationProperty.get()
                .getKeyInFile());

        try {
            // The cast to T is unavoidable. Even though the function returns the same data type,
            // compiler doesn't know about it. We could add the data type as a parameter,
            // but that only complicates the method call.
            return (T) deriveValidConfigurationValue(configurationProperty.get().getKeyInFile(),
                    configurationProperty.get().getDataType(),
                    configurationProperty.get().getDefaultValue(), valueInFile);
        } catch (ConfigurationException e) {
            log.error(e); // Since the descriptive message is wrapped in exception itself

            // Return the parsed default value. This path will be met if a user adds an invalid value to a property.
            // Assuming we always have proper default values defined, this will rescue us from exploding due to a
            // small mistake.
            try {
                return (T) deriveValidConfigurationValue(configurationProperty.get().getKeyInFile(),
                        configurationProperty.get().getDataType(), configurationProperty.get()
                        .getDefaultValue(), null);
            } catch (ConfigurationException e1) {
                // It is highly unlikely that this will throw an exception. But if it does, it should be propagated.
                throw new AndesException(GENERIC_CONFIGURATION_PARSE_ERROR +
                        configurationProperty.toString(), e);
            }
        }
    }

    /**
     * Using this method, you can access a singular property of a child.
     * <p/>
     * example,
     * <p/>
     * <users>
     * <user userName="testuser1" password="password1" />
     * <user userName="testuser2" password="password2" />
     * </users> scenario.
     *
     * @param configurationProperty relevant enum value (e.g.- above scenario -> config
     *                              .enums.AndesConfiguration.TRANSPORTS_MQTT_PASSWORD)
     * @param index                 index of the child of whom you seek the property (e.g. above scenario -> 1)
     * @throws org.wso2.andes.kernel.AndesException
     *
     */
    public <T> T readPropertyOfChildByIndex(AndesConfiguration configurationProperty, int index) throws
            AndesException {

        String constructedKey = configurationProperty.get().getKeyInFile().replace("{i}",
                String.valueOf(index));

        String valueInFile = compositeConfiguration.getString(constructedKey);

        // The cast to T is unavoidable. Even though the function returns the same data type,
        // compiler doesn't know about it. We could add the data type as a parameter,
        // but that only complicates the method call.
        try {
            return (T) deriveValidConfigurationValue(configurationProperty.get().getKeyInFile(),
                    configurationProperty.get().getDataType(),
                    configurationProperty.get().getDefaultValue(), valueInFile);
        } catch (ConfigurationException e) {
            throw new AndesException(GENERIC_CONFIGURATION_PARSE_ERROR + configurationProperty
                    .toString(), e);
        }
    }

    /**
     * Using this method, you can access a singular property of a child.
     * <p/>
     * example,
     * <p/>
     * <users>
     * <user userName="testuser1">password1</user>
     * <user userName="testuser2">password2</user>
     * </users> scenario.
     *
     * @param configurationProperty relevant enum value (e.g.- above scenario -> config
     *                              .enums.AndesConfiguration.TRANSPORTS_MQTT_PASSWORD)
     * @param key                   key of the child of whom you seek the value (e.g. above scenario -> "testuser2")
     */
    public <T> T readValueOfChildByKey(AndesConfiguration configurationProperty, String key) throws
            AndesException {

        String constructedKey = configurationProperty.get().getKeyInFile().replace("{key}",
                key);

        String valueInFile = compositeConfiguration.getString(constructedKey);

        // The cast to T is unavoidable. Even though the function returns the same data type,
        // compiler doesn't know about it. We could add the data type as a parameter,
        // but that only complicates the method call.
        try {
            return (T) deriveValidConfigurationValue(configurationProperty.get().getKeyInFile(),
                    configurationProperty.get().getDataType(),
                    configurationProperty.get().getDefaultValue(), valueInFile);
        } catch (ConfigurationException e) {
            throw new AndesException(GENERIC_CONFIGURATION_PARSE_ERROR + configurationProperty
                    .toString(), e);
        }
    }

    /**
     * Use this method when you need to acquire a list of properties of same group.
     *
     * @param configurationProperty relevant enum value (e.g.- config.enums.AndesConfiguration
     *                              .LIST_TRANSPORTS_MQTT_USERNAMES)
     * @return String list of required property values
     */
    public List<String> readPropertyList(AndesConfiguration configurationProperty) {

        if (configurationProperty.toString().startsWith("LIST_")) {
            return compositeConfiguration.getList(configurationProperty.get().getKeyInFile());
        } else {
            log.error("Invalid Property to request a list : " + configurationProperty.get().getKeyInFile());
            return new ArrayList<String>();
        }
    }

    /**
     * Given the data type and the value read from a config, this returns the parsed value
     * of the property.
     *
     * @param key          The Key to the property being read (n xpath format as contained in file.)
     * @param dataType     Expected data type of the property
     * @param defaultValue This parameter should NEVER be null since we assign a default value to
     *                     every config property.
     * @param readValue    Value read from the config file
     * @param <T>          Expected data type of the property
     * @return Value of config in the expected data type.
     */
    private static <T> T deriveValidConfigurationValue(String key, Class<T> dataType,
                                                       String defaultValue,
                                                       String readValue) throws ConfigurationException {

        if (log.isDebugEnabled()) {
            log.debug("Reading andes configuration value " + key);
        }

        String validValue = defaultValue;

        if (StringUtils.isBlank(readValue)) {
            log.warn("Error when trying to read property : " + key + ". Switching to " + "default value : " +
                    defaultValue);
        } else {
            validValue = readValue;
        }

        if (log.isDebugEnabled()) {
            log.debug("Valid value read for andes configuration property " + key + " is : " + validValue);
        }

        try {
            if (Boolean.class.equals(dataType)) {
                return dataType.cast(Boolean.parseBoolean(validValue));
            } else if (Date.class.equals(dataType)) {
                // Sample date : "Sep 28 20:29:30 JST 2000"
                DateFormat df = new SimpleDateFormat("MMM dd kk:mm:ss z yyyy", Locale.ENGLISH);
                return dataType.cast(df.parse(validValue));
            } else {
                return dataType.getConstructor(String.class).newInstance(validValue);
            }
        } catch (NoSuchMethodException e) {
            throw new ConfigurationException(GENERIC_CONFIGURATION_PARSE_ERROR + key, e);
        } catch (ParseException e) {
            throw new ConfigurationException(GENERIC_CONFIGURATION_PARSE_ERROR + key, e);
        } catch (IllegalAccessException e) {
            throw new ConfigurationException(GENERIC_CONFIGURATION_PARSE_ERROR + key, e);
        } catch (InvocationTargetException e) {
            throw new ConfigurationException(GENERIC_CONFIGURATION_PARSE_ERROR + key, e);
        } catch (InstantiationException e) {
            throw new ConfigurationException(GENERIC_CONFIGURATION_PARSE_ERROR + key, e);
        }
    }

    /**
     * This method is used to derive certain special properties that are not simply specified in
     * the configuration files.
     */
    private void addDerivedProperties() throws AndesException, UnknownHostException {

        // For AndesConfiguration.TRANSPORTS_BIND_ADDRESS
        if ("*".equals(this.readConfigurationValue(AndesConfiguration.TRANSPORTS_BIND_ADDRESS))) {

            InetAddress host = InetAddress.getLocalHost();
            compositeConfiguration.setProperty(AndesConfiguration.TRANSPORTS_BIND_ADDRESS.get().getKeyInFile(),
                    host.getHostAddress());
        }
    }

}
