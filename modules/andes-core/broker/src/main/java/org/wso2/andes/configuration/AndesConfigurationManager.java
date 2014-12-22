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
import java.text.MessageFormat;
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

    /**
     * Reserved Suffixes that activate different processing logic.
     */
    private static final String PORT_TYPE = "_PORT";

    /**
     * Reserved Prefixes that activate different processing logic.
     */
    private static final String LIST_TYPE = "LIST_";

    /**
     * Common Error states
     */
    private static final String GENERIC_CONFIGURATION_PARSE_ERROR = "Error occurred when trying to parse " +
            "configuration value {0}.";

    private static final String NO_CHILD_FOR_INDEX_IN_PROPERTY = "There was no child at the given index {0} for the " +
            "parent property {1}.";

    private static final String NO_CHILD_FOR_KEY_IN_PROPERTY = "There was no child at the given key {0} for the " +
            "parent property {1}.";

    private static final String PROPERTY_NOT_A_LIST = "The input property {0} does not contain a list of child " +
            "properties.";

    private static final String PROPERTY_NOT_A_PORT = "The input property {0} is not defined as an integer value. " +
            "Therefore it is not a port property.";

    /**
     * Main file path of configuration files into which all other andes-specific config files (if any)
     * should be linked.
     */
    private static final String ROOT_CONFIG_FILE_PATH = System.getProperty(ServerConstants.CARBON_HOME) +
            "/repository/conf/";

    /**
     * File name of the main configuration file.
     */
    private static final String ROOT_CONFIG_FILE_NAME = "broker.xml";

    /**
     * Apache commons composite configuration is used to collect and maintain properties from multiple configuration
     * sources.
     */
    private static CompositeConfiguration compositeConfiguration;

    /**
     * Decisive configurations coming from carbon.xml that affect the MB configs. e.g port Offset
     * These are injected as custom logic when reading the configurations.
     */
    private static int carbonPortOffset;

    /**
     * initialize the configuration manager. this MUST be called at application startup.
     * (QpidServiceComponent bundle -> activate event)
     *
     * @throws AndesException
     */
    public static void initialize(int portOffset) throws AndesException {
        try {

            compositeConfiguration = new CompositeConfiguration();

            log.info("Main andes configuration located at : " + ROOT_CONFIG_FILE_PATH + ROOT_CONFIG_FILE_NAME);

            XMLConfiguration rootConfiguration = new XMLConfiguration(ROOT_CONFIG_FILE_PATH + ROOT_CONFIG_FILE_NAME);
            rootConfiguration.setExpressionEngine(new XPathExpressionEngine());

            compositeConfiguration.addConfiguration(rootConfiguration);

            // Load and combine other configurations linked to broker.xml
            List<String> linkedConfigurations = compositeConfiguration.getList("links/link");

            for (String linkedConfigurationPath : linkedConfigurations) {

                log.info("Linked configuration file path : " + ROOT_CONFIG_FILE_PATH + linkedConfigurationPath);

                XMLConfiguration linkedConfiguration = new XMLConfiguration(ROOT_CONFIG_FILE_PATH +
                        linkedConfigurationPath);
                linkedConfiguration.setExpressionEngine(new XPathExpressionEngine());
                compositeConfiguration.addConfiguration(linkedConfiguration);
            }

            compositeConfiguration.setDelimiterParsingDisabled(true); // If we don't do this,
            // we can't add a new configuration to the compositeConfiguration by code.

            // derive certain special properties that are not simply specified in
            // the configuration files.
            addDerivedProperties();

            // set carbonPortOffset coming from carbon
            AndesConfigurationManager.carbonPortOffset = portOffset;

        } catch (ConfigurationException e) {
            String error = "Error occurred when trying to construct configurations from file at path : " +
                    ROOT_CONFIG_FILE_PATH + ROOT_CONFIG_FILE_NAME;
            log.error(error, e);
            throw new AndesException(error, e);

        } catch (UnknownHostException e) {
            String error = "Error occurred when trying to derive the bind address for messaging from configurations.";
            log.error(error, e);
            throw new AndesException(error, e);
        }
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
     */
    public static <T> T readValue(ConfigurationProperty configurationProperty) {

        // If the property requests a port value, we need to apply the carbon offset to it.
        if (configurationProperty.get().getName().endsWith(PORT_TYPE)) {
            return (T) readPortValue(configurationProperty);
        }

        String valueInFile = compositeConfiguration.getString(configurationProperty.get()
                .getKeyInFile());

        try {
            // The cast to T is unavoidable. Even though the function returns the same data type,
            // compiler doesn't know about it. We could add the data type as a parameter,
            // but that only complicates the method call.
            return (T) deriveValidConfigurationValue(configurationProperty.get().getKeyInFile(),
                    configurationProperty.get().getDataType(), configurationProperty.get().getDefaultValue(),
                    valueInFile);
        } catch (ConfigurationException e) {

            log.error(e); // Since the descriptive message is wrapped in exception itself

            // Return the parsed default value. This path will be met if a user adds an invalid value to a property.
            // Assuming we always have proper default values defined, this will rescue us from exploding due to a
            // small mistake.
            try {
                return (T) deriveValidConfigurationValue(configurationProperty.get().getKeyInFile(),
                        configurationProperty.get().getDataType(), configurationProperty.get().getDefaultValue(), null);
            } catch (ConfigurationException e1) {
                // It is highly unlikely that this will throw an exception (if defined default values are also invalid).
                // But if it does, the method will return null.
                // Exception is not propagated to avoid unnecessary clutter of config related exception handling.

                log.error(e); // Since the descriptive message is wrapped in exception itself
                return null;
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
     */
    public static <T> T readValueOfChildByIndex(AndesConfiguration configurationProperty, int index) {

        String constructedKey = configurationProperty.get().getKeyInFile().replace("{i}", String.valueOf(index));

        String valueInFile = compositeConfiguration.getString(constructedKey);

        // The cast to T is unavoidable. Even though the function returns the same data type,
        // compiler doesn't know about it. We could add the data type as a parameter,
        // but that only complicates the method call.
        try {
            return (T) deriveValidConfigurationValue(configurationProperty.get().getKeyInFile(),
                    configurationProperty.get().getDataType(),
                    configurationProperty.get().getDefaultValue(), valueInFile);
        } catch (ConfigurationException e) {
            // This means that there is no child by the given index for the parent property.
            log.error(MessageFormat.format(NO_CHILD_FOR_INDEX_IN_PROPERTY, index, configurationProperty), e);
            return null;
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
    public static <T> T readValueOfChildByKey(AndesConfiguration configurationProperty, String key) {

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
            // This means that there is no child by the given key for the parent property.
            log.error(MessageFormat.format(NO_CHILD_FOR_KEY_IN_PROPERTY, key, configurationProperty), e);
            return null;
        }
    }

    /**
     * Use this method when you need to acquire a list of properties of same group.
     *
     * @param configurationProperty relevant enum value (e.g.- config.enums.AndesConfiguration
     *                              .LIST_TRANSPORTS_MQTT_USERNAMES)
     * @return String list of required property values
     */
    public static List<String> readValueList(AndesConfiguration configurationProperty) {

        if (configurationProperty.toString().startsWith(LIST_TYPE)) {
            return compositeConfiguration.getList(configurationProperty.get().getKeyInFile());
        } else {
            log.error(MessageFormat.format(PROPERTY_NOT_A_LIST, configurationProperty));
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
     * @throws ConfigurationException
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
            throw new ConfigurationException(MessageFormat.format(GENERIC_CONFIGURATION_PARSE_ERROR, key), e);
        } catch (ParseException e) {
            throw new ConfigurationException(MessageFormat.format(GENERIC_CONFIGURATION_PARSE_ERROR, key), e);
        } catch (IllegalAccessException e) {
            throw new ConfigurationException(MessageFormat.format(GENERIC_CONFIGURATION_PARSE_ERROR, key), e);
        } catch (InvocationTargetException e) {
            throw new ConfigurationException(MessageFormat.format(GENERIC_CONFIGURATION_PARSE_ERROR, key), e);
        } catch (InstantiationException e) {
            throw new ConfigurationException(MessageFormat.format(GENERIC_CONFIGURATION_PARSE_ERROR, key), e);
        }
    }

    /**
     * This method is used to derive certain special properties that are not simply specified in
     * the configuration files.
     */
    private static void addDerivedProperties() throws AndesException, UnknownHostException {

        // For AndesConfiguration.TRANSPORTS_BIND_ADDRESS
        if ("*".equals(readValue(AndesConfiguration.TRANSPORTS_BIND_ADDRESS))) {

            InetAddress host = InetAddress.getLocalHost();
            compositeConfiguration.setProperty(AndesConfiguration.TRANSPORTS_BIND_ADDRESS.get().getKeyInFile(),
                    host.getHostAddress());
        }
    }

    /**
     * This method is used when reading a port value from configuration. It is intended to abstract the port offset
     * logic.If the enum contains keyword "_PORT", this will be called
     *
     * @param configurationProperty relevant enum value (e.g.- above scenario -> config.enums.AndesConfiguration
     *                              .TRANSPORTS_MQTT_PORT)
     * @return port with carbon port offset
     */
    private static Integer readPortValue(ConfigurationProperty configurationProperty) {

        if (!Integer.class.equals(configurationProperty.get().getDataType())) {
            log.error(MessageFormat.format(AndesConfigurationManager.PROPERTY_NOT_A_PORT, configurationProperty));
            return 0; // 0 can never be a valid port. therefore, returning 0 in the error path will keep code
            // predictable.
        }

        try {
            String valueInFile = compositeConfiguration.getString(configurationProperty.get().getKeyInFile());

            Integer portFromConfiguration = (Integer) deriveValidConfigurationValue(configurationProperty.get()
                    .getKeyInFile(), configurationProperty.get().getDataType(),
                    configurationProperty.get().getDefaultValue(), valueInFile);

            return portFromConfiguration + carbonPortOffset;

        } catch (ConfigurationException e) {
            log.error(MessageFormat.format(GENERIC_CONFIGURATION_PARSE_ERROR, configurationProperty), e);

            //recover and return default port with offset value.
            return Integer.parseInt(configurationProperty.get().getDefaultValue()) + carbonPortOffset;
        }
    }

}
