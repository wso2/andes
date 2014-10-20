/*
 *  Copyright (c) 2008, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.wso2.andes.wso2.jndi;

import org.wso2.andes.jndi.PropertiesFileInitialContextFactory;
import org.wso2.andes.util.Strings;
import javax.naming.Context;
import javax.naming.NamingException;
import java.io.*;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantAwareInitialContextFactory extends PropertiesFileInitialContextFactory {
    private static final Logger _logger = LoggerFactory.getLogger(TenantAwareInitialContextFactory.class);
    private static final String TRANSPORT_CON_FAC = "transport.jms.ConnectionFactoryJNDIName";
    private static final String DEFAULT_CON_FAC = "org.apache.qpid.wso2.default.ConnectionFactoryJNDIName";
    private static final String USERNAEM_START_TOKEN = "://";
    private static final String USERNAME_END_TOKEN = ":";
    private static final String DOMAIN_NAME_SEPARATOR = "@";
    private static final String DOMAIN_NAME_SEPARATOR_INTERNAL = "!";

    public Context getInitialContext(Hashtable environment) throws NamingException {
        Map data = new ConcurrentHashMap();

        // Load properties from file if provided
        loadPropertiesFromFile(environment);

        String tenantDomain = getTenantDomain(environment);
        if (!tenantDomain.isEmpty()) {
            makeNamesTenantAware(environment, tenantDomain);
        }

        // Load properties from Carbon Registry and create objects
        // Connection factories
        // setEnvironmentProperties(JMSRegistryClient.getConnectionFactories(), environment,getConnectionPrefix());
        createConnectionFactories(data, environment);

        // Queues
        //setEnvironmentProperties(JMSRegistryClient.getQueues(), environment, getQueuePrefix());
        createQueues(data, environment);

        // Topics
        //setEnvironmentProperties(JMSRegistryClient.getTopics(), environment, getTopicPrefix());
        createTopics(data, environment);

        // Destinations
        //setEnvironmentProperties(JMSRegistryClient.getDestinations(), environment, getDestinationPrefix());
        createDestinations(data, environment);

        return createContext(data, environment);
    }

    private void loadPropertiesFromFile(Hashtable environment) {
        try
        {
            String file = null;
            if (environment.containsKey(Context.PROVIDER_URL))
            {
                file = (String) environment.get(Context.PROVIDER_URL);
            }
            else
            {
                file = System.getProperty(Context.PROVIDER_URL);
            }

            if (file != null)
            {
                _logger.info("Loading Properties from:" + file);

                // Load the properties specified
                // This load properties method has written to avoid the issue where all the \
                // get vanished in a path like E:\shared\WSO2AS~1.1\bin
                Properties p = loadProperties(file);

                Strings.Resolver resolver = new Strings.ChainedResolver
                    (Strings.SYSTEM_RESOLVER, new Strings.PropertiesResolver(p));

                for (Map.Entry me : p.entrySet())
                {
                    String key = (String) me.getKey();
                    String value = (String) me.getValue();
                    String expanded = Strings.expand(value, resolver);
                    environment.put(key, expanded);
                    if (System.getProperty(key) == null)
                    {
                        System.setProperty(key, expanded);
                    }
                }
                _logger.info("Loaded Context Properties:" + environment.toString());
            }
            else
            {
                _logger.info("No Provider URL specified.");
            }
        }
        catch (IOException ioe)
        {
            _logger.warn("Unable to load property file specified in Provider_URL:" + environment.get(Context.PROVIDER_URL) +"\n" +
                         "Due to:"+ioe.getMessage());
        }
    }

    private String getTenantDomain(Hashtable environment) {
        String tenantDomain = "";
        String defaultConnectionFactoryName = null;

        if (environment.containsKey(TRANSPORT_CON_FAC)) {
            defaultConnectionFactoryName = (String)environment.get(TRANSPORT_CON_FAC);
        } else if (environment.containsKey(DEFAULT_CON_FAC)) {
            defaultConnectionFactoryName = (String)environment.get(DEFAULT_CON_FAC);
        }
        
        if (null != defaultConnectionFactoryName) {
            String defaultConnectionFactory =
                    (String)environment.get(getConnectionPrefix() + defaultConnectionFactoryName);

            if (null != defaultConnectionFactory) {
                // amqp://tenantuser@tenantdomain:tenantuserpassword@clientid/test?brokerlist='tcp://localhost:5672'
                int startIndex = defaultConnectionFactory.indexOf(USERNAEM_START_TOKEN);
                if (-1 != startIndex) {
                    startIndex+= USERNAEM_START_TOKEN.length();
                    int endIndex = defaultConnectionFactory.indexOf(USERNAME_END_TOKEN, startIndex);

                    String username = defaultConnectionFactory.substring(startIndex, endIndex);
                    if (null != username) {
                        int domainNameSeparatorIndex = username.indexOf(DOMAIN_NAME_SEPARATOR);
                        if (-1 != domainNameSeparatorIndex) { // Tenant username
                            tenantDomain = username.substring(domainNameSeparatorIndex + 1);

                            // Replace . with - in domain name
                            if (null != tenantDomain) {
                                tenantDomain = tenantDomain.replace(".", "-");
                            }

                            // Replace @ in the tenant username with # as it is not supported by Qpid
                            String defaultConnectionFactoryInternal = getInternalConnectionURL(
                                    defaultConnectionFactory, startIndex + domainNameSeparatorIndex);
                            // Replace connection url
                            environment.put(getConnectionPrefix() + defaultConnectionFactoryName,
                                    defaultConnectionFactoryInternal);
                        }
                    }
                }
            }
        }

        return tenantDomain;
    }

    private String getInternalConnectionURL(String connectionURL, int domainNameSeparatorIndex) {
        StringBuffer buffer = new StringBuffer();

        buffer.append(connectionURL.substring(0, domainNameSeparatorIndex));
        buffer.append(DOMAIN_NAME_SEPARATOR_INTERNAL);
        buffer.append(connectionURL.substring(domainNameSeparatorIndex + 1));

        return buffer.toString();
    }

    private void makeNamesTenantAware(Hashtable environment, String domainName) {
        for (Iterator iter = environment.entrySet().iterator(); iter.hasNext();)
        {
            Map.Entry entry = (Map.Entry) iter.next();

            String key = entry.getKey().toString();
            if (key.startsWith(getDestinationPrefix()) ||
                    key.startsWith(getQueuePrefix()) ||
                    key.startsWith(getTopicPrefix()))
            {
                String physicalName = (String)entry.getValue();
                entry.setValue(domainName + "-" + physicalName);
            }
        }
    }

    /**
        * Set JNDI environment properties
        *
        * @param properties
        *              JNDI properties
        * @param environment
        *              Environment variables
        * @param jndiNamePrefix
        */
    private void setEnvironmentProperties(Properties properties, Hashtable environment, String jndiNamePrefix) {
        Strings.Resolver resolver = new Strings.ChainedResolver
                (Strings.SYSTEM_RESOLVER, new Strings.PropertiesResolver(properties));

        for (Map.Entry me : properties.entrySet())
        {
            String key = new StringBuffer(jndiNamePrefix).append((String)me.getKey()).toString();
            String value = (String)me.getValue();
            String expanded = Strings.expand(value, resolver);
            environment.put(key, expanded);
            if (System.getProperty(key) == null)
            {
                System.setProperty(key, expanded);
            }
        }
    }

    private Properties loadProperties(String propertyFile) throws IOException {
        Properties p1;
        DataInputStream dis = null;
        BufferedReader br = null;
        try {
            dis = new DataInputStream(new FileInputStream(propertyFile));
            br = new BufferedReader(new InputStreamReader(dis));
            p1 = new Properties();

            String eachLine;
            while ((eachLine = br.readLine()) != null) {
                int eqIndex = eachLine.indexOf('=');
                p1.put(eachLine.substring(0, eqIndex), eachLine.substring(eqIndex + 1));
            }
        } finally {
            if (dis != null) {
                dis.close();
            }
            if (br != null) {
                br.close();
            }
        }
        return p1;
    }
}
