/*
* Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.andes.jndi.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.client.AMQBrokerDetails;
import org.wso2.andes.client.AMQConnectionFactory;
import org.wso2.andes.client.AMQDestination;
import org.wso2.andes.client.AMQHeadersExchange;
import org.wso2.andes.client.AMQQueue;
import org.wso2.andes.client.AMQTopic;
import org.wso2.andes.exchange.ExchangeDefaults;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.jms.ConnectionURL;
import org.wso2.andes.jndi.ReadOnlyContext;
import org.wso2.andes.url.BindingURL;
import org.wso2.andes.url.URLSyntaxException;
import org.wso2.andes.util.Strings;
import org.wso2.andes.ws.DynamicDiscoveryWebServices;
import org.wso2.securevault.SecretResolver;
import org.wso2.securevault.SecretResolverFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.ConfigurationException;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DynamicDiscoveryContext class implements InitialContextFactory.
 * <p>
 * This class use for the creating connection factories, creating queues, creating topics
 */
@SuppressWarnings("unused")
public class DynamicDiscoveryContext implements InitialContextFactory {

    protected final Logger logger = LoggerFactory.getLogger(DynamicDiscoveryContext.class);
    private List<String> detailList = new ArrayList<>();
    private String carbonClientId = null;
    private String carbonHostName = null;
    private String[] initialURLs = null;
    private int retries = 0;
    private int connectdelay = 0;
    private int cyclecount = 0;
    private String mode = "defalut";
    private String certificateAliasInTrustsStore = null;
    private String pathToTrustStore = null;
    private String trustStorePassword = null;
    private String pathToKeyStore = null;
    private String keyStorePassword = null;
    private String trustStoreLocation = null;
    private String CONNECTION_FACTORY_PREFIX = "connectionfactory.";
    private String DESTINATION_PREFIX = "destination.";
    private String QUEUE_PREFIX = "queue.";
    private String TOPIC_PREFIX = "topic.";
    private DynamicDiscoveryWebServices dynamicDiscoveryWebServices;
    private float time = 60;
    private AMQConnectionFactory amqConnectionFactory;



    /**
     * Resolve carbon secure vault encrypted properties.
     *
     * @param environment property values which need to construct the InitialContext
     */
    private static void resolveEncryptedProperties(Hashtable environment) {
        if (environment != null) {
            Properties properties = convertToProperties(environment);
            SecretResolver secretResolver = SecretResolverFactory.create(properties);
            for (Object key : environment.keySet()) {
                if (secretResolver != null && secretResolver.isInitialized()) {
                    String value = environment.get(key.toString()).toString();
                    String SECRET_ALIAS_PREFIX = "secretAlias:";
                    if (value != null && value.startsWith(SECRET_ALIAS_PREFIX)) {
                        value = value.split(SECRET_ALIAS_PREFIX)[1];
                    }
                    if (secretResolver.isTokenProtected(value)) {
                        environment.put(key.toString(), secretResolver.resolve(value));
                    }
                }
            }
        }
    }

    /**
     * Convert Map to Properties object.
     *
     * @param map key value pair details
     */
    private static Properties convertToProperties(Map<String, String> map) {
        Properties properties = new Properties();
        for (Map.Entry entry : map.entrySet()) {
            properties.setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
        return properties;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public synchronized Context getInitialContext(Hashtable environment) throws NamingException {
        Map data = new HashMap();

        try {

            String file;
            if (environment.containsKey(Context.PROVIDER_URL)) {
                file = (String) environment.get(Context.PROVIDER_URL);
            } else {
                file = System.getProperty(Context.PROVIDER_URL);
            }

            // Load the properties specified
            if (file != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Loading Properties from:" + file);
                }
                BufferedInputStream inputStream;

                if (file.contains("file:")) {
                    inputStream = new BufferedInputStream(new FileInputStream(new File(new URI(file))));
                } else {
                    inputStream = new BufferedInputStream(new FileInputStream(file));
                }

                Properties properties = new Properties();

                try {
                    properties.load(inputStream);
                } finally {

                    try {
                        inputStream.close();
                    }catch (IOException e){
                        logger.error("Error occurred while closing input stream",e);
                    }
                }

                Strings.Resolver resolver = new Strings.ChainedResolver
                        (Strings.SYSTEM_RESOLVER, new Strings.PropertiesResolver(properties));

                for (Map.Entry me : properties.entrySet()) {
                    String key = (String) me.getKey();
                    String value = (String) me.getValue();
                    String expanded = Strings.expand(value, resolver);
                    environment.put(key, expanded);
                    if (System.getProperty(key) == null) {
                        System.setProperty(key, expanded);
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Loaded Context Properties:" + environment.toString());
                }
            }
        } catch (IOException | URISyntaxException ioe) {
            logger.warn("Unable to load property file specified in Provider_URL:" + environment.get(Context.PROVIDER_URL)
                    + "\n" +
                    "Due to:" + ioe.getMessage());
        }

        try {
            makeURL(environment);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new NamingException();
        }

        autoScale();

        createConnectionFactories(data, environment);

        createDestinations(data, environment);

        createQueues(data, environment);

        createTopics(data, environment);

        return createContext(data, environment);
    }

    /**
     * This method create the context
     *
     * @param data        Context information
     * @param environment property values which need to construct the InitialContext
     * @return ReadOnlyContext
     */
    private ReadOnlyContext createContext(Map data, Hashtable environment) {
        return new ReadOnlyContext(environment, data);
    }

    /**
     * Make the AMQP URL by getting details from web service.
     *
     * @param environment property values which need to construct the InitialContext
     */
    private synchronized void makeURL(Hashtable environment) throws IllegalAccessException, InstantiationException,
            ClassNotFoundException, NamingException {

        for (Object object : environment.entrySet()) {
            Map.Entry entry = (Map.Entry) object;
            String key = entry.getKey().toString();

            //Get the initial URL to call web service
            if (key.startsWith(CONNECTION_FACTORY_PREFIX)) {

                String initialURL = entry.getValue().toString();
                initialURLs = initialURL.split(",");

                //Get the fail over properties
            } else if (key.startsWith(DiscoveryValues.FAILOVER_PROPERTIES)) {

                String names = entry.getValue().toString();

                StringTokenizer stringTokenizer = new StringTokenizer(names, ",");

                while (stringTokenizer.hasMoreElements()) {

                    String failOverProperties = (String) stringTokenizer.nextElement();

                    if (failOverProperties.startsWith("RETRIES")) {
                        retries = Integer.parseInt(failOverProperties.substring(failOverProperties.lastIndexOf("=") + 1));
                    } else if (failOverProperties.startsWith("CONNECTION_DELAY")) {
                        connectdelay = Integer.parseInt(failOverProperties.substring(failOverProperties.lastIndexOf("=") + 1));
                    } else if (failOverProperties.startsWith("CYCLE_COUNT")) {
                        cyclecount = Integer.parseInt(failOverProperties.substring(failOverProperties.lastIndexOf("=") + 1));
                    }
                }

                //Get the SSL certification URI.
            } else if (key.startsWith(DiscoveryValues.TRUSTSTORE)) {

                trustStoreLocation = entry.getValue().toString();


            } else if (key.startsWith(DiscoveryValues.PERIODIC_TIME_PREFIX)) {

                time = Float.parseFloat(entry.getValue().toString());

                //Get the ssl properties.
            } else if (key.startsWith("ssl")) {
                mode = "ssl";
                String names = entry.getValue().toString();
                String[] namesList = names.split(",");

                certificateAliasInTrustsStore = namesList[0];
                pathToTrustStore = namesList[1];
                trustStorePassword = namesList[2];
                pathToKeyStore = namesList[3];
                keyStorePassword = namesList[4];

                //Get Carbon properties.
            } else if (key.startsWith(DiscoveryValues.CARBON_PROPERTIES)) {

                String names = entry.getValue().toString();
                String[] namesList = names.split(",");
                carbonClientId = namesList[0];
                carbonHostName = namesList[1];

                //Get username and password.
            } else if (key.startsWith(DiscoveryValues.AUTHENTICATION_CLIENT)) {

               // byte[] valueDecoded = Base64.decodeBase64((byte[]) entry.getValue());

                StringTokenizer stringTokenizer = new StringTokenizer(entry.getValue().toString(), ",");

                while (stringTokenizer.hasMoreElements()) {
                    detailList.add((String) stringTokenizer.nextElement());
                }

            }

        }
        try {
            Class aClass = Class.forName("org.wso2.andes.ws.DynamicDiscovery");
            dynamicDiscoveryWebServices = (DynamicDiscoveryWebServices) aClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new NamingException();
        }

        //Making AMQP URL
        String amqpURL = getTCPConnectionURL(detailList.get(0), detailList.get(0), carbonClientId, carbonHostName, initialURLs,
                retries, connectdelay, cyclecount);
        environment.put("connectionfactory.qpidConnectionfactory", amqpURL);

       /* environment.put("connectionfactory.qpidConnectionfactory",
                "amqp://admin:admin@carbon/carbon?brokerlist='tcp://10.100.4.165:5673?retries='null'&connectdelay='null';tcp://10.100.4.165:5672?retries='0'&connectdelay='0''&failover='roundrobin?cyclecount='null''");
*/

    }

    /**
     * Periodically calling web service and update broker list.
     */
    private void autoScale() {

        Runnable autoScaleRunnable = new Runnable() {
            public void run() {

                List<String> checkBrokersList;
                List<String> exsistingBrokerList = new ArrayList<>();

                checkBrokersList = dynamicDiscoveryWebServices.getLocalDynamicDiscoveryDetails(initialURLs,
                        detailList.get(0), detailList.get(1), mode, trustStoreLocation);

                try {
                    ConnectionURL amqConnectionURL = amqConnectionFactory.getAmqpConnection();

                    for (int i = 0; i < amqConnectionURL.getAllBrokerDetails().size(); i++) {

                        exsistingBrokerList.add("" + amqConnectionURL.getAllBrokerDetails().get(i).getHost() + ":"
                                + amqConnectionURL.getAllBrokerDetails().get(i).getPort() + "");
                    }

                    ((Collection) checkBrokersList).removeAll((Collection) exsistingBrokerList);

                    for (Object object : ((Collection) checkBrokersList)) {
                        String broker = object.toString();
                        String addBroker = "tcp://" + broker + "?retries='" + retries + "'&connectdelay='"
                                + connectdelay + "'";

                        AMQBrokerDetails brokerDetails = new AMQBrokerDetails(addBroker);
                        amqConnectionURL.addBrokerDetails(brokerDetails);
                    }

                } catch (URLSyntaxException e) {
                    logger.error("URLSyntax error occurred while adding broker details to broker URL", e);
                }
            }
        };

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(0);
        executor.scheduleAtFixedRate(autoScaleRunnable, 1, (long) time, TimeUnit.MINUTES);
    }

    /**
     * Making AMQP URL.
     *
     * @param username                 username
     * @param password                 password
     * @param CARBON_CLIENT_ID         carbon client ID
     * @param CARBON_VIRTUAL_HOST_NAME carbon host name
     * @param URL                      web service calling IP address and port
     * @param retries                  The number of times to retry connection to each broker in the broker list
     * @param connectDelay             Length of time (in milliseconds) to wait before attempting to reconnect
     * @param cycleCount               How many cycles for retries.
     * @return AMQP URL
     */
    private synchronized String getTCPConnectionURL(String username, String password, String CARBON_CLIENT_ID,
                                       String CARBON_VIRTUAL_HOST_NAME, String[] URL, int retries,
                                       int connectDelay, int cycleCount) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String ip = null;
        StringBuilder output = new StringBuilder();
        String failOver_Config = "retries='" + retries + "'&connectDelay='" + connectDelay + "'";

        String ssl_Config = "?ssl='true'&ssl_cert_alias='" + certificateAliasInTrustsStore + "'&trust_store='"
                + pathToTrustStore + "'&trust_store_password='" + trustStorePassword + "'&key_store='"
                + pathToKeyStore + "'&key_store_password='" + keyStorePassword + "'";

        List<String> wsResult = dynamicDiscoveryWebServices.getLocalDynamicDiscoveryDetails(URL, username, password, mode,
                trustStoreLocation);

        if (!wsResult.isEmpty()) {
            ip = wsResult.get(0);
            for (int i = 0; i < wsResult.size(); ) {
                if (wsResult.get(i).equals(ip)) {
                    i++;
                } else {
                    if (mode.equals("ssl")) {
                        output.append(";tcp://").append(wsResult.get(i)).append(ssl_Config).append("&").append(failOver_Config);
                        i++;
                    } else {
                        output.append(";tcp://").append(wsResult.get(i)).append("?").append(failOver_Config);
                        i++;
                    }
                }
            }
        }

        if (mode.equals("ssl")) {
            // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
            return "amqp://" + username + ":" + password
                    + "@" + CARBON_CLIENT_ID
                    + "/" + CARBON_VIRTUAL_HOST_NAME + "?failover='roundrobin'&cycleCount='"
                    + cycleCount + "'"
                    + "&brokerlist='tcp://" + ip + ssl_Config
                    + "&" + failOver_Config + output + "'";

        } else {
            // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
            return "amqp://" + username + ":" + password
                    + "@" + CARBON_CLIENT_ID
                    + "/" + CARBON_VIRTUAL_HOST_NAME + "?failover='roundrobin'&cycleCount='"
                    + cycleCount + "'"
                    + "&brokerlist='tcp://" + ip + "?" + failOver_Config
                    + output + "'";
        }
    }

    /**
     * Create connection factory using given environment variables
     *
     * @param data        connection factory information to the relevant jndiname
     * @param environment property values which need to construct the InitialContext
     */
    private synchronized void createConnectionFactories(Map data, Hashtable environment) throws ConfigurationException {
        resolveEncryptedProperties(environment);

        for (Object object : environment.entrySet()) {
            Map.Entry entry = (Map.Entry) object;
            String key = entry.getKey().toString();
            if (key.startsWith(CONNECTION_FACTORY_PREFIX)) {
                String jndiName = key.substring(CONNECTION_FACTORY_PREFIX.length());
                ConnectionFactory connectionFactory = createFactory(entry.getValue().toString().trim());
                if (connectionFactory != null) {
                    data.put(jndiName, connectionFactory);
                }
            }
        }
    }

    /**
     * Create destinations using given environment variables
     *
     * @param data        designation information to the relevant jndiname
     * @param environment property values which need to construct the InitialContext
     * @throws ConfigurationException
     */
    private void createDestinations(Map data, Hashtable environment) throws ConfigurationException {
        for (Object object : environment.entrySet()) {
            Map.Entry entry = (Map.Entry) object;
            String key = entry.getKey().toString();
            if (key.startsWith(DESTINATION_PREFIX)) {
                String jndiName = key.substring(DESTINATION_PREFIX.length());
                Destination destination = createDestination(entry.getValue().toString().trim());
                if (destination != null) {
                    data.put(jndiName, destination);
                }
            }
        }
    }

    /**
     * Queue is created using this method.
     *
     * @param data        Queue information to the relevant jndiname
     * @param environment property values which need to construct the InitialContext
     */
    private void createQueues(Map data, Hashtable environment) {
        for (Object object : environment.entrySet()) {
            Map.Entry entry = (Map.Entry) object;
            String key = entry.getKey().toString();
            if (key.startsWith(QUEUE_PREFIX)) {
                String jndiName = key.substring(QUEUE_PREFIX.length());
                Queue q = createQueue(entry.getValue().toString().trim());
                if (q != null) {
                    data.put(jndiName, q);
                }
            }
        }
    }

    /**
     * Topic is created in this method.
     *
     * @param data        Topic information to the relevant jndiname
     * @param environment property values which need to construct the InitialContext
     */
    private void createTopics(Map data, Hashtable environment) {
        for (Object object : environment.entrySet()) {
            Map.Entry entry = (Map.Entry) object;
            String key = entry.getKey().toString();
            if (key.startsWith(TOPIC_PREFIX)) {
                String jndiName = key.substring(TOPIC_PREFIX.length());
                Topic topic = createTopic(entry.getValue().toString().trim());
                if (topic != null) {
                    if (logger.isDebugEnabled()) {
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("Creating the topic: ").append(jndiName).append(" with the following binding keys ");
                        for (AMQShortString binding : ((AMQTopic) topic).getBindingKeys()) {
                            stringBuilder.append(binding.toString()).append(",");
                        }

                        logger.debug(stringBuilder.toString());
                    }
                    data.put(jndiName, topic);
                }
            }
        }
    }

    /**
     * Factory method to create new Connection Factory instances
     *
     * @param url AMQP URL
     * @return AMQ connection factory
     * @throws ConfigurationException
     */
    private synchronized ConnectionFactory createFactory(String url) throws ConfigurationException {
        try {

            amqConnectionFactory = new AMQConnectionFactory(url);
            return amqConnectionFactory;
        } catch (URLSyntaxException urlse) {
            logger.warn("Unable to create factory:" + urlse);

            ConfigurationException configurationException = new ConfigurationException("Failed to parse entry: " + urlse + " due to : " + urlse.getMessage());
            configurationException.initCause(urlse);
            throw configurationException;
        }
    }

    /**
     * Factory method to create new Destination instances from an AMQP BindingURL
     */
    private Destination createDestination(String str) throws ConfigurationException {
        try {
            return AMQDestination.createDestination(str);
        } catch (Exception e) {
            logger.warn("Unable to create destination:", e);

            ConfigurationException configurationException = new ConfigurationException("Failed to parse entry: " + str + " due to : " + e.getMessage());
            configurationException.initCause(e);
            throw configurationException;
        }
    }

    /**
     * Factory method to create new Queue instances
     */
    protected Queue createQueue(Object value) {
        if (value instanceof AMQShortString) {
            return new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_NAME, (AMQShortString) value);
        } else if (value instanceof String) {
            return new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_NAME, new AMQShortString((String) value));
        } else if (value instanceof BindingURL) {
            return new AMQQueue((BindingURL) value);
        }

        return null;
    }

    /**
     * Factory method to create new Topic instances
     */
    protected Topic createTopic(Object value) {
        if (value instanceof AMQShortString) {
            return new AMQTopic(ExchangeDefaults.TOPIC_EXCHANGE_NAME, (AMQShortString) value);
        } else if (value instanceof String) {
            String[] keys = ((String) value).split(",");
            AMQShortString[] bindings = new AMQShortString[keys.length];
            int i = 0;
            for (String key : keys) {
                bindings[i] = new AMQShortString(key.trim());
                i++;
            }
            // The Destination has a dual nature. If this was used for a producer the key is used
            // for the routing key. If it was used for the consumer it becomes the bindingKey
            return new AMQTopic(ExchangeDefaults.TOPIC_EXCHANGE_NAME, bindings[0], null, bindings);
        } else if (value instanceof BindingURL) {
            return new AMQTopic((BindingURL) value);
        }

        return null;
    }

    /**
     * Factory method to create new HeaderExchange instances
     */
    protected Destination createHeaderExchange(Object value) {
        if (value instanceof String) {
            return new AMQHeadersExchange((String) value);
        } else if (value instanceof BindingURL) {
            return new AMQHeadersExchange((BindingURL) value);
        }

        return null;
    }


    // Properties
    @SuppressWarnings("unused")
    public String getConnectionPrefix() {
        return CONNECTION_FACTORY_PREFIX;
    }

    @SuppressWarnings("unused")
    public void setConnectionPrefix(String connectionPrefix) {
        this.CONNECTION_FACTORY_PREFIX = connectionPrefix;
    }

    @SuppressWarnings("unused")
    public String getDestinationPrefix() {
        return DESTINATION_PREFIX;
    }

    @SuppressWarnings("unused")
    public void setDestinationPrefix(String destinationPrefix) {
        this.DESTINATION_PREFIX = destinationPrefix;
    }

    @SuppressWarnings("unused")
    public String getQueuePrefix() {
        return QUEUE_PREFIX;
    }

    @SuppressWarnings("unused")
    public void setQueuePrefix(String queuePrefix) {
        this.QUEUE_PREFIX = queuePrefix;
    }

    @SuppressWarnings("unused")
    public String getTopicPrefix() {
        return TOPIC_PREFIX;
    }

    @SuppressWarnings("unused")
    public void setTopicPrefix(String topicPrefix) {
        this.TOPIC_PREFIX = topicPrefix;
    }
}
