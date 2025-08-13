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
package org.wso2.andes.server.virtualhost;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQStoreException;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.configuration.qpid.*;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.AMQBrokerManagerMBean;
import org.wso2.andes.server.binding.BindingFactory;
import org.wso2.andes.server.connection.ConnectionRegistry;
import org.wso2.andes.server.connection.IConnectionRegistry;
import org.wso2.andes.server.exchange.*;
import org.wso2.andes.server.federation.BrokerLink;
import org.wso2.andes.server.information.management.QueueManagementInformationMBean;
import org.wso2.andes.server.logging.LogSubject;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.messages.VirtualHostMessages;
import org.wso2.andes.server.logging.subjects.MessageStoreLogSubject;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.management.ManagedObject;
import org.wso2.andes.server.protocol.AMQConnectionModel;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.AMQQueueFactory;
import org.wso2.andes.server.queue.DefaultQueueRegistry;
import org.wso2.andes.server.queue.QueueRegistry;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.registry.IApplicationRegistry;
import org.wso2.andes.server.security.SecurityManager;
import org.wso2.andes.server.security.auth.manager.AuthenticationManager;
import org.wso2.andes.server.stats.StatisticsCounter;
import org.wso2.andes.server.store.*;
import org.wso2.andes.server.store.MessageStore;
import org.wso2.andes.server.virtualhost.plugins.VirtualHostPlugin;
import org.wso2.andes.server.virtualhost.plugins.VirtualHostPluginFactory;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class VirtualHostImpl implements VirtualHost {
    private static final Log log = LogFactory.getLog(VirtualHostImpl.class);

    private final String name;

    private ConnectionRegistry connectionRegistry;

    private QueueRegistry queueRegistry;

    private ExchangeRegistry exchangeRegistry;

    private ExchangeFactory exchangeFactory;

    private MessageStore messageStore;

    protected VirtualHostMBean virtualHostMBean;

    private AMQBrokerManagerMBean brokerMBean;

    private QueueManagementInformationMBean queueManagementInformationMBean;

    private final AuthenticationManager authenticationManager;

    private SecurityManager securityManager;

    private final ScheduledThreadPoolExecutor houseKeepingTasks;
    private final IApplicationRegistry appRegistry;
    private VirtualHostConfiguration configuration;
    private DurableConfigurationStore durableConfigurationStore;
    private BindingFactory bindingFactory;
    private BrokerConfig broker;
    private UUID id;

    private boolean statisticsEnabled = false;
    private StatisticsCounter messagesDelivered, dataDelivered, messagesReceived, dataReceived;

    private final long createTime = System.currentTimeMillis();
    private final ConcurrentHashMap<BrokerLink, BrokerLink> links = new ConcurrentHashMap<BrokerLink, BrokerLink>();
    private static final int HOUSEKEEPING_SHUTDOWN_TIMEOUT = 5;

    public IConnectionRegistry getConnectionRegistry() {
        return connectionRegistry;
    }

    public VirtualHostConfiguration getConfiguration() {
        return configuration;
    }

    public UUID getId() {
        return id;
    }

    public VirtualHostConfigType getConfigType() {
        return VirtualHostConfigType.getInstance();
    }

    public ConfiguredObject getParent() {
        return getBroker();
    }

    public boolean isDurable() {
        return false;
    }

    /**
     * Virtual host JMX MBean class.
     * <p/>
     * This has some of the methods implemented from management intrerface for exchanges. Any
     * implementaion of an Exchange MBean should extend this class.
     */
    public class VirtualHostMBean extends AMQManagedObject implements ManagedVirtualHost {
        public VirtualHostMBean() throws NotCompliantMBeanException {
            super(ManagedVirtualHost.class, ManagedVirtualHost.TYPE);
        }

        public String getObjectInstanceName() {
            return ObjectName.quote(name);
        }

        public String getName() {
            return name;
        }

        public VirtualHostImpl getVirtualHost() {
            return VirtualHostImpl.this;
        }
    }

    public VirtualHostImpl(IApplicationRegistry appRegistry, VirtualHostConfiguration hostConfig) throws Exception {
        this(appRegistry, hostConfig, null);
    }


    public VirtualHostImpl(VirtualHostConfiguration hostConfig, MessageStore store) throws Exception {
        this(ApplicationRegistry.getInstance(), hostConfig, store);
    }

    private VirtualHostImpl(IApplicationRegistry appRegistry, VirtualHostConfiguration hostConfig, MessageStore store) throws Exception {
        if (hostConfig == null) {
            throw new IllegalAccessException("HostConfig and MessageStore cannot be null");
        }

        this.appRegistry = appRegistry;
        broker = this.appRegistry.getBroker();
        configuration = hostConfig;
        name = configuration.getName();

        id = this.appRegistry.getConfigStore().createId();

        CurrentActor.get().message(VirtualHostMessages.CREATED(name));

        if (name == null || name.length() == 0) {
            throw new IllegalArgumentException("Illegal name (" + name + ") for virtualhost.");
        }

        securityManager = new SecurityManager(this.appRegistry.getSecurityManager());
        securityManager.configureHostPlugins(configuration);

        virtualHostMBean = new VirtualHostMBean();

        connectionRegistry = new ConnectionRegistry();

        houseKeepingTasks = new ScheduledThreadPoolExecutor(configuration.getHouseKeepingThreadCount());

        queueRegistry = new DefaultQueueRegistry(this);

        exchangeFactory = new DefaultExchangeFactory(this);
        exchangeFactory.initialise(configuration);

        StartupRoutingTable configFileRT = new StartupRoutingTable();

        durableConfigurationStore = configFileRT;

        if (store != null) {
            messageStore = store;
            durableConfigurationStore = store;
        } else {
            initialiseAndesStores(hostConfig);
        }

        AndesKernelBoot.startAndesCluster();
        exchangeRegistry = new DefaultExchangeRegistry(this);
        bindingFactory = new BindingFactory(this);


        // This needs to be after the RT has been defined as it creates the default durable exchanges.
        initialiseModel(configuration);
        exchangeRegistry.initialise();

        authenticationManager = ApplicationRegistry.getInstance().getAuthenticationManager();

        brokerMBean = new AMQBrokerManagerMBean(virtualHostMBean);
        brokerMBean.register();

        queueManagementInformationMBean = new QueueManagementInformationMBean(virtualHostMBean);
        queueManagementInformationMBean.register();

        initialiseHouseKeeping(hostConfig.getHousekeepingExpiredMessageCheckPeriod());

        initialiseStatistics();

    }

    private void initialiseHouseKeeping(long period) {
        /* add a timer task to iterate over queues, cleaning expired messages from queues with no consumers */
        if (period != 0L) {
            class ExpiredMessagesTask extends HouseKeepingTask {
                public ExpiredMessagesTask(VirtualHost vhost) {
                    super(vhost);
                }

                public void execute() {
                    for (AMQQueue q : queueRegistry.getQueues()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Checking message status for queue: " + q.getName());
                        }
                        try {
                            q.checkMessageStatus();
                        } catch (Exception e) {
                            log.error("Exception in housekeeping for queue: "
                                    + q.getNameShortString().toString(), e);
                            //Don't throw exceptions as this will stop the
                            // house keeping task from running.
                        }
                    }
                    for (AMQConnectionModel connection : getConnectionRegistry().getConnections()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Checking for long running open transactions on connection " + connection);
                        }
                        for (AMQSessionModel session : connection.getSessionModels()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Checking for long running open transactions on session " + session);
                            }
                            try {
                                session.checkTransactionStatus(configuration.getTransactionTimeoutOpenWarn(),
                                        configuration.getTransactionTimeoutOpenClose(),
                                        configuration.getTransactionTimeoutIdleWarn(),
                                        configuration.getTransactionTimeoutIdleClose());
                            } catch (Exception e) {
                                log.error("Exception in housekeeping for connection: " + connection.toString(), e);
                            }
                        }
                    }
                }
            }

            scheduleHouseKeepingTask(period, new ExpiredMessagesTask(this));

            Map<String, VirtualHostPluginFactory> plugins =
                    ApplicationRegistry.getInstance().getPluginManager().getVirtualHostPlugins();

            if (plugins != null) {
                for (Map.Entry<String, VirtualHostPluginFactory> entry : plugins.entrySet()) {
                    String pluginName = entry.getKey();
                    VirtualHostPluginFactory factory = entry.getValue();
                    try {
                        VirtualHostPlugin plugin = factory.newInstance(this);

                        // If we had configuration for the plugin the schedule it.
                        if (plugin != null) {
                            houseKeepingTasks.scheduleAtFixedRate(plugin, plugin.getDelay() / 2,
                                    plugin.getDelay(), plugin.getTimeUnit());

                            log.info("Loaded VirtualHostPlugin:" + plugin);
                        }
                    } catch (RuntimeException e) {
                        log.error("Unable to load VirtualHostPlugin:" + pluginName + " due to:" + e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * Allow other broker components to register a HouseKeepingTask
     *
     * @param period How often this task should run, in ms.
     * @param task   The task to run.
     */
    public void scheduleHouseKeepingTask(long period, HouseKeepingTask task) {
        houseKeepingTasks.scheduleAtFixedRate(task, period / 2, period,
                TimeUnit.MILLISECONDS);
    }

    public long getHouseKeepingTaskCount() {
        return houseKeepingTasks.getTaskCount();
    }

    public long getHouseKeepingCompletedTaskCount() {
        return houseKeepingTasks.getCompletedTaskCount();
    }

    public int getHouseKeepingPoolSize() {
        return houseKeepingTasks.getCorePoolSize();
    }

    public void setHouseKeepingPoolSize(int newSize) {
        houseKeepingTasks.setCorePoolSize(newSize);
    }


    public int getHouseKeepingActiveCount() {
        return houseKeepingTasks.getActiveCount();
    }


    /**
     * Initialize the message store
     *
     * @param hostConfig VirtualHost Configuration
     * @throws Exception
     */
    private void initialiseAndesStores(VirtualHostConfiguration hostConfig) throws Exception {

        //Set virtual host
        AndesKernelBoot.initVirtualHostConfigSynchronizer(this);

        //kernel will start message stores for Andes
        AndesKernelBoot.startAndesStores();

        // this is considered as an internal impl now, so hard coding
        // qpid related messagestore
        MessageStore messageStore = new QpidDeprecatedMessageStore();
        VirtualHostConfigRecoveryHandler recoveryHandler = new VirtualHostConfigRecoveryHandler(this);

        MessageStoreLogSubject storeLogSubject = new MessageStoreLogSubject(this, messageStore);

        messageStore.configureConfigStore(this.getName(),
                recoveryHandler,
                hostConfig.getStoreConfiguration(),
                storeLogSubject);

        messageStore.configureMessageStore(this.getName(),
                recoveryHandler,
                hostConfig.getStoreConfiguration(),
                storeLogSubject);

        messageStore.configureTransactionLog(this.getName(),
                recoveryHandler,
                hostConfig.getStoreConfiguration(),
                storeLogSubject);

        this.messageStore = messageStore;
        durableConfigurationStore = messageStore;

    }

    private void initialiseModel(VirtualHostConfiguration config) throws ConfigurationException, AMQException {
        if (log.isDebugEnabled()) {
            log.debug("Loading configuration for virtualhost: " + config.getName());
        }

        List exchangeNames = config.getExchanges();

        for (Object exchangeNameObj : exchangeNames) {
            String exchangeName = String.valueOf(exchangeNameObj);
            configureExchange(config.getExchangeConfiguration(exchangeName));
        }

        String[] queueNames = config.getQueueNames();

        for (Object queueNameObj : queueNames) {
            String queueName = String.valueOf(queueNameObj);
            configureQueue(config.getQueueConfiguration(queueName));
        }
    }

    private void configureExchange(ExchangeConfiguration exchangeConfiguration) throws AMQException {
        AMQShortString exchangeName = new AMQShortString(exchangeConfiguration.getName());

        Exchange exchange;
        exchange = exchangeRegistry.getExchange(exchangeName);
        if (exchange == null) {

            AMQShortString type = new AMQShortString(exchangeConfiguration.getType());
            boolean durable = exchangeConfiguration.getDurable();
            boolean autodelete = exchangeConfiguration.getAutoDelete();

            Exchange newExchange = exchangeFactory.createExchange(exchangeName, type, durable, autodelete, 0);
            exchangeRegistry.registerExchange(newExchange);

            if (newExchange.isDurable()) {
                durableConfigurationStore.createExchange(newExchange);

                //tell Andes kernel to create Exchange
                QpidAndesBridge.createExchange(newExchange);
            }
        }
    }

    private void configureQueue(QueueConfiguration queueConfiguration) throws AMQException, ConfigurationException {
        AMQQueue queue = AMQQueueFactory.createAMQQueueImpl(queueConfiguration, this);

        if (queue.isDurable()) {
            getDurableConfigurationStore().createQueue(queue);
        }

        //tell Andes kernel to create queue
        QpidAndesBridge.createQueue(queue);

        String exchangeName = queueConfiguration.getExchange();

        Exchange exchange = exchangeRegistry.getExchange(exchangeName == null ? null : new AMQShortString(exchangeName));

        if (exchange == null) {
            exchange = exchangeRegistry.getDefaultExchange();
        }

        if (exchange == null) {
            throw new ConfigurationException("Attempt to bind queue to unknown exchange:" + exchangeName);
        }

        List routingKeys = queueConfiguration.getRoutingKeys();
        if (routingKeys == null || routingKeys.isEmpty()) {
            routingKeys = Collections.singletonList(queue.getNameShortString());
        }

        for (Object routingKeyNameObj : routingKeys) {
            AMQShortString routingKey = new AMQShortString(String.valueOf(routingKeyNameObj));
            if (log.isInfoEnabled()) {
                log.info("Binding queue:" + queue + " with routing key '" + routingKey + "' to exchange:" + this);
            }
            bindingFactory.addBinding(routingKey.toString(), queue, exchange, null);
        }

        if (exchange != exchangeRegistry.getDefaultExchange()) {
            bindingFactory.addBinding(queue.getNameShortString().toString(), queue, exchange, null);
        }
    }

    public String getName() {
        return name;
    }

    public BrokerConfig getBroker() {
        return broker;
    }

    public String getFederationTag() {
        return broker.getFederationTag();
    }

    public void setBroker(final BrokerConfig broker) {
        this.broker = broker;
    }

    public long getCreateTime() {
        return createTime;
    }

    public QueueRegistry getQueueRegistry() {
        return queueRegistry;
    }

    public ExchangeRegistry getExchangeRegistry() {
        return exchangeRegistry;
    }

    public ExchangeFactory getExchangeFactory() {
        return exchangeFactory;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public TransactionLog getTransactionLog() {
        return messageStore;
    }

    public DurableConfigurationStore getDurableConfigurationStore() {
        return durableConfigurationStore;
    }

    public AuthenticationManager getAuthenticationManager() {
        return authenticationManager;
    }

    public SecurityManager getSecurityManager() {
        return securityManager;
    }

    public void close() {
        //Stop Connections
        connectionRegistry.close();

        //Stop the Queues processing
        if (queueRegistry != null) {
            for (AMQQueue queue : queueRegistry.getQueues()) {
                queue.stop();
            }
        }

        //Stop Housekeeping
        if (houseKeepingTasks != null) {
            houseKeepingTasks.shutdown();

            try {
                if (!houseKeepingTasks.awaitTermination(HOUSEKEEPING_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
                    houseKeepingTasks.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted during Housekeeping shutdown:" + e.getMessage());
                // Swallowing InterruptedException ok as we are shutting down.
            }
        }

        //Close MessageStore
        if (messageStore != null) {
            //Remove MessageStore Interface should not throw Exception
            try {
                messageStore.close();
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        CurrentActor.get().message(VirtualHostMessages.CLOSED());
    }

    public ManagedObject getBrokerMBean() {
        return brokerMBean;
    }

    public ManagedObject getManagedObject() {
        return virtualHostMBean;
    }

    public UUID getBrokerId() {
        return appRegistry.getBrokerId();
    }

    public IApplicationRegistry getApplicationRegistry() {
        return appRegistry;
    }

    public BindingFactory getBindingFactory() {
        return bindingFactory;
    }

    public void registerMessageDelivered(long messageSize) {
        if (isStatisticsEnabled()) {
            messagesDelivered.registerEvent(1L);
            dataDelivered.registerEvent(messageSize);
        }
        appRegistry.registerMessageDelivered(messageSize);
    }

    public void registerMessageReceived(long messageSize, long timestamp) {
        if (isStatisticsEnabled()) {
            messagesReceived.registerEvent(1L, timestamp);
            dataReceived.registerEvent(messageSize, timestamp);
        }
        appRegistry.registerMessageReceived(messageSize, timestamp);
    }

    public StatisticsCounter getMessageReceiptStatistics() {
        return messagesReceived;
    }

    public StatisticsCounter getDataReceiptStatistics() {
        return dataReceived;
    }

    public StatisticsCounter getMessageDeliveryStatistics() {
        return messagesDelivered;
    }

    public StatisticsCounter getDataDeliveryStatistics() {
        return dataDelivered;
    }

    public void resetStatistics() {
        messagesDelivered.reset();
        dataDelivered.reset();
        messagesReceived.reset();
        dataReceived.reset();

        for (AMQConnectionModel connection : connectionRegistry.getConnections()) {
            connection.resetStatistics();
        }
    }

    public void initialiseStatistics() {
        setStatisticsEnabled(!StatisticsCounter.DISABLE_STATISTICS &&
                appRegistry.getConfiguration().isStatisticsGenerationVirtualhostsEnabled());

        messagesDelivered = new StatisticsCounter("messages-delivered-" + getName());
        dataDelivered = new StatisticsCounter("bytes-delivered-" + getName());
        messagesReceived = new StatisticsCounter("messages-received-" + getName());
        dataReceived = new StatisticsCounter("bytes-received-" + getName());
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void setStatisticsEnabled(boolean enabled) {
        statisticsEnabled = enabled;
    }

    public void createBrokerConnection(final String transport,
                                       final String host,
                                       final int port,
                                       final String vhost,
                                       final boolean durable,
                                       final String authMechanism,
                                       final String username,
                                       final String password) {
        BrokerLink blink = new BrokerLink(this, transport, host, port, vhost, durable, authMechanism, username, password);
        if (links.putIfAbsent(blink, blink) != null) {
            getConfigStore().addConfiguredObject(blink);
        }
    }

    public void removeBrokerConnection(final String transport,
                                       final String host,
                                       final int port,
                                       final String vhost) {
        removeBrokerConnection(new BrokerLink(this, transport, host, port, vhost, false, null, null, null));

    }

    public void removeBrokerConnection(BrokerLink blink) {
        blink = links.get(blink);
        if (blink != null) {
            blink.close();
            getConfigStore().removeConfiguredObject(blink);
        }
    }

    public ConfigStore getConfigStore() {
        return getApplicationRegistry().getConfigStore();
    }

    /**
     * Temporary Startup RT class to record the creation of persistent queues / exchanges.
     * <p/>
     * <p/>
     * This is so we can replay the creation of queues/exchanges in to the real _RT after it has been loaded.
     * This should be removed after the _RT has been fully split from the the TL
     */
    private static class StartupRoutingTable implements DurableConfigurationStore {
        public List<Exchange> exchange = new LinkedList<Exchange>();
        public List<CreateQueueTuple> queue = new LinkedList<CreateQueueTuple>();
        public List<CreateBindingTuple> bindings = new LinkedList<CreateBindingTuple>();

        public void configure(VirtualHost virtualHost, String base, VirtualHostConfiguration config) throws Exception {
        }

        public void close() throws Exception {
        }

        public void removeMessage(Long messageId) throws AMQException {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public void configureConfigStore(String name,
                                         ConfigurationRecoveryHandler recoveryHandler,
                                         Configuration config,
                                         LogSubject logSubject) throws Exception {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public void createExchange(Exchange exchange) throws AMQStoreException {
            if (exchange.isDurable()) {
                this.exchange.add(exchange);
            }
        }

        public void removeExchange(Exchange exchange) throws AMQStoreException {
        }

        public void bindQueue(Exchange exchange, AMQShortString routingKey, AMQQueue queue, FieldTable args) throws AMQStoreException {
            if (exchange.isDurable() && queue.isDurable()) {
                bindings.add(new CreateBindingTuple(exchange, routingKey, queue, args));
            }
        }

        public void unbindQueue(Exchange exchange, AMQShortString routingKey, AMQQueue queue, FieldTable args) throws AMQStoreException {
        }

        public void createQueue(AMQQueue queue) throws AMQStoreException {
            createQueue(queue, null);

        }

        public void createQueue(AMQQueue queue, FieldTable arguments) throws AMQStoreException {
            if (queue.isDurable()) {
                this.queue.add(new CreateQueueTuple(queue, arguments));
            }
        }

        public void removeQueue(AMQQueue queue) throws AMQStoreException {
        }


        private static class CreateQueueTuple {
            public AMQQueue queue;
            public FieldTable arguments;

            public CreateQueueTuple(AMQQueue queue, FieldTable arguments) {
                this.queue = queue;
                this.arguments = arguments;
            }
        }

        private static class CreateBindingTuple {
            public AMQQueue queue;
            public FieldTable arguments;
            public Exchange exchange;
            public AMQShortString routingKey;

            public CreateBindingTuple(Exchange exchange, AMQShortString routingKey, AMQQueue queue, FieldTable args) {
                this.exchange = exchange;
                this.routingKey = routingKey;
                this.queue = queue;
                arguments = args;
            }
        }

        public void updateQueue(AMQQueue queue) throws AMQStoreException {
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
