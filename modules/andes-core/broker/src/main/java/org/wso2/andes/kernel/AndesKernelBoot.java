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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.StoreConfiguration;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.disruptor.inbound.InboundExchangeEvent;
import org.wso2.andes.kernel.dtx.DtxRegistry;
import org.wso2.andes.kernel.registry.MessageRouterRegistry;
import org.wso2.andes.kernel.registry.StorageQueueRegistry;
import org.wso2.andes.kernel.registry.SubscriptionRegistry;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.ClusterManagementInformationMBean;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationListenerManager;
import org.wso2.andes.server.cluster.coordination.CoordinationComponentFactory;
import org.wso2.andes.server.connection.IConnectionRegistry;
import org.wso2.andes.server.information.management.MessageStatusInformationMBean;
import org.wso2.andes.server.information.management.SubscriptionManagementInformationMBean;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostConfigSynchronizer;
import org.wso2.andes.store.FailureObservingAndesContextStore;
import org.wso2.andes.store.FailureObservingMessageStore;
import org.wso2.andes.store.FailureObservingStoreManager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;

/**
 * Andes kernel startup/shutdown related work is done through this class.
 */
public class AndesKernelBoot {

    private static Log log = LogFactory.getLog(AndesKernelBoot.class);

    /**
     * Store for keeping messages (i.e persistent store)
     */
    private static MessageStore messageStore;

    /**
     * Store for keeping AMQP based artifacts
     */
    private static AMQPConstructStore amqpConstructStore;

    /**
     * In-memory store for keeping subscription entries
     */
    private static SubscriptionRegistry subscriptionRegistry;

    /**
     * Scheduled thread pool executor to run periodic andes recovery task
     */
    private static ScheduledExecutorService andesRecoveryTaskScheduler;

    /**
     * Scheduled thread pool executor to run periodic expiry message deletion task
     */
    private static ScheduledExecutorService expiryMessageDeletionTaskScheduler;

    /**
     * Used to get information from context store
     */
    private static AndesContextStore contextStore;

    /**
     * This is used by independent worker threads to identify if the kernel is performing shutdown operations.
     */
    private static boolean isKernelShuttingDown = false;

    /**
     * Used to initialize cluster notifications listners.
     */
    private static ClusterNotificationListenerManager clusterNotificationListenerManager;

    /**
     * Used to close connections when broker becomes passive.
     */
    private static IConnectionRegistry amqpConnectionRegistry;

    /**
     * This will boot up all the components in Andes kernel and bring the server to working state
     */
    public static void initializeComponents() throws AndesException {
        isKernelShuttingDown = false;
        //loadConfigurations - done from outside
        //startAndesStores - done from outside
        int threadPoolCount = 1;
        andesRecoveryTaskScheduler = Executors.newScheduledThreadPool(threadPoolCount);
        expiryMessageDeletionTaskScheduler = Executors.newScheduledThreadPool(threadPoolCount);
        startHouseKeepingThreads();
        createDefinedProtocolArtifacts();
        syncNodeWithClusterState();
        registerMBeans();
    }

    /**
     * Initialize the VirtualHostConfigSynchronizaer based on the provide virtual host. Andes operates on this virtual
     * host only
     *
     * @param defaultVirtualHost virtual host to set
     */
    public static void initVirtualHostConfigSynchronizer(VirtualHost defaultVirtualHost) {
        amqpConnectionRegistry = defaultVirtualHost.getConnectionRegistry();
        // initialize amqp constructs syncing into Qpid
        VirtualHostConfigSynchronizer _VirtualHostConfigSynchronizer = new VirtualHostConfigSynchronizer
                (defaultVirtualHost);
        ClusterResourceHolder.getInstance().setVirtualHostConfigSynchronizer(_VirtualHostConfigSynchronizer);
    }

    /**
     * Forcefully close all existing AMQP connections
     */
    public static void closeAllConnections() {
        if (amqpConnectionRegistry != null) {
            amqpConnectionRegistry.close();
        } else {
            log.warn("Cannot close existing connections since the Connection registry is not available");
        }
    }

    /**
     * This will trigger graceful shutdown of andes broker
     *
     * @throws AndesException
     */
    public static void shutDownAndesKernel() throws AndesException {

        // Set flag so independent threads can act accordingly
        isKernelShuttingDown = true;

        // Trigger Shutdown Event
        Andes.getInstance().shutDown();

    }

    
    /**
     * A factory method (/util) to create user specified
     * {@link AndesContextStore} in broker.xml
     * 
     * @return an implementation of {@link AndesContextStore}
     * @throws Exception if an error occures
     */
    private static AndesContextStore createAndesContextStoreFromConfig() throws Exception {
        StoreConfiguration andesConfiguration = AndesContext.getInstance()
                .getStoreConfiguration();
        //create a andes context store and register
        String contextStoreClassName = andesConfiguration.getAndesContextStoreClassName();
        Class<? extends AndesContextStore> contextStoreClass = Class.forName(contextStoreClassName).asSubclass(AndesContextStore.class);
        AndesContextStore contextStoreInstance = contextStoreClass.newInstance();
        
        contextStoreInstance.init(andesConfiguration.getContextStoreProperties());
        log.info("AndesContextStore initialised with " + contextStoreClassName);
        
        return contextStoreInstance;
    }
    
    /**
     * A factory method (/util) to create user specified
     * {@link MessageStore} in broker.xml
     * 
     * @return an implementation of {@link MessageStore}
     * @throws Exception if an error occurs
     */
    private static MessageStore createMessageStoreFromConfig(AndesContextStore andesContextStore,
                                                             FailureObservingStoreManager failureObservingStoreManager)
            throws Exception {

        StoreConfiguration andesConfiguration = AndesContext.getInstance().getStoreConfiguration();
        
     // create a message store and initialise messaging engine
        String messageStoreClassName = andesConfiguration.getMessageStoreClassName();
        Class<? extends MessageStore> messageStoreClass =
                Class.forName(messageStoreClassName).asSubclass(MessageStore.class);

        MessageStore messageStoreInConfig = messageStoreClass.newInstance();

        FailureObservingMessageStore failureObservingMessageStore = new FailureObservingMessageStore(
                messageStoreInConfig, failureObservingStoreManager);

        failureObservingMessageStore.initializeMessageStore(
                andesContextStore, andesConfiguration.getMessageStoreProperties());
        
        log.info("Andes MessageStore initialised with " + messageStoreClassName);
        return failureObservingMessageStore;
    }


    /**
     * Start all andes stores message store/context store and AMQP construct store
     *
     * @throws Exception
     */
    public static void startAndesStores() throws Exception {

        //Create a andes context store and register
        AndesContextStore contextStoreInConfig = createAndesContextStoreFromConfig();
        FailureObservingStoreManager failureObservingStoreManager = new FailureObservingStoreManager();
        AndesKernelBoot.contextStore = new FailureObservingAndesContextStore(contextStoreInConfig,
                failureObservingStoreManager);
        AndesContext.getInstance().setAndesContextStore(contextStore);

        // directly wire the instance without wrapped instance
        messageStore = createMessageStoreFromConfig(contextStoreInConfig, failureObservingStoreManager);

        // Setting the message store in the context store
        AndesContext.getInstance().setMessageStore(messageStore);

        //create AMQP Constructs store
        amqpConstructStore = new AMQPConstructStore(contextStore);
        AndesContext.getInstance().setAMQPConstructStore(amqpConstructStore);

        //create MessageRouter Registry
        MessageRouterRegistry messageRouterRegistry = new MessageRouterRegistry();
        AndesContext.getInstance().setMessageRouterRegistry(messageRouterRegistry);

        //create Storage Queue Registry
        StorageQueueRegistry storageQueueRegistry = new StorageQueueRegistry();
        AndesContext.getInstance().setStorageQueueRegistry(storageQueueRegistry);

        //create subscription registry and manager
        subscriptionRegistry = new SubscriptionRegistry();
    }

    /**
     * Starts all andes components such as the subscription engine, messaging engine, cluster event sync tasks, etc.
     *
     * @throws Exception
     */
    private static void startAndesComponents() throws Exception {

        //create subscription registry and manager
        AndesSubscriptionManager subscriptionManager = new AndesSubscriptionManager(subscriptionRegistry,
                contextStore);
        AndesContext.getInstance().setAndesSubscriptionManager(subscriptionManager);
        ClusterResourceHolder.getInstance().setSubscriptionManager(subscriptionManager);

        MessagingEngine messagingEngine = MessagingEngine.getInstance();
        messagingEngine.initialise(messageStore, new MessageExpiryManager(messageStore));

        // initialise Andes context information related manager class
        AndesContextInformationManager contextInformationManager =
                new AndesContextInformationManager(amqpConstructStore, subscriptionManager,
                        contextStore, messageStore);
        AndesContext.getInstance().setAndesContextInformationManager(contextInformationManager);

        //Create an inbound event manager. This will prepare inbound events to disruptor
        InboundEventManager inboundEventManager =  new InboundEventManager(messagingEngine);
        AndesContext.getInstance().setInboundEventManager(inboundEventManager);

        DtxRegistry dtxRegistry = new DtxRegistry(messageStore.getDtxStore(), messagingEngine, inboundEventManager);

        //Initialize Andes API (used by all inbound transports)
        Andes.getInstance().initialise(messagingEngine, inboundEventManager, contextInformationManager,
                                       subscriptionManager, dtxRegistry);

        if (!AndesContext.getInstance().isClusteringEnabled()) {
            Andes.getInstance().makeActive();
        }

        //Initialize cluster notification listener (null if standalone)
        if(null != clusterNotificationListenerManager) {
            clusterNotificationListenerManager.initializeListener(inboundEventManager, subscriptionManager,
                    contextInformationManager);
        }
    }

    /**
     * Create Pre-defined exchanges, queues, bindings and subscriptions and other artifacts
     * at the startup
     *
     * @throws AndesException
     */
    private static void createDefinedProtocolArtifacts() throws AndesException {
        //Create MQTT exchange
        InboundExchangeEvent inboundExchangeEvent = new
                InboundExchangeEvent(MQTTUtils.MQTT_EXCHANGE_NAME, "topic", false);
        Andes.getInstance().createExchange(inboundExchangeEvent);
    }

    /**
     * Initialize mode of cluster event synchronization depending on configurations and start listeners.
     */
    private static void initClusterEventListener() throws AndesException {

        CoordinationComponentFactory coordinationComponentFactory = new CoordinationComponentFactory();
        clusterNotificationListenerManager = coordinationComponentFactory.createClusterNotificationListener();
        AndesContext.getInstance().setClusterNotificationListenerManager(clusterNotificationListenerManager);
    }

    /**
     * Starts the andes cluster.
     */
    public static void startAndesCluster() throws Exception {

        // Initialize cluster manager
        initClusterManager();

        // Create the listener for listening for cluster events
        initClusterEventListener();

        //Start components such as the subscription manager, subscription engine, messaging engine, etc.
        startAndesComponents();

    }

    /**
     * Stops tasks for cluster event synchronization.
     */
    public static void shutDownAndesClusterEventSynchronization() throws AndesException {
        if (ClusterResourceHolder.getInstance().getClusterManager().isClusteringEnabled()) {
            clusterNotificationListenerManager.stopListener();
        }
    }


    /**
     * Bring the node to the state of the cluster. If this is the coordinator, disconnect all active durable
     * subscriptions.
     *
     * @throws AndesException
     */
    public static void syncNodeWithClusterState() throws AndesException {

        //at the startup reload exchanges/queues/bindings and subscriptions
        log.info("Syncing exchanges, queues, bindings and subscriptions");
        ClusterResourceHolder.getInstance().getAndesRecoveryTask()
                .recoverBrokerArtifacts();

        /**
         * remove all subscriptions registered by the local node ID.
         * During a node crash there can be stale subscriptions hanging around.
         */
        AndesContext.getInstance().getAndesSubscriptionManager().
                closeAllActiveLocalSubscriptions();
    }

    /**
     * start andes house keeping threads for the broker
     *
     * @throws AndesException
     */
    public static void startHouseKeepingThreads() throws AndesException {

        //reload exchanges/queues/bindings and subscriptions
        AndesContextInformationManager contextInformationManager = AndesContext.getInstance()
                .getAndesContextInformationManager();
        AndesSubscriptionManager subscriptionManager = AndesContext.getInstance()
                .getAndesSubscriptionManager();
        InboundEventManager inboundEventManager = AndesContext.getInstance()
                .getInboundEventManager();
        AndesRecoveryTask andesRecoveryTask = new AndesRecoveryTask(contextInformationManager,
                subscriptionManager, inboundEventManager);

        //deleted the expired message from db
        PeriodicExpiryMessageDeletionTask periodicExpiryMessageDeletionTask = null;

        int recoveryTaskScheduledPeriod = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_FAILOVER_VHOST_SYNC_TASK_INTERVAL);
        int dbBasedDeletionTaskScheduledPeriod = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_PERIODIC_EXPIRY_MESSAGE_DELETION_INTERVAL);
        int safeDeleteRegionSlotCount = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SAFE_DELETE_REGION_SLOT_COUNT);

        periodicExpiryMessageDeletionTask = new PeriodicExpiryMessageDeletionTask();

        andesRecoveryTaskScheduler.scheduleAtFixedRate(andesRecoveryTask, recoveryTaskScheduledPeriod,
                recoveryTaskScheduledPeriod, TimeUnit.SECONDS);
        if (safeDeleteRegionSlotCount >= 1) {
            expiryMessageDeletionTaskScheduler.scheduleAtFixedRate(periodicExpiryMessageDeletionTask,
                    dbBasedDeletionTaskScheduledPeriod, dbBasedDeletionTaskScheduledPeriod, TimeUnit.SECONDS);
        } else {
            log.error("DB based expiry message deletion task is not scheduled due to not providing "
                    + "a valid safe delete region slot count is not given. Given slot count is "
                    + safeDeleteRegionSlotCount);
        }

        ClusterResourceHolder.getInstance().setAndesRecoveryTask(andesRecoveryTask);
    }

    /**
     * Stop andes house keeping threads
     */
    public static void stopHouseKeepingThreads() {
        log.info("Stop syncing exchanges, queues, bindings and subscriptions...");
        int threadTerminationTimePerod = 20; // seconds
        try {
            andesRecoveryTaskScheduler.shutdown();
            expiryMessageDeletionTaskScheduler.shutdown();
            expiryMessageDeletionTaskScheduler.awaitTermination(threadTerminationTimePerod, TimeUnit.SECONDS);
            andesRecoveryTaskScheduler.awaitTermination(threadTerminationTimePerod, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            andesRecoveryTaskScheduler.shutdownNow();
            log.warn("Recovery task scheduler is forcefully shutdown.");
        }

    }

    /**
     * Register Andes MBeans
     *
     */
    public static void registerMBeans() throws AndesException {

        try {
            ClusterManagementInformationMBean clusterManagementMBean = new
                    ClusterManagementInformationMBean(
                    ClusterResourceHolder.getInstance().getClusterManager());
            clusterManagementMBean.register();

            SubscriptionManagementInformationMBean subscriptionManagementInformationMBean = new
                    SubscriptionManagementInformationMBean();
            subscriptionManagementInformationMBean.register();

            MessageStatusInformationMBean messageStatusInformationMBean = new
                    MessageStatusInformationMBean();
            messageStatusInformationMBean.register();
        } catch (JMException ex) {
            throw new AndesException("Unable to register Andes MBeans", ex);
        }
    }

    /**
     * Start manager to join node to the cluster and register
     * node in the cluster
     *
     * @throws AndesException
     */
    private static void initClusterManager() throws AndesException {

        /**
         * initialize cluster manager for managing nodes in MB cluster
         */
        ClusterManager clusterManager = new ClusterManager();
        clusterManager.init();
        ClusterResourceHolder.getInstance().setClusterManager(clusterManager);

        //TODO: remove as submanager starts after cluster manager now
       // AndesContext.getInstance().getAndesSubscriptionManager().
        //        setLocalNodeId(clusterManager.getMyNodeID());
    }

    /**
     * reinitialize message stores after a connection lost
     * to DB
     * @throws Exception
     */
    public static void reInitializeAndesStores() throws Exception {
        log.info("Reinitializing Andes Stores...");
        StoreConfiguration virtualHostsConfiguration =
                AndesContext.getInstance().getStoreConfiguration();
        AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();
        andesContextStore.init(virtualHostsConfiguration.getContextStoreProperties());
        messageStore.initializeMessageStore(andesContextStore,
                                            virtualHostsConfiguration.getMessageStoreProperties());
    }

    /**
     * Start accepting and delivering messages
     */
    public static void startMessaging() throws AndesException {
        Andes.getInstance().startMessageDelivery();
    }

    /**
     * Stop worker threads, close transports and stop message delivery
     *
     */
    private static void stopMessaging() throws AndesException {
        //this will un-assign all slots currently owned
        Andes.getInstance().stopMessageDelivery();
    }

    /**
     * Create a DEAD_LETTER_CHANNEL for the super tenant.
     */
    public static void createSuperTenantDLC() throws AndesException {
//        CarbonContext carbonContext = CarbonContext.getThreadLocalCarbonContext();
//        try {
//            String adminUserName = carbonContext.getUserRealm().getRealmConfiguration().getAdminUserName();
//            DLCQueueUtils.createDLCQueue(carbonContext.getTenantDomain(), adminUserName);
//        } catch (UserStoreException e) {
//            throw new AndesException("Error getting super tenant username", e);
//        }
    }

    public static boolean isKernelShuttingDown() {
        return isKernelShuttingDown;
    }

}
