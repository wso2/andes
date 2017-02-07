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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.wso2.andes.kernel.slot.SlotCreator;
import org.wso2.andes.kernel.slot.SlotDeletionExecutor;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.ClusterAgent;
import org.wso2.andes.server.cluster.ClusterManagementInformationMBean;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationListenerManager;
import org.wso2.andes.server.cluster.coordination.CoordinationComponentFactory;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.information.management.MessageStatusInformationMBean;
import org.wso2.andes.server.information.management.SubscriptionManagementInformationMBean;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostConfigSynchronizer;
import org.wso2.andes.store.FailureObservingAndesContextStore;
import org.wso2.andes.store.FailureObservingMessageStore;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.thrift.MBThriftServer;
import org.wso2.carbon.context.CarbonContext;
import org.wso2.carbon.user.api.UserStoreException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
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
        startThriftServer();
        Andes.getInstance().startSafeZoneAnalysisWorker();
        int slotDeletingWorkerCount = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SLOT_DELETE_WORKER_COUNT);
        int maxNumberOfPendingSlotsToDelete = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SLOT_DELETE_QUEUE_DEPTH_WARNING_THRESHOLD);
        SlotDeletionExecutor.getInstance().init(slotDeletingWorkerCount, maxNumberOfPendingSlotsToDelete);
    }

    /**
     * This will recreate slot mapping for queues which have messages left in the message store.
     * The slot mapping is required only for the cluster implementation.
     *
     * First we acquire the slot initialization lock and check if the cluster is already
     * initialized using a distributed variable. Then if the cluster is not initialized, the
     * server will reset slot storage and iterate through all the queues available in the
     * context store and inform the the slot mapping. Finally the distribute variable is updated to
     * indicate the success and the lock is released.
     *
     * @throws AndesException
     */
    public static void clearMembershipEventsAndRecoverDistributedSlotMap() throws AndesException {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
            try {
                hazelcastAgent.acquireInitializationLock();
                if (!hazelcastAgent.isClusterInitializedSuccessfully()) {
                    contextStore.clearMembershipEvents();
                    contextStore.clearHeartBeatData();
                    clusterNotificationListenerManager.clearAllClusterNotifications();
                    clearSlotStorage();

                    // Initialize current node's last published ID
                    ClusterAgent clusterAgent = AndesContext.getInstance().getClusterAgent();
                    contextStore.setLocalSafeZoneOfNode(clusterAgent.getLocalNodeIdentifier(), 0);

                    recoverMapsForEachQueue();
                    hazelcastAgent.indicateSuccessfulInitilization();
                }
            } finally {
                hazelcastAgent.releaseInitializationLock();
            }
        } else {
            recoverMapsForEachQueue();
        }
    }

    /**
     * Generate slots for each queue
     * @throws AndesException
     */
    private static void recoverMapsForEachQueue() throws AndesException {
        List<StorageQueue> queueList = contextStore.getAllQueuesStored();
        List<Future> futureSlotRecoveryExecutorList = new ArrayList<>();
        Integer concurrentReads = AndesConfigurationManager.readValue
                (AndesConfiguration.RECOVERY_MESSAGES_CONCURRENT_STORAGE_QUEUE_READS);
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat
                ("SlotRecoveryThread-%d").build();
        ExecutorService executorService = Executors.newFixedThreadPool(concurrentReads,namedThreadFactory);
        for (final StorageQueue queue : queueList) {
            final String queueName = queue.getName();
            // Skip slot creation for Dead letter Channel
            if (DLCQueueUtils.isDeadLetterQueue(queueName)) {
                continue;
            }
            Future submit = executorService.submit(new SlotCreator(messageStore, queueName));
            futureSlotRecoveryExecutorList.add(submit);
        }
        for (Future slotRecoveryExecutor : futureSlotRecoveryExecutorList) {
            try {
                slotRecoveryExecutor.get();
            } catch (InterruptedException e) {
                log.error("Error occurred in slot recovery.", e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("Error occurred in slot recovery.", e);
            }
        }
        executorService.shutdown();
    }

    /**
     * Initialize the VirtualHostConfigSynchronizaer based on the provide virtual host. Andes operates on this virtual
     * host only
     *
     * @param defaultVirtualHost virtual host to set
     */
    public static void initVirtualHostConfigSynchronizer(VirtualHost defaultVirtualHost) {
        // initialize amqp constructs syncing into Qpid
        VirtualHostConfigSynchronizer _VirtualHostConfigSynchronizer = new VirtualHostConfigSynchronizer
                (defaultVirtualHost);
        ClusterResourceHolder.getInstance().setVirtualHostConfigSynchronizer(_VirtualHostConfigSynchronizer);
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
        messagingEngine.initialise(messageStore, new MessageExpiryManager(messageStore), subscriptionManager);

        // initialise Andes context information related manager class
        AndesContextInformationManager contextInformationManager =
                new AndesContextInformationManager(amqpConstructStore, subscriptionManager,
                        contextStore, messageStore);
        AndesContext.getInstance().setAndesContextInformationManager(contextInformationManager);

        //Create an inbound event manager. This will prepare inbound events to disruptor
        InboundEventManager inboundEventManager =  new InboundEventManager(subscriptionManager, messagingEngine);
        AndesContext.getInstance().setInboundEventManager(inboundEventManager);

        DtxRegistry dtxRegistry = new DtxRegistry(messageStore.getDtxStore(), messagingEngine, inboundEventManager);

        //Initialize Andes API (used by all inbound transports)
        Andes.getInstance().initialise(messagingEngine, inboundEventManager, contextInformationManager,
                                       subscriptionManager, dtxRegistry);

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

        // Clear all slots and cluster notifications at a cluster startup
        clearMembershipEventsAndRecoverDistributedSlotMap();

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
    public static void startMessaging() {
        Andes.getInstance().startMessageDelivery();
    }

    /**
     * Stop worker threads, close transports and stop message delivery
     *
     */
    private static void stopMessaging() {
        //this will un-assign all slots currently owned
        Andes.getInstance().stopMessageDelivery();
    }


    /**
     * Start the thrift server
     * @throws AndesException
     */
    private static void startThriftServer() throws AndesException {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            MBThriftServer.getInstance().start(AndesContext.getInstance().getThriftServerHost(),
                    AndesContext.getInstance().getThriftServerPort(), "MB-ThriftServer-main-thread");
        }

    }

    /**
     * Stop the thrift server
     */
    public static void stopThriftServer(){
        MBThriftServer.getInstance().stop();
    }

    /**
     * Create a DEAD_LETTER_CHANNEL for the super tenant.
     */
    public static void createSuperTenantDLC() throws AndesException {
        CarbonContext carbonContext = CarbonContext.getThreadLocalCarbonContext();
        try {
            String adminUserName = carbonContext.getUserRealm().getRealmConfiguration().getAdminUserName();
            DLCQueueUtils.createDLCQueue(carbonContext.getTenantDomain(), adminUserName);
        } catch (UserStoreException e) {
            throw new AndesException("Error getting super tenant username", e);
        }
    }

    public static boolean isKernelShuttingDown() {
        return isKernelShuttingDown;
    }

    /**
     * First node in the cluster clear and reset slot storage
     * This will clear slot related records in database which were created in previous session.
     * @throws AndesException
     */
    private static void clearSlotStorage() throws AndesException {
        SlotManagerClusterMode.getInstance().clearSlotStorage();
        log.info("Slots stored in last session were cleared to avoid duplicates when recovering.");
    }
}
