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
import org.wso2.andes.kernel.slot.SlotCreator;
import org.wso2.andes.kernel.slot.SlotDeletionExecutor;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.ClusterAgent;
import org.wso2.andes.server.cluster.ClusterManagementInformationMBean;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationListenerManager;
import org.wso2.andes.server.cluster.coordination.EventListenerCreator;
import org.wso2.andes.server.cluster.coordination.StandaloneEventListenerCreator;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastBasedEventListenerCreator;
import org.wso2.andes.server.cluster.coordination.rdbms.RDBMSBasedEventListenerCreator;
import org.wso2.andes.server.cluster.coordination.rdbms.RDBMSClusterNotificationListenerManager;
import org.wso2.andes.server.information.management.MessageStatusInformationMBean;
import org.wso2.andes.server.information.management.SubscriptionManagementInformationMBean;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostConfigSynchronizer;
import org.wso2.andes.store.FailureObservingAndesContextStore;
import org.wso2.andes.store.FailureObservingMessageStore;
import org.wso2.andes.subscription.SubscriptionEngine;
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
    private static MessageStore messageStore;

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
     * This enables creating cluster event listeners depending on whether cluster communication happens through
     * Hazelcast or RDBMS or whether it is running in the standalone mode.
     */
    private static EventListenerCreator eventListenerCreator;

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
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("AndesRecoveryTask-%d").build();
        andesRecoveryTaskScheduler = Executors.newScheduledThreadPool(threadPoolCount);
        expiryMessageDeletionTaskScheduler = Executors.newScheduledThreadPool(threadPoolCount);
        startHouseKeepingThreads();
        syncNodeWithClusterState();
        registerMBeans();
        startThriftServer();
        Andes.getInstance().startSafeZoneAnalysisWorker();
        //Start slot deleting thread only if clustering is enabled.
        //Otherwise slots assignment will not happen
        if (AndesContext.getInstance().isClusteringEnabled()) {
            SlotDeletionExecutor.getInstance().init();
        }
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
        List<AndesQueue> queueList = contextStore.getAllQueuesStored();
        List<Future> futureSlotRecoveryExecutorList = new ArrayList<Future>();
        Integer concurrentReads = AndesConfigurationManager.readValue
                (AndesConfiguration.RECOVERY_MESSAGES_CONCURRENT_STORAGE_QUEUE_READS);
        ExecutorService executorService = Executors.newFixedThreadPool(concurrentReads);
        for (final AndesQueue queue : queueList) {
            final String queueName = queue.queueName;
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
    private static MessageStore createMessageStoreFromConfig(AndesContextStore andesContextStore) throws Exception {
        StoreConfiguration andesConfiguration = AndesContext.getInstance()
                .getStoreConfiguration();
        
     // create a message store and initialise messaging engine
        String messageStoreClassName = andesConfiguration.getMessageStoreClassName();
        Class<? extends MessageStore> messageStoreClass = Class.forName(messageStoreClassName).asSubclass(MessageStore.class);
        MessageStore messageStoreInConfig = messageStoreClass.newInstance();

        messageStoreInConfig.initializeMessageStore(andesContextStore,
                                                    andesConfiguration.getMessageStoreProperties());
        
        log.info("Andes MessageStore initialised with " + messageStoreClassName);
        return messageStoreInConfig;
    }

    /**
     * Starts all andes components such as the subscription engine, messaging engine, cluster event sync tasks, etc.
     *
     * @throws Exception
     */
    private static void startAndesComponents() throws Exception {

        //create subscription store
        SubscriptionEngine subscriptionEngine = new SubscriptionEngine();
        AndesContext.getInstance().setSubscriptionEngine(subscriptionEngine);

        /**
         * initialize subscription managing
         */
        AndesSubscriptionManager subscriptionManager = new AndesSubscriptionManager();
        ClusterResourceHolder.getInstance().setSubscriptionManager(subscriptionManager);
        subscriptionManager.init(eventListenerCreator);

        // Whether expiry check is enabled/disabled for DLC
        boolean isExpiryCheckEnabledInDLC
                = AndesConfigurationManager.readValue(AndesConfiguration.PERFORMANCE_TUNING_EXPIRE_MESSAGES_IN_DLC);
        MessageExpiryManager messageExpiryManager;
        //depends on the configuration bind the appropriate expiry manger with the messaging engine
        if (isExpiryCheckEnabledInDLC) {
            messageExpiryManager = new DLCMessageExpiryManager(messageStore);
        } else {
            messageExpiryManager = new DefaultMessageExpiryManager(messageStore);
        }

        MessagingEngine messagingEngine = MessagingEngine.getInstance();
        messagingEngine.initialise(messageStore, subscriptionEngine, messageExpiryManager, eventListenerCreator);

        // initialise Andes context information related manager class
        AndesContextInformationManager contextInformationManager =
                new AndesContextInformationManager(AndesContext.getInstance().getAMQPConstructStore(),
                        subscriptionEngine, contextStore, eventListenerCreator);

        // When message stores are initialised initialise Andes as well.
        Andes.getInstance().initialise(subscriptionEngine, messagingEngine,
                contextInformationManager, subscriptionManager);
    }

    /**
     * Start all andes stores message store/context store and AMQP construct store
     *
     * @throws Exception
     */
    public static void startAndesStores() throws Exception {

        //Create a andes context store and register
        AndesContextStore contextStoreInConfig = createAndesContextStoreFromConfig();
        AndesKernelBoot.contextStore = new FailureObservingAndesContextStore(contextStoreInConfig);
        AndesContext.getInstance().setAndesContextStore(contextStore);

        // directly wire the instance without wrapped instance
        messageStore = new FailureObservingMessageStore(createMessageStoreFromConfig(contextStoreInConfig));
        // Setting the message store in the context store
        AndesContext.getInstance().setMessageStore(messageStore);

        //create AMQP Constructs store
        AMQPConstructStore amqpConstructStore = new AMQPConstructStore(contextStore, messageStore);
        AndesContext.getInstance().setAMQPConstructStore(amqpConstructStore);
    }

    /**
     * Initialize mode of cluster event synchronization depending on configurations and start listeners.
     */
    private static void initClusterEventSynchronizationMode() throws AndesException {

        if (ClusterResourceHolder.getInstance().getClusterManager().isClusteringEnabled()) {
            if (AndesConfigurationManager.readValue(AndesConfiguration.CLUSTER_EVENT_SYNC_MODE_RDBMS_ENABLED)) {
                eventListenerCreator = new RDBMSBasedEventListenerCreator();
                log.info("Broker is initialized with RDBMS based cluster event synchronization.");
                clusterNotificationListenerManager = new RDBMSClusterNotificationListenerManager();
            } else {
                eventListenerCreator = new HazelcastBasedEventListenerCreator();
                log.info("Broker is initialized with HAZELCAST based cluster event synchronization.");
                clusterNotificationListenerManager = HazelcastAgent.getInstance();
            }
        } else {
            eventListenerCreator = new StandaloneEventListenerCreator();
        }
    }

    /**
     * Starts the andes cluster.
     */
    public static void startAndesCluster() throws Exception {

        // Initialize cluster manager
        initClusterManager();

        // Initialize the cluster communication mode whether it is HAZELCAST or RDBMS.
        initClusterEventSynchronizationMode();

        // Clear all slots and cluster notifications at a cluster startup
        clearMembershipEventsAndRecoverDistributedSlotMap();

        //Start components such as the subscription manager, subscription engine, messaging engine, etc.
        startAndesComponents();

        // Initialize listener to be notified of cluster events.
        if (ClusterResourceHolder.getInstance().getClusterManager().isClusteringEnabled()) {
            clusterNotificationListenerManager.initializeListener();
        }
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

        // Mark all the existing durable subscriptions as inactive when starting a standalone node or the coordinator
        // node since there can be no active durable subscribers when starting the first node of the cluster.
        // Having existing active durable subscribers causes them to not be able to reconnect to the broker
        // The deactivation should not be performed by just any node but the first node to start
        boolean isClusteringEnabled = AndesContext.getInstance().isClusteringEnabled();
        if (!(isClusteringEnabled)
                || (isClusteringEnabled && AndesContext.getInstance().getClusterAgent().isCoordinator())) {
            ClusterResourceHolder.getInstance().getSubscriptionManager().deactivateAllActiveSubscriptions();
        }

        //at the startup reload exchanges/queues/bindings and subscriptions
        log.info("Syncing exchanges, queues, bindings and subscriptions");
        ClusterResourceHolder.getInstance().getAndesRecoveryTask()
                .recoverExchangesQueuesBindingsSubscriptions();

        // All non-durable subscriptions subscribed from this node will be deleted since, there
        // can't be any non-durable subscriptions as node just started.
        // closeAllClusterSubscriptionsOfNode() should only be called after
        // recoverExchangesQueuesBindingsSubscriptions() executed.
        String myNodeId = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        ClusterResourceHolder.getInstance().getSubscriptionManager()
                .closeAllLocalSubscriptionsOfNode(myNodeId);
    }

    /**
     * start andes house keeping threads for the broker
     *
     * @throws AndesException
     */
    public static void startHouseKeepingThreads() throws AndesException {

        //reload exchanges/queues/bindings and subscriptions
        AndesRecoveryTask andesRecoveryTask = new AndesRecoveryTask(eventListenerCreator);
        //deleted the expired message from db
        PeriodicExpiryMessageDeletionTask periodicExpiryMessageDeletionTask = null;

        int recoveryTaskScheduledPeriod = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_FAILOVER_VHOST_SYNC_TASK_INTERVAL);
        int dbBasedDeletionTaskScheduledPeriod = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_PERIODIC_EXPIRY_MESSAGE_DELETION_INTERVAL);
        int safeDeleteRegionSlotCount = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SAFE_DELETE_REGION_SLOT_COUNT);
        boolean isDLCExpiryCheckEnabled = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_EXPIRE_MESSAGES_IN_DLC);

        //based on the DLC expiration check configuration bind the appropriate deletion task
        if (isDLCExpiryCheckEnabled) {
            periodicExpiryMessageDeletionTask = new DLCExpiryCheckEnabledDeletionTask();
        } else {
            periodicExpiryMessageDeletionTask = new PeriodicExpiryMessageDeletionTask();
        }

        andesRecoveryTaskScheduler.scheduleAtFixedRate(andesRecoveryTask, recoveryTaskScheduledPeriod,
                                                       recoveryTaskScheduledPeriod, TimeUnit.SECONDS);
        if (safeDeleteRegionSlotCount >= 1) {
            expiryMessageDeletionTaskScheduler.scheduleAtFixedRate(periodicExpiryMessageDeletionTask,
                    dbBasedDeletionTaskScheduledPeriod, dbBasedDeletionTaskScheduledPeriod, TimeUnit.SECONDS);
        } else {
            log.error("DB based expiry message deletion task is not scheduled due to not providing " +
                    "a valid safe delete region slot count is not given");
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
     * Start andes components
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

        // NOTE: Feature Message Expiration moved to a future release
//        Andes.getInstance().startMessageExpirationWorker();
    }

    /**
     * Stop worker threads, close transports and stop message delivery
     *
     */
    private static void stopMessaging() {
        // NOTE: Feature Message Expiration moved to a future release
//        Andes.getInstance().stopMessageExpirationWorker();

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
