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
import org.wso2.andes.kernel.slot.SlotCreator;
import org.wso2.andes.kernel.slot.SlotDeletionExecutor;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.ClusterAgent;
import org.wso2.andes.server.cluster.ClusterManagementInformationMBean;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.information.management.MessageStatusInformationMBean;
import org.wso2.andes.server.information.management.SubscriptionManagementInformationMBean;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostConfigSynchronizer;
import org.wso2.andes.store.FailureObservingAndesContextStore;
import org.wso2.andes.store.FailureObservingMessageStore;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.andes.thrift.MBThriftServer;
import org.wso2.carbon.context.CarbonContext;
import org.wso2.carbon.user.api.UserStoreException;

import javax.management.JMException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Andes kernel startup/shutdown related work is done through this class.
 */
public class AndesKernelBoot {
    private static Log log = LogFactory.getLog(AndesKernelBoot.class);
    private static VirtualHost virtualHost;
    private static MessageStore messageStore;

    /**
     * Scheduled thread pool executor to run periodic andes recovery task
     */
    private static ScheduledExecutorService andesRecoveryTaskScheduler;

    /**
     * Used to get information from context store
     */
    private static AndesContextStore contextStore;

    /**
     * This is used by independent worker threads to identify if the kernel is performing shutdown operations.
     */
    private static boolean isKernelShuttingDown = false;

    /**
     * This will boot up all the components in Andes kernel and bring the server to working state
     */
    public static void bootAndesKernel() throws AndesException {
        try {
            isKernelShuttingDown = false;
            //loadConfigurations - done from outside
            //startAndesStores - done from outside
            int threadPoolCount = 1;
            andesRecoveryTaskScheduler = Executors.newScheduledThreadPool(threadPoolCount);
            startAndesComponents();
            startHouseKeepingThreads();
            syncNodeWithClusterState();
            registerMBeans();
            startThriftServer();
            startMessaging();
            createSuperTenantDLC();

            //Start slot deleting thread only if clustering is enabled.
            //Otherwise slots assignment will not happen
            if (AndesContext.getInstance().isClusteringEnabled()) {
                SlotDeletionExecutor.getInstance().init();
            }

            Andes.getInstance().startSafeZoneAnalysisWorker();
        } catch (JMException e) {
            throw new AndesException("Unable to register Andes MBeans", e);
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
    public static void recoverDistributedSlotMap() throws AndesException {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
            try {
                hazelcastAgent.acquireInitializationLock();
                if (!hazelcastAgent.isClusterInitializedSuccessfully()) {
                    clearSlotStorage();

                    // Initialize current node's last published ID
                    ClusterAgent clusterAgent = AndesContext.getInstance().getClusterAgent();
                    contextStore.setNodeToLastPublishedId(clusterAgent.getLocalNodeIdentifier(), 0);

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
     * Set the default virtual host. Andes operates
     * this virtual host only
     * @param defaultVirtualHost virtual host to set
     */
    public static void setVirtualHost(VirtualHost defaultVirtualHost) {
        virtualHost = defaultVirtualHost;
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
     * start all andes stores message store/context store and AMQP construct store
     * @throws Exception
     */
    public static void startAndesStores() throws Exception {

        //Create a andes context store and register
        AndesContextStore contextStoreInConfig = createAndesContextStoreFromConfig();
        
        AndesKernelBoot.contextStore =  new FailureObservingAndesContextStore(contextStoreInConfig) ;
        AndesContext.getInstance().setAndesContextStore(contextStore);
        
        //create subscription store
        SubscriptionStore subscriptionStore = new SubscriptionStore();
        AndesContext.getInstance().setSubscriptionStore(subscriptionStore);
        
        /**
         * initialize subscription managing
         */
        AndesSubscriptionManager subscriptionManager = new AndesSubscriptionManager();
        ClusterResourceHolder.getInstance().setSubscriptionManager(subscriptionManager);
        subscriptionManager.init();

        // directly wire the instance without wrapped instance
        messageStore = new FailureObservingMessageStore(createMessageStoreFromConfig(contextStoreInConfig));
        MessagingEngine messagingEngine = MessagingEngine.getInstance();
        messagingEngine.initialise(messageStore, subscriptionStore);

        // Setting the message store in the context store
        AndesContext.getInstance().setMessageStore(messageStore);

        //create AMQP Constructs store
        AMQPConstructStore amqpConstructStore = new AMQPConstructStore(contextStore, messageStore);
        AndesContext.getInstance().setAMQPConstructStore(amqpConstructStore);

        // initialise Andes context information related manager class
        AndesContextInformationManager contextInformationManager = 
                new AndesContextInformationManager(amqpConstructStore, subscriptionStore,
                                                   contextStore, messageStore);
        
        // When message stores are initialised initialise Andes as well.
        Andes.getInstance().initialise(subscriptionStore, messagingEngine,
                contextInformationManager, subscriptionManager);

        // initialize amqp constructs syncing into Qpid
        VirtualHostConfigSynchronizer _VirtualHostConfigSynchronizer = new
                VirtualHostConfigSynchronizer(
                virtualHost);
        ClusterResourceHolder.getInstance()
                             .setVirtualHostConfigSynchronizer(_VirtualHostConfigSynchronizer);
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
        AndesRecoveryTask andesRecoveryTask = new AndesRecoveryTask();
        Integer scheduledPeriod = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_FAILOVER_VHOST_SYNC_TASK_INTERVAL);
        andesRecoveryTaskScheduler.scheduleAtFixedRate(andesRecoveryTask, scheduledPeriod,
                                                       scheduledPeriod, TimeUnit.SECONDS);
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
            andesRecoveryTaskScheduler
                    .awaitTermination(threadTerminationTimePerod, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            andesRecoveryTaskScheduler.shutdownNow();
            log.warn("Recovery task scheduler is forcefully shutdown.");
        }

    }

    /**
     * Register andes MBeans
     *
     * @throws JMException
     */
    public static void registerMBeans() throws JMException {

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
    }

    /**
     * Start andes components
     *
     * @throws AndesException
     */
    public static void startAndesComponents() throws AndesException {

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
    private static void createSuperTenantDLC() throws AndesException {
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
