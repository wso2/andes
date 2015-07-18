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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.JMException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.StoreConfiguration;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.SlotDeletionExecutor;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.kernel.slot.SlotManagerStandalone;
import org.wso2.andes.server.ClusterResourceHolder;
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

/**
 * Andes kernel startup/shutdown related work is done through this class.
 */
public class AndesKernelBoot {
    private static Log log = LogFactory.getLog(AndesKernelBoot.class);
    private static VirtualHost virtualHost;
    private static MessageStore messageStore;

    /**
     * slot remapping database read counter keep against storage queue name
     */
    private static Map<String, Integer> databaseReadsCounterMap;

    /**
     * slot remapping message counter keep against storage queue name
     */
    private static Map<String, Integer> restoreMessagesCounterMap;

    /**
     * Number of remaining messages retrieves and used to display useful log message while
     * slot recovery task running
     */
    private static Map<String, Long> totalRemainingMessagesInQueue;

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
     * Keep track of first message id read from database in recovery mode to
     * use in queue browse as soon as server startup
     */
    private static Map<String, Long> firstRecoveredMessageIdMap;

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
     * server will iterate through all the queues available in the context store and inform the
     * slot manager to recreate the slot mapping. Finally the distribute variable is updated to
     * indicate the success and the lock is released.
     *
     * @throws AndesException
     */
    public static void recoverDistributedSlotMap() throws AndesException {
        // Slot recreation
        databaseReadsCounterMap = new HashMap<String, Integer>();
        restoreMessagesCounterMap = new HashMap<String, Integer>();
        firstRecoveredMessageIdMap = new HashMap<String, Long>();
        totalRemainingMessagesInQueue = new HashMap<String, Long>();
        if (AndesContext.getInstance().isClusteringEnabled()) {
            HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
            try {
                hazelcastAgent.acquireInitializationLock();
                if (!hazelcastAgent.isClusterInitializedSuccessfully()) {
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
            Future submit = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        log.info("Slot restoring start in " + queueName);
                        initializeSlotMapForQueue(queueName);
                        log.info("Slot restoring end in " + queueName);
                    } catch (AndesException e) {
                        log.error("Error occurred in slot recovery.", e);
                    }
                }
            });
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
     * Create slots for the given queue name. This is done by reading all the messages from the
     * message store and creating slots according to the slot window size.
     *
     * @param queueName
     *         Name of the queue
     * @throws AndesException
     */
    private static void initializeSlotMapForQueue(String queueName)
            throws AndesException {
        int databaseReadsCounter = 0;
        int restoreMessagesCounter = 0;
        // Read slot window size from cluster configuration
        Integer slotSize = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SLOTS_SLOT_WINDOW_SIZE);
        //Retrieve message metadata from the store that are not in the DLC
        List<AndesMessageMetadata> messageList = messageStore
                .getNextNMessageMetadataFromQueue(queueName, 0, slotSize, true);
        int numberOfMessages = messageList.size();

        //setting up timer to print restoring message count and database read count
        databaseReadsCounter++;
        restoreMessagesCounter = restoreMessagesCounter + messageList.size();
        databaseReadsCounterMap.put(queueName, databaseReadsCounter);
        restoreMessagesCounterMap.put(queueName, restoreMessagesCounter);
        long messageCountOfQueue = MessagingEngine.getInstance().getMessageCountOfQueue(queueName);
        totalRemainingMessagesInQueue.put(queueName, messageCountOfQueue);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduleTimerToPrintCounter(scheduledExecutorService, queueName);

        if (restoreMessagesCounter > 0) {
           firstRecoveredMessageIdMap.put(queueName, messageList.get(0).getMessageID());
        }

        long lastMessageID;
        long firstMessageID;

        while (numberOfMessages > 0) {
            lastMessageID = messageList.get(messageList.size() - 1).getMessageID();
            firstMessageID = messageList.get(0).getMessageID();

            if (log.isDebugEnabled()) {
                log.debug("Created a slot with " + messageList.size() + " messages for queue (" + queueName + ")");
            }
            if (AndesContext.getInstance().isClusteringEnabled()) {
                SlotManagerClusterMode.getInstance().updateMessageID(queueName,
                        HazelcastAgent.getInstance().getNodeId(), firstMessageID, lastMessageID);
            } else {
                SlotManagerStandalone.getInstance().updateMessageID(queueName,lastMessageID);
            }
            // We need to increment lastMessageID since the getNextNMessageMetadataFromQueue returns message list
            // including the given starting ID.
            //Retrieve message metadata from the store that are not in the DLC
            messageList = messageStore
                    .getNextNMessageMetadataFromQueue(queueName, lastMessageID + 1, slotSize, true);
            numberOfMessages = messageList.size();
            //increase value of counters
            databaseReadsCounter++;
            restoreMessagesCounter = restoreMessagesCounter + messageList.size();
            databaseReadsCounterMap.put(queueName, databaseReadsCounter);
            restoreMessagesCounterMap.put(queueName, restoreMessagesCounter);
        }
        printCounter(queueName);
        scheduledExecutorService.shutdownNow();
    }

    /**
     * Message count and database read count prints in each 30 seconds until slot mapping restoration completes
     *
     * @param scheduledExecutorService ScheduledExecutorService object
     */
    private static void scheduleTimerToPrintCounter(ScheduledExecutorService scheduledExecutorService,
                                                    final String queueName) {
        long printDelay = 30L;
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                printCounter(queueName);
            }
        }, 0, printDelay, TimeUnit.SECONDS);
    }

    /**
     * Print INFO log with slot mapping restore details
     *
     */
    private static void printCounter(String queueName) {
        if (restoreMessagesCounterMap.get(queueName) > 0) {
            double restoreMessageCount = restoreMessagesCounterMap.get(queueName);
            double totalRemainingCount = totalRemainingMessagesInQueue.get(queueName);
            double percentage = restoreMessageCount / totalRemainingCount * 100;
            log.info("Message recovery daemon " + restoreMessagesCounterMap.get(queueName)
                    + "/" + totalRemainingMessagesInQueue.get(queueName)
                    + " (" + new BigDecimal(percentage).setScale(0, RoundingMode.CEILING) + "%)"
                    + " - ["+queueName+"] number of database calls ["+databaseReadsCounterMap.get(queueName)+"].");
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
     * bring the node to the state of the cluster
     *
     * @throws Exception
     */
    public static void syncNodeWithClusterState() throws AndesException {
        //at the startup reload exchanges/queues/bindings and subscriptions
        log.info("Syncing exchanges, queues, bindings and subscriptions");
        ClusterResourceHolder.getInstance().getAndesRecoveryTask()
                             .recoverExchangesQueuesBindingsSubscriptions();
    }

    /**
     * clean up broker states and notify the cluster
     *
     * @throws AndesException
     */
    private static void cleanUpAndNotifyCluster() throws AndesException {
        //at the shutDown close all localSubscriptions and notify cluster

        ClusterResourceHolder.getInstance().getSubscriptionManager().closeAllLocalSubscriptionsOfNode();
        // notify cluster this MB node is shutting down. For other nodes to do recovery tasks
        ClusterResourceHolder.getInstance().getClusterManager().shutDownMyNode();
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
        andesRecoveryTaskScheduler.scheduleAtFixedRate(andesRecoveryTask, scheduledPeriod, scheduledPeriod, TimeUnit.SECONDS);
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
        messageStore.initializeMessageStore(andesContextStore, virtualHostsConfiguration.getMessageStoreProperties());
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
     * Return first recovered message id by queue name
     * @param queueName name of the queue
     * @return first recovered message id
     */
    public static long getFirstRecoveredMessageId(String queueName) {
        long firstMessageId = 0L;
        Long messageId = firstRecoveredMessageIdMap.get(queueName);
        if (messageId != null) {
            firstMessageId = messageId;
        }
        return firstMessageId;
    }
}
