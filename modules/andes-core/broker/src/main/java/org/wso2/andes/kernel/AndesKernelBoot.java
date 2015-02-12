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
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.ClusterManagementInformationMBean;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.information.management.MessageStatusInformationMBean;
import org.wso2.andes.server.information.management.SubscriptionManagementInformationMBean;
import org.wso2.andes.kernel.slot.SlotManager;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.thrift.MBThriftServer;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostConfigSynchronizer;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.carbon.context.CarbonContext;
import org.wso2.carbon.user.api.UserStoreException;

import java.util.List;
import java.util.concurrent.Executors;
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
     * This will boot up all the components in Andes kernel and bring the server to working state
     */
    public static void bootAndesKernel() throws Exception {

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
        // Slot recreation is required in the clustering mode
        if (AndesContext.getInstance().isClusteringEnabled()) {
            HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();

            try {
                hazelcastAgent.acquireInitializationLock();
                if (!hazelcastAgent.isClusterInitializedSuccessfully()) {
                    log.info("Restoring slot mapping in the cluster.");

                    List<AndesQueue> queueList = contextStore.getAllQueuesStored();

                    for (AndesQueue queue : queueList) {
                        // Skip slot creation for Dead letter Channel
                        if (DLCQueueUtils.isDeadLetterQueue(queue.queueName)) {
                            continue;
                        }

                        initializeSlotMapForQueue(queue.queueName);
                    }

                    hazelcastAgent.indicateSuccessfulInitilization();
                }
            } finally {
                hazelcastAgent.releaseInitializationLock();
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
        // Read slot window size from cluster configuration
        Integer slotSize = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SLOTS_SLOT_WINDOW_SIZE);
        List<AndesMessageMetadata> messageList = messageStore
                .getNextNMessageMetadataFromQueue(queueName, 0, slotSize);
        int numberOfMessages = messageList.size();
        long lastMessageID;
        long firstMessageID;

        while (numberOfMessages > 0) {
            lastMessageID = messageList.get(messageList.size() - 1).getMessageID();
            firstMessageID = messageList.get(0).getMessageID();

            if (log.isDebugEnabled()) {
                log.debug("Created a slot with " + messageList.size() + " messages for queue (" + queueName + ")");
            }
            SlotManager.getInstance().updateMessageID(queueName, HazelcastAgent.getInstance().getNodeId(), firstMessageID, lastMessageID);

            // We need to increment lastMessageID since the getNextNMessageMetadataFromQueue returns message list
            // including the given starting ID.
            messageList = messageStore
                    .getNextNMessageMetadataFromQueue(queueName, lastMessageID + 1, slotSize);
            numberOfMessages = messageList.size();
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
     * @throws Exception
     */
    public static void shutDownAndesKernel() throws Exception {

        stopMessaging();
        stopThriftServer();
        unregisterMBeans();
        cleanUpAndNotifyCluster();
        stopHouseKeepingThreads();
        stopAndesComponents();
        //close stores
        AndesContext.getInstance().getAndesContextStore().close();
        Andes.getInstance().shutDown();
    }

    /**
     * start all andes stores message store/context store and AMQP construct store
     * @throws Exception
     */
    public static void startAndesStores() throws Exception {

        StoreConfiguration virtualHostsConfiguration = AndesContext.getInstance()
                                                                          .getStoreConfiguration();
        //create a andes context store and register
        String contextStoreClassName = virtualHostsConfiguration.getAndesContextStoreClassName();
        Class contextStoreClass = Class.forName(contextStoreClassName);
        Object contextStoreInstance = contextStoreClass.newInstance();

        if (!(contextStoreInstance instanceof AndesContextStore)) {
            throw new ClassCastException(
                    "Message store class must implement " + AndesContextStore.class + ". Class "
                    + contextStoreClass + " does not.");
        }

        AndesContextStore andesContextStore = (AndesContextStore) contextStoreInstance;
        andesContextStore.init(
                virtualHostsConfiguration.getContextStoreProperties()
        );
        
        log.info("AndesContextStore initialised with " + contextStoreClassName);
        AndesContext.getInstance().setAndesContextStore(andesContextStore);
        AndesKernelBoot.contextStore = andesContextStore;

        //create subscription store
        SubscriptionStore subscriptionStore = new SubscriptionStore();
        AndesContext.getInstance().setSubscriptionStore(subscriptionStore);

        /**
         * initialize subscription managing
         */
        AndesSubscriptionManager subscriptionManager = new AndesSubscriptionManager();
        ClusterResourceHolder.getInstance().setSubscriptionManager(subscriptionManager);
        subscriptionManager.init();

        //create AMQP Constructs store
        AMQPConstructStore amqpConstructStore = new AMQPConstructStore();
        AndesContext.getInstance().setAMQPConstructStore(amqpConstructStore);

        // create a message store and initialise messaging engine
        String messageStoreClassName = virtualHostsConfiguration.getMessageStoreClassName();
        Class messageStoreClass = Class.forName(messageStoreClassName);
        Object messageStoreInstance = messageStoreClass.newInstance();

        if (!(messageStoreInstance instanceof org.wso2.andes.kernel.MessageStore)) {
            throw new ClassCastException(
                    "Message store class must implement " + MessageStore.class + ". Class " +
                    messageStoreClass + " does not.");
        }

        MessageStore messageStore = (MessageStore) messageStoreInstance;
        messageStore.initializeMessageStore(
                virtualHostsConfiguration.getMessageStoreProperties()
        );
        log.info("Andes MessageStore initialised with " + messageStoreClassName);
        
        MessagingEngine messagingEngine = MessagingEngine.getInstance();
        messagingEngine.initialise(messageStore, andesContextStore,subscriptionStore);
        AndesKernelBoot.messageStore = messageStore;

        // initialise Andes context information related manager class
        AndesContextInformationManager contextInformationManager = 
                new AndesContextInformationManager(subscriptionStore);
        
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
    public static void syncNodeWithClusterState() throws Exception {
        //at the startup reload exchanges/queues/bindings and subscriptions
        log.info("Syncing exchanges, queues, bindings and subscriptions");
        ClusterResourceHolder.getInstance().getAndesRecoveryTask()
                             .recoverExchangesQueuesBindingsSubscriptions();
    }

    /**
     * clean up broker states and notify the cluster
     *
     * @throws Exception
     */
    private static void cleanUpAndNotifyCluster() throws Exception {
        //at the shutDown close all localSubscriptions and notify cluster
        log.info("Closing all local subscriptions existing...");
        ClusterResourceHolder.getInstance().getSubscriptionManager()
                             .closeAllLocalSubscriptionsOfNode();
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
     * stop andes house keeping threads
     *
     * @throws Exception
     */
    private static void stopHouseKeepingThreads() throws Exception {
        log.info("Stop syncing exchanges, queues, bindings and subscriptions...");
        int threadTerminationTimePerod = 20; // seconds
        try {
            andesRecoveryTaskScheduler.shutdown();
            andesRecoveryTaskScheduler
                    .awaitTermination(threadTerminationTimePerod, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            andesRecoveryTaskScheduler.shutdownNow();
            log.warn("Recovery task scheduler is forcefully shutdown.");
            throw e;
        }

    }

    /**
     * register andes MBeans
     *
     * @throws Exception
     */
    public static void registerMBeans() throws Exception {

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
     * unregister any MBeans registered
     *
     * @throws Exception
     */
    private static void unregisterMBeans() throws Exception {

    }

    /**
     * start andes components
     *
     * @throws Exception
     */
    public static void startAndesComponents() throws Exception {

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
        messageStore.initializeMessageStore(virtualHostsConfiguration.getMessageStoreProperties());
        AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();
        andesContextStore.init(virtualHostsConfiguration.getContextStoreProperties());
    }

    /**
     * Stop andes components
     *
     * @throws Exception
     */
    private static void stopAndesComponents() throws Exception {

    }

    /**
     * Start accepting and delivering messages
     *
     * @throws Exception
     */
    public static void startMessaging() throws Exception {
        Andes.getInstance().startMessageDelivery();

        // NOTE: Feature Message Expiration moved to a future release
//        Andes.getInstance().startMessageExpirationWorker();
    }

    /**
     * Close transports and stop message delivery
     *
     * @throws Exception
     */
    private static void stopMessaging() throws Exception {
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
        MBThriftServer.getInstance().start(AndesContext.getInstance().getThriftServerHost(),
                AndesContext.getInstance().getThriftServerPort(), "MB-ThriftServer-main-thread");

    }

    /**
     * Stop the thrift server
     */
    private static void stopThriftServer(){
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
}
