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
import org.wso2.andes.configuration.VirtualHostsConfiguration;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.AndesSubscriptionManager;
import org.wso2.andes.server.cluster.ClusterManagementInformationMBean;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.configuration.BrokerConfiguration;
import org.wso2.andes.server.information.management.MessageStatusInformationMBean;
import org.wso2.andes.server.information.management.SubscriptionManagementInformationMBean;
import org.wso2.andes.server.slot.thrift.MBThriftServer;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostConfigSynchronizer;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Andes kernel startup/shutdown related work is done through this class.
 */
public class AndesKernelBoot {
    private static Log log = LogFactory.getLog(AndesKernelBoot.class);
    private static BrokerConfiguration clusterConfiguration;
    private static VirtualHost virtualHost;
    private static MessageStore messageStore;

    /**
     * Scheduled thread pool executor to run periodic andes recovery task
     */
    private static ScheduledExecutorService andesRecoveryTaskScheduler;

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
        MessagingEngine.getInstance().close();
    }

    /**
     * load configurations to andes kernel
     *
     * @param configuration configuration to load
     */
    public static void loadConfigurations(BrokerConfiguration configuration) {
        clusterConfiguration = configuration;
    }

    /**
     * start all andes stores message store/context store and AMQP construct store
     * @throws Exception
     */
    public static void startAndesStores() throws Exception {

        VirtualHostsConfiguration virtualHostsConfiguration = AndesContext.getInstance()
                                                                          .getVirtualHostsConfiguration();
        //create a andes context store and register
        String contextStoreClassName = virtualHostsConfiguration.getAndesContextStoreClassName();
        Class contextStoreClass = Class.forName(contextStoreClassName);
        Object contextStoreInstance = contextStoreClass.newInstance();

        if (!(contextStoreInstance instanceof AndesContextStore)) {
            throw new ClassCastException(
                    "Message store class must implement " + AndesContextStore.class + ". Class "
                    + contextStoreClass +
                    " does not.");
        }

        AndesContextStore andesContextStore = (AndesContextStore) contextStoreInstance;
        andesContextStore.init(
                virtualHostsConfiguration.getAndesContextStoreProperties()
        );
        AndesContext.getInstance().setAndesContextStore(andesContextStore);

        //create subscription store
        SubscriptionStore subscriptionStore = new SubscriptionStore();
        AndesContext.getInstance().setSubscriptionStore(subscriptionStore);

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
                    messageStoreClass +
                    " does not.");
        }

        MessageStore messageStore = (MessageStore) messageStoreInstance;
        messageStore.initializeMessageStore(
                virtualHostsConfiguration.getMessageStoreProperties()
        );
        MessagingEngine.getInstance().initialise(messageStore);
        AndesKernelBoot.messageStore = messageStore;

        /**
         * initialize amqp constructs syncing into Qpid
         */
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
        //tell cluster I am leaving
        ClusterResourceHolder.getInstance().getClusterManager().shutDownMyNode();
    }

    /**
     * start andes house keeping threads for the broker
     *
     * @throws Exception
     */
    public static void startHouseKeepingThreads() throws Exception {
        //reload exchanges/queues/bindings and subscriptions
        AndesRecoveryTask andesRecoveryTask = new AndesRecoveryTask();
        int scheduledPeriod = clusterConfiguration.getAndesRecoveryTaskInterval();
        andesRecoveryTaskScheduler.scheduleAtFixedRate(andesRecoveryTask, scheduledPeriod,
                                                       scheduledPeriod, TimeUnit.SECONDS);
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
         * initialize subscription managing
         */
        AndesSubscriptionManager subscriptionManager =
                new AndesSubscriptionManager();
        ClusterResourceHolder.getInstance().setSubscriptionManager(subscriptionManager);
        subscriptionManager.init();

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
        VirtualHostsConfiguration virtualHostsConfiguration =
                AndesContext.getInstance().getVirtualHostsConfiguration();
        messageStore.initializeMessageStore(virtualHostsConfiguration.getMessageStoreProperties());
        AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();
        andesContextStore.init(virtualHostsConfiguration.getAndesContextStoreProperties());
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
        MessagingEngine.getInstance().startMessageDelivery();
        MessagingEngine.getInstance().startMessageExpirationWorker();
    }

    /**
     * Close transports and stop message delivery
     *
     * @throws Exception
     */
    private static void stopMessaging() throws Exception {
        MessagingEngine.getInstance().stopMessageExpirationWorker();
        //this will un-assign all slots currently owned
        MessagingEngine.getInstance().stopMessageDelivery();
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
}
