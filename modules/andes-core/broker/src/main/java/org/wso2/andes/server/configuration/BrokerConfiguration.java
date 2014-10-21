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

package org.wso2.andes.server.configuration;


/**
 * <class>ClusterConfiguration</class> Holds all the cluster specific Configurations;
 */
public class BrokerConfiguration {

    /**
     * keeps sever configuration object
     */
    private final ServerConfiguration serverConfig;

    private String bindIpAddress;

    private int globalQueueCount;

    private int slotWindowSize;

    private int slotDeliveryWorkerThreads;

    private int messageBatchSizeForSubscribersQueues;

    private int maxNumberOfUnackedMessages;

    private int maxNumberOfReadButUndeliveredMessages;

    private int andesInternalParallelThreadPoolSize;

    private int internalSequentialThreadPoolSize;

    private int publisherPoolSize;

    private int maxAckWaitTime;

    private int contentRemovalTaskInterval;

    private int slotSubmitTimeOut;

    private int andesRecoveryTaskInterval;

    private int messageReadCacheSize;

    private int messageBatchSizeForBrowserSubscriptions;

    private int numberOfMaximumDeliveryCount;

    private boolean isInMemoryMode;

    private String messageIdGeneratorClass;

    private double globalMemoryThresholdRatio;

    private double globalMemoryRecoveryThresholdRatio;

    private long memoryCheckInterval;

    private int JMSExpirationCheckInterval;

    private int expirationMessageBatchSize;

    private boolean saveExpiredToDLC;

    /**
     * Create cluster configuration object
     * @param serverConfig  server configuration to get configs
     */
    public BrokerConfiguration(final ServerConfiguration serverConfig) {
         this.serverConfig = serverConfig;
         intilizeConfig();
    }

    public void intilizeConfig() {

        globalQueueCount =  serverConfig.getGlobalQueueCount();
        slotWindowSize =  serverConfig.getSlotWindowSize();
        slotDeliveryWorkerThreads = serverConfig.getNumberOFSlotDeliveryWorkerThreads();
        messageBatchSizeForSubscribersQueues =  serverConfig.getMessageBatchSizeForSubscribersQueues();
        maxNumberOfUnackedMessages = serverConfig.getMaxNumberOfUnackedMessages();
        maxNumberOfReadButUndeliveredMessages = serverConfig.getMaxNumberOfReadButUndeliveredMessages();
        andesInternalParallelThreadPoolSize = serverConfig.getAndesInternalParallelThreadPoolSize();
        internalSequentialThreadPoolSize = serverConfig.getInternalSequentialThreadPoolSize();
        publisherPoolSize = serverConfig.getPublisherPoolSize();
        maxAckWaitTime =  serverConfig.getMaxAckWaitTime();
        contentRemovalTaskInterval = serverConfig.getContentRemovalTaskInterval();
        slotSubmitTimeOut = serverConfig.getSlotSubmitTimeOut();
        andesRecoveryTaskInterval = serverConfig.getAndesRecoveryTaskInterval();
        messageReadCacheSize = serverConfig.getMessageReadCacheSize();
        messageBatchSizeForBrowserSubscriptions = serverConfig.getMessageBatchSizeForBrowserSubscriptions();
        numberOfMaximumDeliveryCount = serverConfig.getNumberOfMaximumDeliveryCount();
        isInMemoryMode = serverConfig.isInMemoryModeEnabled();
        messageIdGeneratorClass = serverConfig.getMessageIdGeneratorClass();
        globalMemoryThresholdRatio =  serverConfig.getFlowControlGlobalMemoryThresholdRatio();
        globalMemoryRecoveryThresholdRatio = serverConfig.getGlobalMemoryRecoveryThresholdRatio();
        memoryCheckInterval = serverConfig.getMemoryCheckInterval();
        JMSExpirationCheckInterval = serverConfig.getJMSExpirationCheckInterval();
        expirationMessageBatchSize = serverConfig.getExpirationMessageBatchSize();
        saveExpiredToDLC = serverConfig.getSaveExpiredToDLC();

    }

    public int getGlobalQueueCount(){
        return globalQueueCount;
    }

    public int getSlotWindowSize(){
        return slotWindowSize;
    }

    public int getNumberOFSlotDeliveryWorkerThreads(){
        return slotDeliveryWorkerThreads;
    }

    public int getMaxNumberOfUnackedMessages() {
        return maxNumberOfUnackedMessages;
    }

    public int getMaxNumberOfReadButUndeliveredMessages(){
        return  maxNumberOfReadButUndeliveredMessages;
    }

    public int getAndesInternalParallelThreadPoolSize(){
        return andesInternalParallelThreadPoolSize;
    }

    public int getInternalSequentialThreadPoolSize() {
        return internalSequentialThreadPoolSize;
    }

    public int getPublisherPoolSize() {
        return publisherPoolSize;
    }

    public int getMaxAckWaitTime() {
        return maxAckWaitTime;
    }

    public int getContentRemovalTaskInterval() {
        return contentRemovalTaskInterval;
    }

    public int getSlotSubmitTimeOut() {
        return slotSubmitTimeOut;
    }

    public int getAndesRecoveryTaskInterval() {
        return andesRecoveryTaskInterval;
    }

    public String getBindIpAddress() {
        return bindIpAddress;
    }

    public void setBindIpAddress(String bindIpAddress) {
        this.bindIpAddress = bindIpAddress;
    }

    public int getMessageReadCacheSize() {
        return serverConfig.getMessageReadCacheSize();
    }

    public int getMessageBatchSizeForBrowserSubscriptions(){
        return messageBatchSizeForBrowserSubscriptions;
    }

    public int getNumberOfMaximumDeliveryCount(){
        return numberOfMaximumDeliveryCount;
    }

      /**
     *
     * @return  whether running in INMemory Mode
     */
    public Boolean isInMemoryMode() {
         return  isInMemoryMode;
    }

    public String getMessageIdGeneratorClass() {
        return messageIdGeneratorClass;
    }


    public double getGlobalMemoryThresholdRatio() {
        return globalMemoryThresholdRatio;
    }

    public double getGlobalMemoryRecoveryThresholdRatio() {
        return globalMemoryRecoveryThresholdRatio;
    }

    public long getMemoryCheckInterval() {
        return memoryCheckInterval;
    }

    public int getJMSExpirationCheckInterval() {
        return JMSExpirationCheckInterval;
    }

    public int getExpirationMessageBatchSize() {
        return expirationMessageBatchSize;
    }

    public Boolean getSaveExpiredToDLC() {
        return saveExpiredToDLC;
    }
}
