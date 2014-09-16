/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
public class ClusterConfiguration {

    /**
     * keeps sever configuration object
     */
    private final ServerConfiguration serverConfig;

    private String bindIpAddress;


    /**
     * Create cluster configuration object
     * @param serverConfig
     */
    public ClusterConfiguration (final ServerConfiguration serverConfig) {
         this.serverConfig = serverConfig;
    }

//    public boolean isOnceInOrderSupportEnabled() {
//        return serverConfig.isOnceInOrderSupportEnabled();
//    }

    public int getGlobalQueueCount(){
        return serverConfig.getGlobalQueueCount();
    }

    public int getMessageBatchSizeForSubscribersQueues() {
        return serverConfig.getMessageBatchSizeForSubscribersQueues();
    }

    public int getMessageBatchSizeForSubscribers() {
        return serverConfig.getDefaultMessageBatchSizeForSubscribers();
    }

    public int getMaxMessageBatchSizeForSubscribers() {
        return serverConfig.getMaxMessageBatchSizeForSubscribers();
    }

    public int getMinMessageBatchSizeForSubscribers() {
        return serverConfig.getMinMessageBatchSizeForSubscribers();
    }

    public int getMaxNumberOfUnackedMessages() {
        return serverConfig.getMaxNumberOfUnackedMessages();
    }

    public int getMaxNumberOfReadButUndeliveredMessages(){
        return  serverConfig.getMaxNumberOfReadButUndeliveredMessages();
    }

    public int getFlusherPoolSize() {
        return serverConfig.getFlusherPoolSize();
    }

    public int getAndesInternalParallelThreadPoolSize(){
        return serverConfig.getAndesInternalParallelThreadPoolSize();
    }

    public int andesExecutorServicePoolSize() {
        return serverConfig.getAndesExecutorServicePoolSize();
    }

    public int getSubscriptionPoolSize() {
        return serverConfig.getSubscriptionPoolSize();
    }

    public int getInternalSequentialThreadPoolSize() {
        return serverConfig.getInternalSequentialThreadPoolSize();
    }

    public int getPublisherPoolSize() {
        return serverConfig.getPublisherPoolSize();
    }

    public int getMaxAckWaitTime() {
        return serverConfig.getMaxAckWaitTime();
    }

    public int getMaxAckWaitTimeForBatch() {
        return serverConfig.getMaxAckWaitTimeForBatch();
    }

    public int getQueueWorkerInterval() {
        return serverConfig.getQueueWorkerInterval();
    }

    public int getPubSubMessageRemovalTaskInterval() {
        return serverConfig.getPubSubMessageRemovalTaskInterval();
    }

    public int getContentRemovalTaskInterval() {
        return serverConfig.getContentRemovalTaskInterval();
    }


    public int getContentRemovalTimeDifference() {
        return serverConfig.getContentRemovalTimeDifference();
    }

    public int getAndesRecoveryTaskInterval() {
        return serverConfig.getAndesRecoveryTaskInterval();
    }

    public int getQueueMsgDeliveryCurserResetTimeInterval() {
        return serverConfig.getQueueMsgDeliveryCurserResetTimeInterval();
    }

    public String getReferenceTime() {
        return serverConfig.getReferenceTime();
    }

    public String getBindIpAddress() {
        return bindIpAddress;
    }

    public void setBindIpAddress(String bindIpAddress) {
        this.bindIpAddress = bindIpAddress;
    }

    public int getGlobalQueueWorkerMessageBatchSize() {
        return serverConfig.getGlobalQueueWorkerMessageBatchSize();
    }

    public int getContentPublisherMessageBatchSize() {
        return serverConfig.getContentPublisherMessageBatchSize();
    }

    public int getMetadataPublisherMessageBatchSize() {
        return serverConfig.getMetadataPublisherMessageBatchSize();
    }

    public int getMessageReadCacheSize() {
        return serverConfig.getMessageReadCacheSize();
    }

    public int getMessageBatchSizeForBrowserSubscriptions(){
        return serverConfig.getMessageBatchSizeForBrowserSubscriptions();
    }

    public boolean getIsExternalCassandraserverRequired() {
        return serverConfig.getIsExternalCassandraServerRequired();
    }

    public int getNumberOfMaximumDeliveryCount(){
        return serverConfig.getNumberOfMaximumDeliveryCount();
    }

    public boolean getViewMessageCounts(){
        return serverConfig.getViewMessageCounts();
    }
      /**
     *
     * @return  whether running in INMemory Mode
     */
    public Boolean isInMemoryMode() {
         return  serverConfig.isInMemoryModeEnabled();
    }

    public String getMessageIdGeneratorClass() {
        return serverConfig.getMessageIdGeneratorClass();
    }


    public double getGlobalMemoryThresholdRatio() {
        return serverConfig.getFlowControlGlobalMemoryThresholdRatio();
    }

    public double getGlobalMemoryRecoveryThresholdRatio() {
        return serverConfig.getGlobalMemoryRecoveryThresholdRatio();
    }

    public long getMemoryCheckInterval() {
        return serverConfig.getMemoryCheckInterval();
    }

    public int getPerConnectionMessageRateThreshold() {
        return serverConfig.getFlowControlPerConnectionMessageThreshold();
    }

    public int getJMSExpirationCheckInterval() {
        return serverConfig.getJMSExpirationCheckInterval();
    }

    public int getExpirationMessageBatchSize() {
        return serverConfig.getExpirationMessageBatchSize();
    }

    public Boolean getSaveExpiredToDLC() {
        return serverConfig.getSaveExpiredToDLC();
    }
}
