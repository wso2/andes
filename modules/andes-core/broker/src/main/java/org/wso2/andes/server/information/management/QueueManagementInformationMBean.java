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
package org.wso2.andes.server.information.management;

import org.wso2.andes.kernel.*;
import org.wso2.andes.management.common.mbeans.QueueManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.GlobalQueueManager;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.SubscriptionStore;

import javax.management.NotCompliantMBeanException;

import java.util.ArrayList;
import java.util.List;

public class QueueManagementInformationMBean extends AMQManagedObject implements QueueManagementInformation {

    GlobalQueueManager globalQueueManager;
    MessageStore messageStore;

    public QueueManagementInformationMBean() throws NotCompliantMBeanException {
        super(QueueManagementInformation.class, QueueManagementInformation.TYPE);
        this.messageStore = MessagingEngine.getInstance().getDurableMessageStore();
        this.globalQueueManager = new GlobalQueueManager(messageStore);
    }

    public String getObjectInstanceName() {
        return QueueManagementInformation.TYPE;
    }

    public synchronized String[] getAllQueueNames() {

        try {
/*            ArrayList<String> queuesList = (ArrayList<String>) messageStore.getDestinationQueueNames();
            Iterator itr = queuesList.iterator();
            //remove topic specific queues
            while (itr.hasNext()) {
                String destinationQueueName = (String) itr.next();
                if(destinationQueueName.startsWith("tmp_") || destinationQueueName.contains(":")) {
                    itr.remove();
                }
            }
            String[] queues= new String[queuesList.size()];
            queuesList.toArray(queues);
            return queues;*/

            List<String> queuesList = AndesContext.getInstance().getSubscriptionStore().listQueues();
            String[] queues= new String[queuesList.size()];
            queuesList.toArray(queues);
            return queues;

        } catch (Exception e) {
          throw new RuntimeException("Error in accessing destination queues",e);
        }

    }

    public boolean isQueueExists(String queueName) {
        try {
            List<String> queuesList = AndesContext.getInstance().getSubscriptionStore().listQueues();
            return queuesList.contains(queueName);
        } catch (Exception e) {
          throw new RuntimeException("Error in accessing destination queues",e);
        }
    }

    @Override
    public void deleteAllMessagesInQueue(@MBeanOperationParameter(name = "queueName",
            description = "Name of the queue to delete messages from") String queueName) {
         //todo: we have to implement this 1. remove all messages in node queue 2.set counter to zero
        //todo: what happens if all messages were not copied to the node queue at the moment?
    }

    //TODO:when deleting queues from UI this is not get called. Instead we use AMQBrokerManagerMBean. Why are we keeping this?
    public void deleteQueue(@MBeanOperationParameter(name = "queueName",
            description = "Name of the queue to be deleted") String queueName) {
/*    	SubscriptionStore subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        try {
            if(subscriptionStore.getActiveClusterSubscribersForDestination(queueName, false).size() >0) {
                throw new Exception("Queue" + queueName +" Has Active Subscribers. Please Stop Them First.");
            }
            //remove queue
            AndesContext.getInstance().getSubscriptionStore().removeQueue(queueName,false);
            //caller should remove messages from global queue
            ClusterResourceHolder.getInstance().getSubscriptionManager().handleMessageRemovalFromGlobalQueue(queueName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }*/
    }
    /**
     * We are returning message count to the UI from this method.
     * When it has received Acks from the clients more than the message actual
     * message in the  queue,( This can happen when a copy of a message get
     * delivered to the consumer while the ACK for the previouse message was
     * on the way back to server), Message count is becoming minus.
     *
     * So from now on , we ll not provide minus values to the front end since
     * it is not acceptable
     *
     * */
    public int getMessageCount(String queueName) {

/*        int messageCount = (int) messageStore.getCassandraMessageCountForQueue(queueName);
        if (messageCount < 0) {
            messageStore.incrementQueueCount(queueName, Math.abs(messageCount));
            messageCount = 0;
        }
        return messageCount;*/
        int messageCount = 0;
        try {
        /**
         * Get message count from all node queues having subscriptions
         * plus the number of messages in respective global queue
         */
        SubscriptionStore subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        List<String> nodeQueuesHavingSubscriptionsForQueue = new ArrayList<String>(subscriptionStore.getNodeQueuesHavingSubscriptionsForQueue(queueName));
        if (nodeQueuesHavingSubscriptionsForQueue.size() > 0) {
            for (String nodeQueue : nodeQueuesHavingSubscriptionsForQueue) {
                QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE,nodeQueue);
                messageCount += messageStore.countMessagesOfQueue(nodeQueueAddress,queueName);
            }
        }
        String globalQueue = AndesUtils.getGlobalQueueNameForDestinationQueue(queueName);
        QueueAddress globalQueueAddress = new QueueAddress(QueueAddress.QueueType.GLOBAL_QUEUE,globalQueue);
        messageCount += messageStore.countMessagesOfQueue(globalQueueAddress,queueName);

        } catch (AndesException e) {
            throw new RuntimeException(e);
        }

        return messageCount;
    }

    public int getSubscriptionCount( String queueName){
        try {
            return AndesContext.getInstance().getSubscriptionStore().numberOfSubscriptionsForQueueInCluster(queueName);
        } catch (Exception e) {
            throw new RuntimeException("Error in getting subscriber count",e);
        }
    }
}
