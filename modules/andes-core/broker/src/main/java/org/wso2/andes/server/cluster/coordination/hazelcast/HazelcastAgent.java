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
package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;

/**
 * This is a singleton class, which contains all Hazelcast related operations.
 */
public class HazelcastAgent {
    private static Log log = LogFactory.getLog(HazelcastAgent.class);

    private static HazelcastAgent hazelcastAgent;

    private HazelcastInstance hazelcastInstance;
    private ITopic subscriptionChangedNotifierChannel;
    private ITopic queueChangedNotifierChannel;

    private HazelcastAgent(){
        log.info("====================================Initializing Hazelcast Instance");
        this.hazelcastInstance = AndesContext.getInstance().getHazelcastInstance();
        this.hazelcastInstance.getCluster().addMembershipListener(new AndesMembershipListener());

        this.subscriptionChangedNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.SUBSCRIPTION_CHANGED_NOTIFIER_TOPIC_NAME);
        this.subscriptionChangedNotifierChannel.addMessageListener(new SubscriptionChangedListener());

        this.queueChangedNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.QUEUE_CHANGED_NOTIFIER_TOPIC_NAME);
        this.queueChangedNotifierChannel.addMessageListener(new QueueChangedListener());
        log.info("====================================Successfully Initialized Hazelcast Instance");
    }

    public static HazelcastAgent getInstance() {
        if (hazelcastAgent == null) {

            synchronized (HazelcastAgent.class) {
                if(hazelcastAgent == null)  {
                    hazelcastAgent = new HazelcastAgent();
                }
            }
        }

        return hazelcastAgent;
    }


}
