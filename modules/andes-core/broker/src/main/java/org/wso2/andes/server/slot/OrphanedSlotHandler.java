/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.slot;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.SubscriptionListener;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.slot.thrift.MBThriftUtils;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.Collection;

/**
 * This class will reassign the slots own by this node when its last subscriber leaves
 */
public class OrphanedSlotHandler implements SubscriptionListener {

    private static Log log = LogFactory
            .getLog(OrphanedSlotHandler.class);

    @Override
    public void handleClusterSubscriptionsChanged(AndesSubscription subscription,
                                                  SubscriptionChange changeType)
            throws AndesException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void handleLocalSubscriptionsChanged(LocalSubscription subscription,
                                                SubscriptionChange changeType)
            throws AndesException {
        switch (changeType) {
            case Deleted:
                reAssignSlots(subscription);
                break;
            case Disconnected:
                reAssignSlots(subscription);
                break;
        }
    }


    /**
     *Reassign slots back to the slot manager
     * @param subscription
     * @throws AndesException
     */
    private void reAssignSlots(AndesSubscription subscription) throws AndesException {
        if (!subscription.isBoundToTopic()) {
            // problem happens only with Queues
            SubscriptionStore subscriptionStore = AndesContext
                    .getInstance().getSubscriptionStore();
            String destination = subscription
                    .getSubscribedDestination();
            Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore
                    .getActiveLocalSubscribers(destination, false);
            if (localSubscribersForQueue.size() == 0) {
                String nodeId = HazelcastAgent.getInstance().getNodeId();
                try {
                    MBThriftUtils.getMBThriftClient().reAssignSlotWhenNoSubscribers(nodeId,
                            subscription.getTargetQueue());
                } catch (TException e) {
                    log.error("Error occurred while re-assigning the slot to slot manager", e);
                    throw new AndesException(
                            "Error occurred while re-assigning the slot to slot manager", e);
                }
            }

        }
    }

}
