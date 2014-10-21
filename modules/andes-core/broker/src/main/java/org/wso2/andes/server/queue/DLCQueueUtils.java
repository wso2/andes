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

package org.wso2.andes.server.queue;


import org.apache.log4j.Logger;

import org.wso2.andes.exchange.ExchangeDefaults;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.subscription.AMQPLocalSubscription;

/**
 * This class centralises the management of Dead Letter Queues by creating Dead Letter Queues when requested and
 * deciding on whether a queue is a Dead Letter Queue or not and generating the 'Dead Letter Queue' queue name for
 * the tenant.
 */
public class DLCQueueUtils {

    private static final Logger log = Logger.getLogger(DLCQueueUtils.class);

    /**
     * Derive the Dead Letter Queue name of the tenant with respect to a given queue of the same tenant.
     *
     * @param queueName  A queue name in the same tenant.
     * @param dlc_string The Dead Letter Queue suffix.
     * @return The Dead Letter Queue name for the tenant.
     */
    public static String identifyTenantInformationAndGenerateDLCString(String queueName, String dlc_string) {
        String destination_string;
        if (queueName.contains("/")) {
            //The Queue is in the tenant realm
            destination_string = queueName.split("/", 2)[0] + "/" + dlc_string;
        } else {
            destination_string = dlc_string;
        }
        return destination_string;
    }

    /**
     * Decides on whether a given queue name is a Dead Letter Queue or not.
     *
     * @param queueName The Queue name to test.
     * @return True if a Dead Letter Queue, False if not a Dead Letter Queue.
     */
    public static boolean isDeadLetterQueue(String queueName) {
        boolean isDeadLetterQueue = false;
        if (queueName.contains("/")) {
            //The Queue is in the tenant realm
            if (queueName.split("/", 2)[1].contains(AndesConstants.DEAD_LETTER_QUEUE_NAME)) {
                isDeadLetterQueue = true;
            }
        } else {
            if (queueName.equals(AndesConstants.DEAD_LETTER_QUEUE_NAME)) {
                isDeadLetterQueue = true;
            }
        }

        return isDeadLetterQueue;
    }

    /**
     * Creates a Dead Letter Queue for the tenant in a given queue name.
     *
     * @param queueName A queue name in the same tenant.
     * @param host      The Virtual Host.
     * @param owner     The tenant owner.
     * @throws AndesException
     */
    public static synchronized void createDLCQueue(String queueName, VirtualHost host, String owner) throws AndesException {
        String dlcQueueName = identifyTenantInformationAndGenerateDLCString(queueName,
                AndesConstants.DEAD_LETTER_QUEUE_NAME);
        QueueRegistry queueRegistry = host.getQueueRegistry();
        AMQQueue queue = queueRegistry.getQueue(new AMQShortString(dlcQueueName));
        try {
            if (queue == null && !isDeadLetterQueue(queueName)) {
                AndesQueue andesQueue = new AndesQueue(dlcQueueName, owner, false, true);

                AndesContext.getInstance().getAMQPConstructStore().addQueue(andesQueue, true);
                ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer().queue(dlcQueueName, owner,
                        false, null);


                LocalSubscription mockSubscription = new AMQPLocalSubscription(queueRegistry.getQueue(new
                        AMQShortString(dlcQueueName)), null, "0", dlcQueueName, false, false, false, null,
                        dlcQueueName, owner, ExchangeDefaults.DIRECT_EXCHANGE_NAME.toString(), "DIRECT", null, false);
                AndesContext.getInstance().getSubscriptionStore().createDisconnectOrRemoveClusterSubscription
                        (mockSubscription, SubscriptionListener.SubscriptionChange.Added);


                log.info(dlcQueueName + " Queue Created as Dead Letter Channel");
            }

        } catch (AndesException e) {
            throw new AndesException("Exception Caught While Creating the Dead Letter Queue : " + dlcQueueName, e);
        }

    }
}
