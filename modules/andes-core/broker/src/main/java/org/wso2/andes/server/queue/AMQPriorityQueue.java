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

import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionList;
import org.wso2.andes.server.virtualhost.VirtualHost;

import java.util.Map;

public class AMQPriorityQueue extends SimpleAMQQueue
{
    protected AMQPriorityQueue(final AMQShortString name,
                               final boolean durable,
                               final AMQShortString owner,
                               final boolean autoDelete,
                               boolean exclusive,
                               final VirtualHost virtualHost, 
                               int priorities, Map<String, Object> arguments)
    {
        super(name, durable, owner, autoDelete, exclusive, virtualHost,new PriorityQueueList.Factory(priorities), arguments);
    }

    public AMQPriorityQueue(String queueName,
                            boolean durable,
                            String owner,
                            boolean autoDelete,
                            boolean exclusive, VirtualHost virtualHost, int priorities, Map<String,Object> arguments)
    {
        this(queueName == null ? null : new AMQShortString(queueName), durable, owner == null ? null : new AMQShortString(owner),
                autoDelete, exclusive,virtualHost, priorities, arguments);
    }

    public int getPriorities()
    {
        return ((PriorityQueueList) _entries).getPriorities();
    }

    @Override
    protected void checkSubscriptionsNotAheadOfDelivery(final QueueEntry entry)
    {
        // check that all subscriptions are not in advance of the entry
        SubscriptionList.SubscriptionNodeIterator subIter = _subscriptionList.iterator();
        while(subIter.advance() && entry.isAvailable())
        {
            final Subscription subscription = subIter.getNode().getSubscription();
            if(!subscription.isClosed())
            {
                QueueContext context = (QueueContext) subscription.getQueueContext();
                if(context != null)
                {
                    QueueEntry subnode = context._lastSeenEntry;
                    QueueEntry released = context._releasedEntry;
                    while(subnode != null && entry.compareTo(subnode) < 0 && entry.isAvailable() && (released == null || released.compareTo(entry) < 0))
                    {
                        if(QueueContext._releasedUpdater.compareAndSet(context,released,entry))
                        {
                            break;
                        }
                        else
                        {
                            subnode = context._lastSeenEntry;
                            released = context._releasedEntry;
                        }
                    }
                }
            }

        }
    }
}
