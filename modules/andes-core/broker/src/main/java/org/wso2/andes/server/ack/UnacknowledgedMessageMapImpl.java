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
package org.wso2.andes.server.ack;

import org.wso2.andes.AMQException;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.carbon.andes.core.internal.configuration.AndesConfigurationManager;
import org.wso2.carbon.andes.core.internal.configuration.enums.AndesConfiguration;
import org.wso2.carbon.andes.core.internal.configuration.util.TopicMessageDeliveryStrategy;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class UnacknowledgedMessageMapImpl implements UnacknowledgedMessageMap
{
    private final Object _lock = new Object();

    private long _unackedSize;

    private Map<Long, QueueEntry> _map;

    private long _lastDeliveryTag;

    private final int _prefetchLimit;

    /**
     * Data structure to keep track of messages which are sent but not yet acknowledged.
     *
     * @param prefetchLimit Initial size of the data structure
     * @param amqChannel    The channel which this data structure belongs to
     * @param isDurable     Is this structure for created to keep durable messages (queues/durable topics)
     */
    public UnacknowledgedMessageMapImpl(int prefetchLimit, AMQChannel amqChannel, boolean isDurable) {
        _prefetchLimit = prefetchLimit;

        TopicMessageDeliveryStrategy messageDeliveryStrategy = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_TOPIC_MESSAGE_DELIVERY_STRATEGY);

        if (!isDurable && messageDeliveryStrategy.equals(TopicMessageDeliveryStrategy.DISCARD_ALLOWED)) {
            int growLimit = AndesConfigurationManager.readValue
                    (AndesConfiguration.PERFORMANCE_TUNING_ACK_HANDLING_MAX_UNACKED_MESSAGES);
            _map = new LimitedSizeQueueEntryHolder(_prefetchLimit, growLimit, amqChannel);
        } else {
            _map = new LinkedHashMap<Long, QueueEntry>(prefetchLimit);
        }
    }

    public void collect(long deliveryTag, boolean multiple, Map<Long, QueueEntry> msgs)
    {
        if (multiple)
        {
            collect(deliveryTag, msgs);
        }
        else
        {
            final QueueEntry entry = get(deliveryTag);
            if(entry != null)
            {
                msgs.put(deliveryTag, entry);
            }
        }

    }

    public void remove(Map<Long,QueueEntry> msgs)
    {
        synchronized (_lock)
        {
            for (Long deliveryTag : msgs.keySet())
            {
                remove(deliveryTag);
            }
        }
    }

    public QueueEntry remove(long deliveryTag)
    {
        synchronized (_lock)
        {

            QueueEntry message = _map.remove(deliveryTag);
            if(message != null)
            {
                _unackedSize -= message.getMessage().getSize();

            }

            return message;
        }
    }

    public void visit(Visitor visitor) throws AMQException
    {
        synchronized (_lock)
        {
            Set<Map.Entry<Long, QueueEntry>> currentEntries = _map.entrySet();
            for (Map.Entry<Long, QueueEntry> entry : currentEntries)
            {
                visitor.callback(entry.getKey().longValue(), entry.getValue());
            }
            visitor.visitComplete();
        }
    }

    public void add(long deliveryTag, QueueEntry message)
    {
        synchronized (_lock)
        {
            _map.put(deliveryTag, message);
            _unackedSize += message.getMessage().getSize();
            _lastDeliveryTag = deliveryTag;
        }
    }

    public Collection<QueueEntry> cancelAllMessages()
    {
        synchronized (_lock)
        {
            Collection<QueueEntry> currentEntries = _map.values();
            _map = new LinkedHashMap<Long, QueueEntry>(_prefetchLimit);
            _unackedSize = 0l;
            return currentEntries;
        }
    }

    public int size()
    {
        synchronized (_lock)
        {
            return _map.size();
        }
    }

    public void clear()
    {
        synchronized (_lock)
        {
            _map.clear();
            _unackedSize = 0l;
        }
    }

    public QueueEntry get(long key)
    {
        synchronized (_lock)
        {
            return _map.get(key);
        }
    }

    public Set<Long> getDeliveryTags()
    {
        synchronized (_lock)
        {
            return _map.keySet();
        }
    }

    private void collect(long key, Map<Long, QueueEntry> msgs)
    {
        synchronized (_lock)
        {
            for (Map.Entry<Long, QueueEntry> entry : _map.entrySet())
            {
                msgs.put(entry.getKey(),entry.getValue());
                if (entry.getKey() == key)
                {
                    break;
                }
            }
        }
    }

}
