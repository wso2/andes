/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.server.virtualhost.plugins.policies;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.configuration.plugins.ConfigurationPlugin;
import org.wso2.andes.server.exchange.TopicExchange;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.protocol.AMQSessionModel;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.virtualhost.plugins.logging.TopicDeletePolicyMessages;
import org.wso2.andes.policies.SlowConsumerPolicyPlugin;
import org.wso2.andes.policies.SlowConsumerPolicyPluginFactory;

public class TopicDeletePolicy implements SlowConsumerPolicyPlugin
{
    Logger _logger = Logger.getLogger(TopicDeletePolicy.class);
    private TopicDeletePolicyConfiguration _configuration;

    public static class TopicDeletePolicyFactory implements SlowConsumerPolicyPluginFactory
    {
        public TopicDeletePolicy newInstance(ConfigurationPlugin configuration) throws ConfigurationException
        {
            TopicDeletePolicyConfiguration config =
                    configuration.getConfiguration(TopicDeletePolicyConfiguration.class.getName());

            TopicDeletePolicy policy = new TopicDeletePolicy();
            policy.configure(config);
            return policy;
        }

        public String getPluginName()
        {
            return "topicdelete";
        }

        public Class<TopicDeletePolicy> getPluginClass()
        {
            return TopicDeletePolicy.class;
        }
    }

    public void performPolicy(AMQQueue q)
    {
        if (q == null)
        {
            return;
        }

        AMQSessionModel owner = q.getExclusiveOwningSession();

        // Only process exclusive queues
        if (owner == null)
        {
            return;
        }

        //Only process Topics
        if (!validateQueueIsATopic(q))
        {
            return;
        }

        try
        {
            CurrentActor.get().message(owner.getLogSubject(),TopicDeletePolicyMessages.DISCONNECTING());
            // Close the consumer . this will cause autoDelete Queues to be purged
            owner.getConnectionModel().
                    closeSession(owner, AMQConstant.RESOURCE_ERROR,
                                 "Consuming to slow.");

            // Actively delete non autoDelete queues if deletePersistent is set
            if (!q.isAutoDelete() && (_configuration != null && _configuration.deletePersistent()))
            {
                CurrentActor.get().message(q.getLogSubject(), TopicDeletePolicyMessages.DELETING_QUEUE());
                q.delete();
            }

        }
        catch (AMQException e)
        {
            _logger.warn("Unable to close consumer:" + owner + ", on queue:" + q.getName());
        }

    }

    /**
     * Check the queue bindings to validate the queue is bound to the
     * topic exchange.
     *
     * @param q the Queue
     *
     * @return true iff Q is bound to a TopicExchange
     */
    private boolean validateQueueIsATopic(AMQQueue q)
    {
        for (Binding binding : q.getBindings())
        {
            if (binding.getExchange() instanceof TopicExchange)
            {
                return true;
            }
        }

        return false;
    }

    public void configure(ConfigurationPlugin config)
    {
        _configuration = (TopicDeletePolicyConfiguration) config;
    }

    @Override
    public String toString()
    {
        return "TopicDelete" + (_configuration == null ? "" : "[" + _configuration + "]");
    }
}
