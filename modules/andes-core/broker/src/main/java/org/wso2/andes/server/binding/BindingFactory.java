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

package org.wso2.andes.server.binding;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQInternalException;
import org.wso2.andes.AMQSecurityException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.amqp.QpidAndesBridge;
import org.wso2.andes.configuration.qpid.BindingConfig;
import org.wso2.andes.configuration.qpid.BindingConfigType;
import org.wso2.andes.configuration.qpid.ConfigStore;
import org.wso2.andes.configuration.qpid.ConfiguredObject;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.NameValidationUtils;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.exchange.TopicExchange;
import org.wso2.andes.server.logging.actors.CurrentActor;
import org.wso2.andes.server.logging.messages.BindingMessages;
import org.wso2.andes.server.logging.subjects.BindingLogSubject;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.store.DurableConfigurationStore;
import org.wso2.andes.server.virtualhost.VirtualHost;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BindingFactory {
    private final VirtualHost _virtualHost;
    private final DurableConfigurationStore.Source _configSource;
    private final Exchange _defaultExchange;

    private static final Logger _logger = Logger.getLogger(BindingFactory.class);
    private final ConcurrentHashMap<BindingImpl, BindingImpl> _bindings = new ConcurrentHashMap<BindingImpl, BindingImpl>();


    public BindingFactory(final VirtualHost vhost) {
        this(vhost, vhost.getExchangeRegistry().getDefaultExchange());
    }

    public BindingFactory(final DurableConfigurationStore.Source configSource, final Exchange defaultExchange) {
        _configSource = configSource;
        _defaultExchange = defaultExchange;
        if (configSource instanceof VirtualHost) {
            _virtualHost = (VirtualHost) configSource;
        } else {
            _virtualHost = null;
        }
    }

    public VirtualHost getVirtualHost() {
        return _virtualHost;
    }


    private final class BindingImpl extends Binding implements AMQQueue.Task, Exchange.Task, BindingConfig {
        private final BindingLogSubject _logSubject;
        //TODO : persist creation time
        private long _createTime = System.currentTimeMillis();

        private BindingImpl(String bindingKey, final AMQQueue queue, final Exchange exchange, final Map<String, Object> arguments) {
            super(queue.getVirtualHost().getConfigStore().createId(), bindingKey, queue, exchange, arguments);
            _logSubject = new BindingLogSubject(bindingKey, exchange, queue);

        }


        public void doTask(final AMQQueue queue) throws AMQException {
            removeBinding(this);
        }

        public void onClose(final Exchange exchange) throws AMQSecurityException, AMQInternalException {
            removeBinding(this);
        }

        void logCreation() {
            CurrentActor.get().message(_logSubject, BindingMessages.CREATED(String.valueOf(getArguments()), getArguments() != null && !getArguments().isEmpty()));
        }

        void logDestruction() {
            CurrentActor.get().message(_logSubject, BindingMessages.DELETED());
        }

        public String getOrigin() {
            return (String) getArguments().get("qpid.fed.origin");
        }

        public long getCreateTime() {
            return _createTime;
        }

        public BindingConfigType getConfigType() {
            return BindingConfigType.getInstance();
        }

        public ConfiguredObject getParent() {
            return _virtualHost;
        }

        public boolean isDurable() {
            return getQueue().isDurable() && getExchange().isDurable();
        }

    }

    public boolean addBinding(String bindingKey, AMQQueue queue, Exchange exchange, Map<String, Object> arguments) throws AMQSecurityException, AMQInternalException {
        //    CassandraMessageStore.getInstance().addBinding(exchange,queue,bindingKey);
        return makeBinding(bindingKey, queue, exchange, arguments, false, false, true);
    }

    public boolean replaceBinding(final String bindingKey,
                                  final AMQQueue queue,
                                  final Exchange exchange,
                                  final Map<String, Object> arguments) throws AMQSecurityException, AMQInternalException {
        return makeBinding(bindingKey, queue, exchange, arguments, false, true, true);
    }

    private synchronized boolean makeBinding(String bindingKey, AMQQueue queue, Exchange exchange, Map<String, Object> arguments, boolean restore, boolean force, boolean isLocal) throws AMQSecurityException, AMQInternalException {
        assert queue != null;
        if (bindingKey == null) {
            bindingKey = "";
        }
        if (exchange == null) {
            exchange = _defaultExchange;
        }
        if (arguments == null) {
            arguments = Collections.emptyMap();
        }

        /**
         * We should check cluster-wide if the queue has a binding with a key A and now it is going to bind with a different key
         * (say B) we should not allow it. It is enough to check with local bindings as whenever we create a binding in cluster
         * it is syned and created locally. We test it before binding authorization to avoid
         * creating registry entries.
         */
        if (exchange.getName().equalsIgnoreCase("amq.topic") && queue.isDurable()) {
            if (checkIfQueueHasBoundToDifferentTopic(bindingKey, queue)) {
                throw new AMQInternalException("An Exclusive Bindings already exists for different topic. Not permitted.");
            }
        }

        BindingImpl binding = new BindingImpl(bindingKey, queue, exchange, arguments);
        BindingImpl existingMapping = _bindings.putIfAbsent(binding, binding);
        if (existingMapping == null || force) {
            if (existingMapping != null) {
                //TODO - we should not remove the existing binding
                removeBinding(existingMapping);
            }

            if (_logger.isDebugEnabled()) {
                _logger.debug("bindingKey: " + bindingKey + ", queue: " + queue + ", exchange: " + exchange);
            }

            // Prevent creating topics, if topic name is invalid
            if (exchange.getType() == TopicExchange.TYPE) {
                if (!NameValidationUtils.isValidTopicName(bindingKey)) {
                    _virtualHost.getQueueRegistry().unregisterQueue(queue.getNameShortString());
                    throw new AMQInternalException("\nTopic name: "
                            + NameValidationUtils.getNameWithoutTenantDomain(bindingKey)
                            + " can contain only alphanumeric characters and star(*) "
                            + "delimited by dots. Names can ends with any of these or, with '#' ");
                }
            }

            //Perform ACLs ONLY after removing/updating any existing bindings.
            if (!getVirtualHost().getSecurityManager().authoriseBind(exchange, queue, new AMQShortString(bindingKey))) {
                throw new AMQSecurityException("Permission denied: binding " + bindingKey);
            }

            //save only durable bindings
            if (binding.isDurable() && !restore) {
                _configSource.getDurableConfigurationStore().bindQueue
                        (exchange, new AMQShortString(bindingKey), queue, FieldTable.convertToFieldTable(arguments));

                if(isLocal) {
                    //tell Andes kernel to create binding
                    QpidAndesBridge.createBinding(exchange, new AMQShortString(bindingKey), queue);
                }
            }

            queue.addQueueDeleteTask(binding);
            exchange.addCloseTask(binding);
            try {
                queue.addBinding(binding);
                exchange.addBinding(binding);
                getConfigStore().addConfiguredObject(binding);
                binding.logCreation();
            } catch (AndesException e) {
                _bindings.remove(binding);
                throw new AMQInternalException("Not permitted. Binding Already exists for this queue", e);
            }

            return true;
        } else {
            return false;
        }
    }

    private boolean checkIfQueueHasBoundToDifferentTopic(String bindingKey, AMQQueue queue) {
        boolean isAlreadyABindingExistsForDifferentKey = false;
        List<Binding> bindingList = queue.getBindings();
        for (Binding b : bindingList) {
            if (b.getExchange().getName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME) && !b.getBindingKey().equals(bindingKey)) {
                isAlreadyABindingExistsForDifferentKey = true;
                break;
            }
        }
        return isAlreadyABindingExistsForDifferentKey;
    }

    private ConfigStore getConfigStore() {
        return getVirtualHost().getConfigStore();
    }

    public void restoreBinding(final String bindingKey, final AMQQueue queue,
                               final Exchange exchange, final Map<String, Object> argumentMap)
            throws AMQSecurityException, AMQInternalException {
        makeBinding(bindingKey, queue, exchange, argumentMap, true, false, false);
    }

    public void removeBinding(final Binding b) throws AMQSecurityException, AMQInternalException {
        removeBinding(b.getBindingKey(), b.getQueue(), b.getExchange(), b.getArguments(), true);
    }

    public synchronized Binding removeBinding(String bindingKey, AMQQueue queue, Exchange exchange,
                                 Map<String, Object> arguments, boolean isLocal) throws AMQSecurityException, AMQInternalException {
        assert queue != null;
        if (bindingKey == null) {
            bindingKey = "";
        }
        if (exchange == null) {
            exchange = _defaultExchange;
        }
        if (arguments == null) {
            arguments = Collections.emptyMap();
        }

        // Check access
        if (!getVirtualHost().getSecurityManager().authoriseUnbind(exchange,
                new AMQShortString(bindingKey), queue)) {
            throw new AMQSecurityException("Permission denied: binding " + bindingKey);
        }

        BindingImpl binding = _bindings.remove(new BindingImpl(bindingKey, queue, exchange, arguments));

        try {
            if (binding != null) {
                if (binding.isDurable()) {
                    if(isLocal) {
                        //inform andes kernel to remove binding.
                        QpidAndesBridge.removeBinding(binding);
                    }
                    _configSource.getDurableConfigurationStore().unbindQueue(exchange,
                            new AMQShortString(bindingKey),
                            queue,
                            FieldTable.convertToFieldTable(arguments));
                }

                exchange.removeBinding(binding);
                queue.removeBinding(binding);
                exchange.removeCloseTask(binding);
                queue.removeQueueDeleteTask(binding);

                binding.logDestruction();
                getConfigStore().removeConfiguredObject(binding);
            }
            return binding;
        } catch (AndesException e) {
            throw new AMQInternalException("Error while removing binding.", e);
        }
    }

    public Binding getBinding(String bindingKey, AMQQueue queue, Exchange exchange, Map<String, Object> arguments) {
        assert queue != null;
        if (bindingKey == null) {
            bindingKey = "";
        }
        if (exchange == null) {
            exchange = _defaultExchange;
        }
        if (arguments == null) {
            arguments = Collections.emptyMap();
        }

        BindingImpl b = new BindingImpl(bindingKey, queue, exchange, arguments);
        return _bindings.get(b);
    }
}
