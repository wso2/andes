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

package org.wso2.andes.kernel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class keep track of AMQP specific
 * artifacts and store them persistently
 */
public class AMQPConstructStore {

    /**
     * Reference to AndesContextStore to manage exchanges/bindings and queues in persistence storage 
     */
    private AndesContextStore andesContextStore;

    //keeps bindings <exchange>,<queue,binding>
    private Map<String, Map<String, AndesBinding>> andesBindings = new HashMap<>();


    public AMQPConstructStore(AndesContextStore contextStore) throws AndesException {
        this.andesContextStore = contextStore;
    }


    /**
     * store binding
     *
     * @param binding binding to be stored
     * @param isLocal is this a local change
     * @throws AndesException
     */
    public void addBinding(AndesBinding binding, boolean isLocal) throws AndesException {
        String messageRouterName = binding.getMessageRouterName();
        String boundQueueName = binding.getBoundQueue().getName();
        if (isLocal) {
            andesContextStore.storeBindingInformation
                    (messageRouterName, boundQueueName, binding.encodeAsString());
        }
        if (andesBindings.get(messageRouterName) != null) {
            (andesBindings.get(messageRouterName)).put(boundQueueName, binding);
        } else {
            Map<String, AndesBinding> tempBindingMap = new HashMap<>();
            tempBindingMap.put(boundQueueName, binding);
            andesBindings.put(messageRouterName, tempBindingMap);
        }
    }

    /**
     * remove binding
     *
     * @param exchangeName name of the exchange name of binding
     * @param queueName    name of the queue binding carries
     * @param isLocal      is this a local change
     * @return  AndesBinding binding removed
     * @throws AndesException
     */
    public AndesBinding removeBinding(String exchangeName, String queueName, boolean isLocal) throws AndesException {
        if (isLocal) {
            andesContextStore.deleteBindingInformation(exchangeName, queueName);
        }
        AndesBinding removedBinding = null;
        if ((andesBindings.get(exchangeName)).get(queueName) != null) {
            removedBinding = (andesBindings.get(exchangeName)).remove(queueName);
        }
        if (andesBindings.get(exchangeName).isEmpty()) {
            andesBindings.remove(exchangeName);
        }
        return removedBinding;
    }

    /**
     * get bindings belonging to an exchange
     *
     * @param exchange name of exchange
     * @return a list of bindings
     * @throws AndesException
     */
    public List<AndesBinding> getBindingsForExchange(String exchange) throws AndesException {
        List<AndesBinding> bindings = new ArrayList<>();
        if (andesBindings.get(exchange) != null) {
            bindings.addAll((andesBindings.get(exchange)).values());
        }
        return bindings;
    }

    /**
     * Remove all the bindings for queue. A queue can bind to multiple message routers
     *
     * @param queueName name of the queue
     * @return A list of bindings stored
     * @throws AndesException
     */
    public List<AndesBinding> removeAllBindingsForQueue(String queueName) throws AndesException {
        List<AndesBinding> bindings = new ArrayList<>();
        for (Map<String, AndesBinding> queueBindingMap : andesBindings.values()) {
            queueBindingMap.remove(queueName);
        }
        return bindings;
    }

}
