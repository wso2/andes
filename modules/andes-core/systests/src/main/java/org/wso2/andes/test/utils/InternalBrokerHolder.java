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
package org.wso2.andes.test.utils;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.Broker;

public class InternalBrokerHolder implements BrokerHolder
{
    private static final Logger LOGGER = Logger.getLogger(InternalBrokerHolder.class);
    private final Broker _broker;

    public InternalBrokerHolder(final Broker broker)
    {
        if(broker == null)
        {
            throw new IllegalArgumentException("Broker must not be null");
        }

        _broker = broker;
    }

    public void shutdown()
    {
        LOGGER.info("Shutting down Broker instance");

        try {
            _broker.shutdown();
        } catch (AndesException e) {
            LOGGER.error("Error occurred while shutting down", e);
        }

        LOGGER.info("Broker instance shutdown");
    }

}
