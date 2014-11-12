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

import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;

/**
 * Will be used to handle meta data, when it comes to clone copies of it.
 */
public interface MetaDataHandler {
    /**
     * Will be called when its required to clone a copy of the meta information, this will be called during durable
     * subscriptions of AMQP and clean session in MQTT
     *
     * @param routingKey        the key wich described the destination of the message
     * @param buf               the buffer instance which will refer to the message meta information header
     * @param originalMeataData the meta data of the message
     * @param exchange          the exchange wich is defined through AMQP
     * @return the cloned copy of the message
     */
    public byte[] constructMetadata(String routingKey, ByteBuffer buf, StorableMessageMetaData originalMeataData,
                                    String exchange);
}
