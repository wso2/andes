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
package org.wso2.andes.mqtt;

import org.wso2.andes.kernel.MetaDataHandler;
import org.wso2.andes.mqtt.utils.MQTTUtils;
import org.wso2.andes.server.store.StorableMessageMetaData;

import java.nio.ByteBuffer;

/**
 * Will be used to clone meta information of MQTT related topic messages recived
 */
public class MQTTMetaDataHandler implements MetaDataHandler {
    @Override
    public byte[] constructMetadata(String routingKey, ByteBuffer buf, StorableMessageMetaData originalMeataData,
                                    String exchange) {

        //For MQTT we just need to take a copy
        MQTTMessageMetaData metaInformation = (MQTTMessageMetaData) originalMeataData;
        //Will re-encode the bytes
        return MQTTUtils.encodeMetaInfo(MQTTUtils.MQTT_META_INFO, metaInformation.getMessageID(), false,
                metaInformation.getQosLevel(), routingKey, metaInformation.isPersistent(),
                metaInformation.getContentSize());

    }
}
