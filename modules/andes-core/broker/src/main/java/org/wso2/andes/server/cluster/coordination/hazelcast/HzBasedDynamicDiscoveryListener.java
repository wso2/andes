/*
* Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.cluster.DiscoveryInformation;

/**
 * The following listener will receive notification through {@link com.hazelcast.core.ITopic} to write node details
 * to database.
 */
public class HzBasedDynamicDiscoveryListener implements MessageListener {


    private static Log log = LogFactory.getLog(HzBasedDynamicDiscoveryListener.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void onMessage(Message message) {

        AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();
        try {
            DiscoveryInformation.setTransportDataList(andesContextStore.getAllTransportDetails());
        } catch (AndesException e) {
            log.info("Error occurred while on message ",e);
        }
    }
}
