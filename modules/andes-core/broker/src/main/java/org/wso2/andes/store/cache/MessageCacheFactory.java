/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store.cache;

import org.wso2.andes.configuration.BrokerConfigurationService;

/**
 * Factory to create a {@link AndesMessageCache} based on the configurations in broker.xml
 *
 */
public class MessageCacheFactory {

    /***
     * Create a {@link AndesMessageCache} with the configurations passed.
     * currently it will either returns a {@link GuavaBasedMessageCacheImpl} or
     * {@link DisabledMessageCacheImpl} if cacheSize is configured as '0' in
     * broker.xml
     * 
     * @param connectionProperties
     *            configuration options
     * @return a {@link AndesMessageCache}
     */
    public AndesMessageCache createMessageCache() {

        int cacheSizeInMegaBytes = BrokerConfigurationService.getInstance().getBrokerConfiguration().getPersistence()
                .getCache().getSize();

        AndesMessageCache cache = null;

        if (cacheSizeInMegaBytes <= 0) {
            cache = new DisabledMessageCacheImpl();
        } else {
            cache = new GuavaBasedMessageCacheImpl();
        }

        return cache;
    }

    /***
     * Create a {@link AndesMetadataCache } with the configurations passed.
     * currently it will either returns a {@link GuavaBasedMetadataCacheImpl} or
     * {@link DisabledMetadataCacheImpl} if cacheSize is configured as '0' in
     * broker.xml
     * 
     * @param connectionProperties
     *            configuration options
     * @return a {@link AndesMessageCache}
     */
    public AndesMessageCache createMetaDataCache() {

        int cacheSizeInMegaBytes = BrokerConfigurationService.getInstance().getBrokerConfiguration().getPersistence()
                .getCache().getSize();

        AndesMessageCache cache = null;

        if (cacheSizeInMegaBytes <= 0) {
            cache = new DisabledMessageCacheImpl();
        } else {
            cache = new GuavaBasedMessageCacheImpl();
        }

        return cache;
    }

}
