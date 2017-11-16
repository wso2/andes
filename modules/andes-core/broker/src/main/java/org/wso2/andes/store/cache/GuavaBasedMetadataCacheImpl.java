/*
*  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.BrokerConfigurationService;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.gs.collections.api.iterator.MutableLongIterator;
import com.gs.collections.impl.list.mutable.primitive.LongArrayList;

public class GuavaBasedMetadataCacheImpl implements AndesMetadataCache {

    /**
     * By default cache values will be kept using strong/ordinary references.
     */
    private static final String CACHE_VALUE_REF_TYPE_STRONG = "strong";

    /**
     * Cache values can be kept using weak references.
     */
    private static final String CACHE_VALUE_REF_TYPE_WEAK = "weak";

    /**
     * Size of the cache is determined via configuration. For example cache can keep
     * 1GB 'worth of' message payloads (and its meta data) in the memory.
     */
    private final Cache<Long, AndesMessageMetadata> cache;

    private static final Logger log = Logger.getLogger(GuavaBasedMetadataCacheImpl.class);

    /**
     * Used to schedule a cache clean up task and print cache statistics ( used for
     * debugging perposes)
     */
    private ScheduledExecutorService maintenanceExecutor;

    /**
     * Flag indicating guava cache statistics should be printed on logs
     */
    private final boolean printStats;

    public GuavaBasedMetadataCacheImpl() {
        long cacheSizeInBytes = 1024L * 1024L * (BrokerConfigurationService.getInstance().getBrokerConfiguration()
                .getPersistence().getCache().getSize());

        int cacheConcurrency = BrokerConfigurationService.getInstance().getBrokerConfiguration().getPersistence()
                .getCache().getConcurrencyLevel();

        int cacheExpirySeconds = BrokerConfigurationService.getInstance().getBrokerConfiguration().getPersistence()
                .getCache().getExpirySeconds();

        String valueRefType = BrokerConfigurationService.getInstance().getBrokerConfiguration().getPersistence()
                .getCache().getValueReferenceType();
        printStats = BrokerConfigurationService.getInstance().getBrokerConfiguration().getPersistence().getCache()
                .isPrintStats();

        CacheBuilder<Long, AndesMessageMetadata> builder = CacheBuilder.newBuilder().concurrencyLevel(cacheConcurrency)
                .expireAfterAccess(cacheExpirySeconds, TimeUnit.SECONDS).maximumWeight(cacheSizeInBytes)
                .weigher(new Weigher<Long, AndesMessageMetadata>() {
                    @Override
                    public int weigh(Long l, AndesMessageMetadata m) {
                        return m.getStorableSize();
                    }
                });

        if (printStats) {
            builder = builder.recordStats();
        }

        if (CACHE_VALUE_REF_TYPE_WEAK.equalsIgnoreCase(valueRefType)) {
            builder = builder.weakValues();
        }

        this.cache = builder.build();

        maintenanceExecutor = Executors.newSingleThreadScheduledExecutor();

        maintenanceExecutor.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                cache.cleanUp();

                if (printStats) {
                    log.info("cache stats:" + cache.stats().toString());
                }

            }
        }, 2, 2, TimeUnit.MINUTES);

    }

    @Override
    public void addToCache(AndesMessage message) {
        cache.put(message.getMetadata().getMessageID(), message.getMetadata());
    }

    @Override
    public void removeFromCache(LongArrayList messagesToRemove) {
        /* We need to remove the GS-collection from the dependencies therefore this will change) */
        ArrayList<Long> arrayList = new ArrayList<>();
        MutableLongIterator iterator = messagesToRemove.longIterator();
        while (iterator.hasNext()) {
            arrayList.add(iterator.next());
        }
        cache.invalidateAll(arrayList);
    
    }

    @Override
    public void removeFromCache(long messageToRemove) {

        cache.invalidate(messageToRemove);

    }

    @Override
    public AndesMessageMetadata getMessageFromCache(long messageId) {
        return cache.getIfPresent(messageId);
    }

}
