/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessagePart;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

/**
 * Message cache implementation based on Guava {@link Cache}
 */
public class GuavaBasedMessageCacheImpl implements AndesMessageCache {
    
    private static final Logger log = Logger.getLogger(GuavaBasedMessageCacheImpl.class);
    
    
    /**
     * By default cache values will be kept using strong/ordinary references.
     */
    private static final String CACHE_VALUE_REF_TYPE_STRONG = "strong";
    
    /**
     * Cache values can be kept using weak references.
     */
    private static final String CACHE_VALUE_REF_TYPE_WEAK = "weak";
    
    
    /**
     * Size of the cache is determined via configuration. For example cache can
     * keep 1GB 'worth of' message payloads (and its meta data) in the memory.
     * 
     */
    private final Cache<Long, AndesMessage> cache;

    /**
     * Used to schedule a cache clean up task and print cache statistics ( used for debugging perposes)
     */
    private ScheduledExecutorService maintenanceExecutor;
    
    /**
     * Flag indicating guava cache statistics should be printed on logs
     */
    private final boolean printStats;

    /**
     * Max chunk size of the stored content in Andes. This is needed to count the index
     * of a particular chunk in the chunk list when the offset is given.
     *
     */
    private static int DEFAULT_CONTENT_CHUNK_SIZE;
    
    
    
    public GuavaBasedMessageCacheImpl(){

        DEFAULT_CONTENT_CHUNK_SIZE = AndesConfigurationManager.readValue(AndesConfiguration
                                                                         .PERFORMANCE_TUNING_MAX_CONTENT_CHUNK_SIZE);

        long cacheSizeInBytes = 1024L * 1024L * ( (int) AndesConfigurationManager.readValue(AndesConfiguration
                                                                                            .PERSISTENCE_CACHE_SIZE));

        int cacheConcurrency =
                AndesConfigurationManager.readValue(AndesConfiguration.PERSISTENCE_CACHE_CONCURRENCY_LEVEL);

        int cacheExpirySeconds =
                AndesConfigurationManager.readValue(AndesConfiguration.PERSISTENCE_CACHE_EXPIRY_SECONDS);

        String valueRefType = AndesConfigurationManager.readValue(AndesConfiguration
                                                                  .PERSISTENCE_CACHE_VALUE_REFERENCE_TYPE);
        printStats = AndesConfigurationManager.readValue(AndesConfiguration.PERSISTENCE_CACHE_PRINT_STATS);

        CacheBuilder<Long, AndesMessage> builder = CacheBuilder.newBuilder().concurrencyLevel(cacheConcurrency)
                                                               .expireAfterAccess(cacheExpirySeconds, TimeUnit.SECONDS)
                                                               .maximumWeight(cacheSizeInBytes)
                                                               .weigher(new Weigher<Long, AndesMessage>() {
                                                                   @Override
                                                                   public int weigh(Long l, AndesMessage m) {
                                                                       return m.getMetadata().getMessageContentLength();
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
                
                if ( printStats){
                    log.info("cache stats:" + cache.stats().toString());
                }
                
                
            }
        }, 2, 2, TimeUnit.MINUTES);
        
    }

    /**
     * {@inheritDoc}
     * 
     * */
    @Override
    public void addToCache(AndesMessage message) {

        cache.put(message.getMetadata().getMessageID(), message);

    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public void removeFromCache(List<Long> messagesToRemove) {
        cache.invalidateAll(messagesToRemove);
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public void removeFromCache(long messageToRemove) {
        cache.invalidate(messageToRemove);
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public AndesMessage getMessageFromCache(long messageId) {

        return cache.getIfPresent(messageId);

    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public void fillContentFromCache(List<Long> messageIDList, Map<Long, List<AndesMessagePart>> contentList) {

        Map<Long, AndesMessage> fromCache = cache.getAllPresent(messageIDList);

        messageIDList.removeAll(fromCache.keySet());

        for (Map.Entry<Long, AndesMessage> cachedMessage : fromCache.entrySet()) {
            contentList.put(cachedMessage.getKey(), cachedMessage.getValue().getContentChunkList());
        }

    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public AndesMessagePart getContentFromCache(long messageId, int offsetValue) {
        AndesMessage cachedMessage = getMessageFromCache(messageId);
        AndesMessagePart part = null;
        if (null != cachedMessage) {
            //the offset comes as a multiple of DEFAULT_CONTENT_CHUNK_SIZE after being converted in method
            //'fillBufferFromContent' in {@link org.wso2.andes.amqp.AMQPUtils}
            //therefore, offsetValue / DEFAULT_CONTENT_CHUNK_SIZE gives the correct index of a particular chunk
            // in the content chunk list
            part = getMessageFromCache(messageId).getContentChunkList().get(offsetValue / DEFAULT_CONTENT_CHUNK_SIZE);
        }
        return part;
    }

}
