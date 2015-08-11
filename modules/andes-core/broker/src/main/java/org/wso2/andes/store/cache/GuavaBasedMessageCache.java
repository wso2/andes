package org.wso2.andes.store.cache;

import java.util.List;
import java.util.Map;

import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessagePart;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

/**
 * Message cache implementation based on Guava {@link Cache}
 */
public class GuavaBasedMessageCache implements AndesMessageCache {

    /**
     * Size of the cache is determined via configuration. For example cache can
     * keep 1GB 'worth of' message payloads (and its meta data) in the memory.
     * 
     */
    private Cache<Long, AndesMessage> cache;

    public GuavaBasedMessageCache(long cacheSize) {

        this.cache =
                     CacheBuilder.newBuilder().concurrencyLevel(2)
                                 .maximumWeight(cacheSize)
                                 .weigher(new Weigher<Long, AndesMessage>() {
                                     @Override
                                     public int weigh(Long l, AndesMessage m) {

                                         return m.getMetadata().getMessageContentLength();
                                     }
                                 }).build();

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
            part = cachedMessage.getContentChunkList().get(offsetValue);
            if (null == part) {
                for (AndesMessagePart currentPart : cachedMessage.getContentChunkList()) {
                    if (currentPart.getOffSet() == offsetValue) {
                        part = currentPart;
                    }
                }
            }
        }
        return part;

    }

}
