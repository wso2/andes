package org.wso2.andes.store.cache;

import java.util.List;
import java.util.Map;

import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessagePart;


/**
 * Implementation which doesn't cache any message. used when user disabled the
 * message cache via configuration.
 */
public class DisabledMessageCache implements AndesMessageCache {

    /**
     * {@inheritDoc}
     */
    @Override
    public void addToCache(AndesMessage message) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeFromCache(List<Long> messagesToRemove) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessage getMessageFromCache(long messageId) {

        return null;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fillContentFromCache(List<Long> messageIDList, Map<Long, List<AndesMessagePart>> contentList) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContentFromCache(long messageId, int offsetValue) {
        return null;

    }

}
