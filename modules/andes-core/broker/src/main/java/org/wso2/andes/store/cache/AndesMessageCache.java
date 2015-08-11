package org.wso2.andes.store.cache;

import java.util.List;
import java.util.Map;

import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessagePart;

/**
 * Keeps track of messages that were stored (from this mb instance).
 * Intention is to eliminate the need to go to database to read
 * messages/metadata if they are inserted from
 * this node. (intension is to reduce the strain on database and improves
 * performance)
 */
public interface AndesMessageCache {

    /**
     * Add the given message to cache
     * 
     * @param message
     *            the message
     */
    abstract void addToCache(AndesMessage message);

    /**
     * Removes given list of messages/ids from the cache
     * 
     * @param messagesToRemove
     *            list of message Ids
     */
    abstract void removeFromCache(List<Long> messagesToRemove);

    /**
     * Returns a message if found in cache
     * 
     * @param messageId
     *            message id to look up
     * @return a message or null (if not found)
     */
    abstract AndesMessage getMessageFromCache(long messageId);

    /**
     * Get the list of messages found from the cache.
     * <b> This method modifies the provided messageIDList </b>
     * 
     * @param messageIDList
     *            message id to be found in cache.
     * @param contentList
     *            the list the fill
     */
    abstract void fillContentFromCache(List<Long> messageIDList, Map<Long, List<AndesMessagePart>> contentList);

    /**
     * Return a {@link AndesMessagePart} from the cache.
     * 
     * @param messageId
     *            id of the massage
     * @param offsetValue
     *            the offset value
     * @return a {@link AndesMessagePart} if the message is found otherwise null
     */
    abstract AndesMessagePart getContentFromCache(long messageId, int offsetValue);

}