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

import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;

import com.gs.collections.impl.list.mutable.primitive.LongArrayList;

/**
 * Defines a contractual obligations a cache which will have {@link AndesMessageMetadata}.
 * 
 */
public interface AndesMetadataCache {

    /**
     * Add the given message metadata to cache
     *
     * @param metadata the message
     */
    void addToCache(AndesMessage metadata);

    /**
     * Removes given list of metadata from the cache
     *
     * @param metadataToRemove list of message Ids
     */
    void removeFromCache(LongArrayList metadataToRemove);

    /**
     * Removes a metadata with a given id from the cache
     *
     * @param messagesToRemove list of message Ids
     */
    void removeFromCache(long metadataToRemove);

    /**
     * Returns a message if found in cache
     *
     * @param messageId message id to look up
     * @return a message or null (if not found)
     */
    AndesMessageMetadata getMessageFromCache(long messageId);

}
