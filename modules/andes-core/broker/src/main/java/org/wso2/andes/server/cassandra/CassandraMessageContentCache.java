/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.cassandra;


import org.wso2.andes.server.ClusterResourceHolder;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public class CassandraMessageContentCache {


    private final int cacheSize = ClusterResourceHolder.getInstance().
            getClusterConfiguration().getMessageReadCacheSize();

    LinkedHashMap<String, byte[]> topicContentCache = new LinkedHashMap<String, byte[]>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
            return this.size() > cacheSize;
        }
    };

    private String createKey(String messageId, int offset) {

        return messageId + ":" + offset;

    }

    public int getContent(String messageId, int offsetValue, ByteBuffer dst) {
        int written = 0;
        int chunkSize = 65534;
        byte[] content = null;
        String keyToGet = createKey(messageId, offsetValue);
        content = (topicContentCache.get(keyToGet));
        if (content != null) {
            if (offsetValue == 0) {
                final int size = (int) content.length;
                int posInArray = offsetValue + written - offsetValue;
                int count = size - posInArray;
                if (count > dst.remaining()) {
                    count = dst.remaining();
                }
                dst.put(content, 0, count);
                written = count;
            } else {
                int k = offsetValue / chunkSize;
                final int size = (int) content.length;
                int posInArray = offsetValue - (k * chunkSize);
                int count = size - posInArray;
                if (count > dst.remaining()) {
                    count = dst.remaining();
                }

                dst.put(content, posInArray, count);

                written += count;
            }
        }

        return written;
    }

    public synchronized void addEntryToCache(String messageId, int offsetValue, byte[] cacheVal) {
        String key = createKey(messageId, offsetValue);
        topicContentCache.put(key, cacheVal);
    }
}
