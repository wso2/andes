/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.inbound;

import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.disruptor.compression.LZ4CompressionHelper;
import java.util.List;

/**
 * Compress the content of the message
 */
public class LZ4ContentCompressionStrategy implements ContentCompressionStrategy {

    /**
     * Used to get configuration values related to compression and used to compress message content
     */
    LZ4CompressionHelper lz4CompressionHelper;

    public LZ4ContentCompressionStrategy(LZ4CompressionHelper lz4CompressionHelper) {
        this.lz4CompressionHelper = lz4CompressionHelper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContentPartHolder ContentChunkStrategy(AndesMessage message) {

        return compressChunks(message);
    }


    /**
     * Compress the content of the message. Returns content chunk pair which contains compressed andes part list and
     * compressed content length.
     *
     * @param message Original AndesMessage before compress
     * @return Content chunk pair which contains andes message part list and content length.
     */
    ContentPartHolder compressChunks(AndesMessage message) {
        List<AndesMessagePart> partList = message.getContentChunkList();
        AndesMessageMetadata metadata = message.getMetadata();
        int contentLength = metadata.getMessageContentLength();
        int originalContentLength = contentLength;

        if (originalContentLength > lz4CompressionHelper.getContentCompressionThreshold()) {
            // Compress message
            AndesMessagePart compressedMessagePart = lz4CompressionHelper.getCompressedMessage(partList, originalContentLength);

            // Update metadata to indicate the message is a compressed one
            metadata.updateMetadata(true);
            message.setMetadata(metadata);

            contentLength = compressedMessagePart.getDataLength();

            partList.clear();
            partList.add(compressedMessagePart);
        }
        ContentPartHolder partHolder = new ContentPartHolder(partList, contentLength);

        return partHolder;
    }
}
