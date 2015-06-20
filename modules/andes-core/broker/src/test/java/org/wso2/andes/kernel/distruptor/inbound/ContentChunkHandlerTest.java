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

package org.wso2.andes.kernel.distruptor.inbound;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.wso2.andes.kernel.AndesMessagePart;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link org.wso2.andes.kernel.distruptor.inbound.ContentChunkHandlerTest}
 * This class tests for the functionality of the class for different content chunk sizes
 */
@RunWith(Parameterized.class)
public class ContentChunkHandlerTest {

    private int maxChunkSize;
    private int originalChunkSize;
    private int originalChunkCount;
    private int messageId;
    private ContentChunkHandler contentChunkHandler;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {65500, 65500, 16, 58}, // same size
                {65500, 65534, 16, 95}, // original chunk larger than max chunk size
                {15000, 512, 30, 25 },  // chunk size smaller than max chunk size
                {65534, 450, 10, 5698}, // total length smaller than max chunk size
                {800, 252525, 4, 6982}  // original chunk size is a multiple of max chunk size
        });
    }

    public ContentChunkHandlerTest(int maxChunkSize, int originalChunkSize, int originalChunkCount, int messageId) {
        this.maxChunkSize = maxChunkSize;
        this.originalChunkSize = originalChunkSize;
        this.originalChunkCount = originalChunkCount;
        this.messageId = messageId;
        contentChunkHandler = new ContentChunkHandler(maxChunkSize);
    }

    /**
     * Test content resize logic
     * Content, message id, offset and content length is tested
     */
    @Test
    public void testResizeChunks() {

        List<AndesMessagePart> originalChunks = new ArrayList<>(originalChunkCount);

        StringBuilder contentBuilder = new StringBuilder(originalChunkCount * originalChunkSize);

        int offset = 0;
        for (int i = 0; i < originalChunkCount; i++) {
            AndesMessagePart part = new AndesMessagePart();
            part.setMessageID(messageId);

            StringBuilder contentPartBuilder = new StringBuilder(originalChunkSize);
            for (int j = 0; j < originalChunkSize; j++) {
                contentPartBuilder.append((int)(Math.random() * 10)); // Should have a single character.
            }
            String contentPart = contentPartBuilder.toString();
            contentBuilder.append(contentPart);

            part.setData(contentPart.getBytes());
            part.setOffSet(offset);
            part.setDataLength(originalChunkSize);
            originalChunks.add(part);
            offset = offset + originalChunkSize;
        }

        String content = contentBuilder.toString();

        List<AndesMessagePart> resultList =
                contentChunkHandler.resizeChunks(originalChunks, originalChunkSize * originalChunkCount);

        int numOfFullChunks = (originalChunkSize * originalChunkCount) / maxChunkSize;
        offset = 0;
        contentBuilder = new StringBuilder(originalChunkCount * originalChunkSize);
        for (int i = 0; i < numOfFullChunks; i++) {
            AndesMessagePart messagePart = resultList.get(i);
            assertEquals("Chunk size mismatch", maxChunkSize, messagePart.getDataLength());
            assertEquals("Incorrect message id", messageId, messagePart.getMessageID());
            assertEquals("Incorrect offset", offset, messagePart.getOffSet());
            contentBuilder.append(new String(messagePart.getData()));
            offset = offset + maxChunkSize;
        }

        int remainingLength = (originalChunkSize * originalChunkCount) - (numOfFullChunks * maxChunkSize);

        if (remainingLength != 0) {
            AndesMessagePart messagePart = resultList.get(numOfFullChunks);
            assertEquals("Chunk size mismatch", remainingLength, messagePart.getDataLength());
            assertEquals("Incorrect message id", messageId, messagePart.getMessageID());
            assertEquals("Incorrect offset", offset, messagePart.getOffSet());
            contentBuilder.append(new String(messagePart.getData()));
        }

        assertEquals("Content mismatch", content, contentBuilder.toString());
    }
}
