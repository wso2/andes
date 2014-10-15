/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.test;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.MessageStoreManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MockPublisher implements Runnable {

    private MessageStoreManager messageStoreManager;
    private AtomicLong messageCount;
    private byte[] metadata = null;
    //todo size of the byte array 65534 chunk size
    private byte[] content = null;
    private int numberOfChunkPerMessage = 4;
    private static Log log = LogFactory.getLog(MockPublisher.class);


    public MockPublisher(MessageStoreManager messageStoreManager, AtomicLong messageCount) {
        this.messageStoreManager = messageStoreManager;
        this.messageCount = messageCount;
        metadata = "mock metadata".getBytes();
        content = "mock content".getBytes();
    }

    @Override
    public void run() {
        long messageId = messageCount.incrementAndGet();
        try {
            List<AndesMessagePart> messageParts = generateMessageContent(messageId);
            AndesMessageMetadata andesMessageMetadata = generateMetadata(messageId);
            List<AndesMessageMetadata> metadataList = new ArrayList<AndesMessageMetadata>();
            metadataList.add(andesMessageMetadata);
            messageStoreManager.storeMetaData(metadataList);
            System.out.println("store metadata ....");
        } catch (AndesException e) {
            log.error("Error while saving message to message store", e);
        }

    }

    private AndesMessageMetadata generateMetadata(long messageId) {
        AndesMessageMetadata andesMetadata = new AndesMessageMetadata(messageId, metadata, false);
        andesMetadata.setDestination("foo");
        return andesMetadata;
    }

    private List<AndesMessagePart> generateMessageContent(long messageId) {
        List<AndesMessagePart> messageContentParts = new ArrayList<AndesMessagePart>();
        for (int i = 0; i < numberOfChunkPerMessage; i++) {
            AndesMessagePart andesMessagePart = new AndesMessagePart();
            andesMessagePart.setMessageID(messageId);
            andesMessagePart.setData(content);
            andesMessagePart.setDataLength(content.length);
            andesMessagePart.setOffSet(i * content.length);
            messageContentParts.add(andesMessagePart);
        }
        return messageContentParts;
    }
}
