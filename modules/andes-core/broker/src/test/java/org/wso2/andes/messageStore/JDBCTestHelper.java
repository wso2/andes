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

package org.wso2.andes.messageStore;

import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;

import java.util.ArrayList;
import java.util.List;

public class JDBCTestHelper {

    protected static List<AndesMessageMetadata> getMetadataList(final String destQueueName, long firstMsgId, long lastMsgId) {
        return getMetadataList(destQueueName, firstMsgId, lastMsgId, 10000);
    }

    protected static List<AndesMessageMetadata> getMetadataList(final String destQueueName, long firstMsgId,
                                                                long lastMsgId, int expirationTime) {
        List<AndesMessageMetadata> lst = new ArrayList<AndesMessageMetadata>(10);
        for (long i = firstMsgId; i < lastMsgId; i++) {
            lst.add(getMetadata(i, destQueueName, expirationTime));
        }
        return lst;
    }
    /**
     *
     * @param qNameArray String array of queue names
     * @param divisor modulo division is done to select the queue name for each message while iterating from firstMsgId
     *                to lastMsgId
     * @param firstMsgId firstMsgId
     * @param lastMsgId lastMsgId
     * @return List of AndesMessageMetadata
     */
    protected static List<AndesMessageMetadata> getMetadataForMultipleQueues(final String[] qNameArray, int divisor,
                                                                             long firstMsgId, long lastMsgId) {
        List<AndesMessageMetadata> lst = new ArrayList<AndesMessageMetadata>(10);
        for (long i = firstMsgId; i < lastMsgId; i++) {
            lst.add(getMetadata(i, qNameArray[(int) i % divisor]));
        }
        return lst;
    }

    protected static AndesMessageMetadata getMetadata(long msgId, final String queueName) {
        return getMetadata(msgId, queueName, 10000);
    }

    protected static AndesMessageMetadata getMetadata(long msgId, final String queueName, int expirationTime) {
        AndesMessageMetadata md = new AndesMessageMetadata();
        md.setMessageID(msgId);
        md.setDestination(queueName);
        md.setMetadata(("\u0002:MessageID=" + msgId + ",persistent=false,Topic=false,Destination=" + queueName +
                ",Persistant=false,MessageContentLength=0").getBytes());
        md.setExpirationTime((msgId % 2 == 0) ? (System.currentTimeMillis() + expirationTime) : 0);
        return md;
    }

    protected static List<AndesMessagePart> getMessagePartList(long firstMsgId, long lastMsgId) {
        List<AndesMessagePart> list = new ArrayList<AndesMessagePart>();
        byte[] content = "test message".getBytes();
        byte[] content2 = "second part".getBytes();

        for (long i = firstMsgId; i < lastMsgId; i++) {
            AndesMessagePart p = new AndesMessagePart();
            p.setMessageID(i);
            p.setData(content);
            p.setDataLength(content.length);
            p.setOffSet(0);
            list.add(p);

            int offset = content.length;
            p = new AndesMessagePart();
            p.setMessageID(i);
            p.setData(content2);
            p.setDataLength(content2.length);
            p.setOffSet(offset);
            list.add(p);
        }
        return list;
    }
}
