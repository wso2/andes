/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.amqp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.store.ForwardingStoredMessage;
import org.wso2.andes.server.store.StorableMessageMetaData;
import org.wso2.andes.server.store.StoredMessage;

import java.nio.ByteBuffer;

/**
 * This class works as a wrapper for AndesContent for Qpid
 *
 * @param <T> StorableMessageMetaData
 *
 */
public class QpidStoredMessage<T extends StorableMessageMetaData> extends ForwardingStoredMessage<T> {
    private static final Log log = LogFactory.getLog(QpidStoredMessage.class);

    /**
     * Provide access to message content
     */
    private final AndesContent content;

    public QpidStoredMessage(StoredMessage<T> s, AndesContent content) {
        super(s);
        this.content = content;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getContent(int offsetInMessage, ByteBuffer dst) {
        int bytesWrittenToBuffer = 0;

        try {
            bytesWrittenToBuffer = content.putContent(offsetInMessage, dst);
        } catch (AndesException e) {
            log.error("Error while getting message content chunk offset " + offsetInMessage, e);
        }

        return bytesWrittenToBuffer;
    }
}
