/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.server.store;

import org.wso2.carbon.andes.core.AndesException;
import org.wso2.carbon.andes.core.internal.slot.Slot;

import java.nio.ByteBuffer;

/**
 * A StoredMessage which forwards all its method calls to another StoredMessage. Subclasses should override one or more
 * methods to modify the behavior of the backing StoredMessage as desired per the decorator pattern.
 *
 * @param <E>
 *         StorableMessageMetaData Type
 */
public class ForwardingStoredMessage<E extends StorableMessageMetaData> implements StoredMessage<E> {
    private final StoredMessage<E> s;

    public ForwardingStoredMessage(StoredMessage<E> s) {
        this.s = s;
    }

    @Override
    public E getMetaData() {
        return s.getMetaData();
    }

    @Override
    public long getMessageNumber() {
        return s.getMessageNumber();
    }

    @Override
    public void addContent(int offsetInMessage, ByteBuffer src) {
        s.addContent(offsetInMessage, src);
    }

    @Override
    public void duplicateMessageContent(long messageId, long messageIdOfClone) throws AndesException {
        s.duplicateMessageContent(messageId, messageIdOfClone);
    }

    @Override
    public int getContent(int offsetInMessage, ByteBuffer dst) {
        return s.getContent(offsetInMessage, dst);
    }

    @Override
    public TransactionLog.StoreFuture flushToStore() {
        return s.flushToStore();
    }

    @Override
    public void remove() {
        s.remove();
    }

    @Override
    public void setExchange(String exchange) {
        s.setExchange(exchange);
    }

    @Override
    public Slot getSlot() {
        return s.getSlot();
    }

    @Override
    public void setSlot(Slot slot) {
        s.setSlot(slot);
    }
}
