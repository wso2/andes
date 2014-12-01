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

package org.wso2.andes.kernel.distrupter;

import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;

import com.lmax.disruptor.EventFactory;

/**
 * Wrapper class of cassandra data publish to disruptor
 *
 */
public class CassandraDataEvent {

    private boolean isPart = false;
    private AndesMessageMetadata metadata;
    private AndesMessagePart messagePart;

    public static class CassandraDEventFactory implements EventFactory<CassandraDataEvent> {
        @Override
        public CassandraDataEvent newInstance() {
            return new CassandraDataEvent();
        }
    }

    public static EventFactory<CassandraDataEvent> getFactory() {
        return new CassandraDEventFactory();
    }

    public boolean isPart() {
        return isPart;
    }

    public void setPart(boolean isPart) {
        this.isPart = isPart;
    }

    public AndesMessageMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(AndesMessageMetadata metadata) {
        this.metadata = metadata;
    }

    public AndesMessagePart getMessagePart() {
        return messagePart;
    }

    public void setMessagePart(AndesMessagePart messagePart) {
        this.messagePart = messagePart;
    }
}
