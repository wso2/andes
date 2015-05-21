/*
 *
 *  * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *  *
 *  * WSO2 Inc. licenses this file to you under the Apache License,
 *  * Version 2.0 (the "License"); you may not use this file except
 *  * in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.wso2.andes.amqp;

import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesSubscription;

import java.util.List;

public class AMQPMessage extends AndesMessage {

    public AMQPMessage(AndesMessageMetadata metadata) {
        super(metadata);
    }

    @Override
    public AndesMessageMetadata getMetadata() {
        return super.getMetadata();
    }

    @Override
    public void setMetadata(AndesMessageMetadata metadata) {
        super.setMetadata(metadata);
    }

    @Override
    public List<AndesMessagePart> getContentChunkList() {
        return super.getContentChunkList();
    }

    @Override
    public void addMessagePart(AndesMessagePart messagePart) {
        super.addMessagePart(messagePart);
    }

    @Override
    public boolean isDelivarable(AndesSubscription subscription) {
        return super.isDelivarable(subscription);
    }
}
