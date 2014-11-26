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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import java.util.ArrayList;
import java.util.List;

/**
 * AndesMessage represents the messages handled by Andes.
 * Metadata and content chunks are referred from this
 */
public class AndesMessage {

    /**
     * Metadata for AndesMessage
     */
    private AndesMessageMetadata metadata;

    /**
     * Content is divided into chunks and referred from this list
     */
    private List<AndesMessagePart> contentChunkList;

    public AndesMessage(AndesMessageMetadata metadata) {
        this.metadata = metadata;
        contentChunkList = new ArrayList<AndesMessagePart>();
    }


    public AndesMessageMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(AndesMessageMetadata metadata) {
        this.metadata = metadata;
    }

    public List<AndesMessagePart> getContentChunkList() {
        return contentChunkList;
    }

    public void addMessagePart(AndesMessagePart messagePart) {
        contentChunkList.add(messagePart);
    }
}
