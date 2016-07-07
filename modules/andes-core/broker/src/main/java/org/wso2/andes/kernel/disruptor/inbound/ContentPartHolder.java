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

import org.wso2.andes.kernel.AndesMessagePart;
import java.util.List;

/**
 * This Pair object holds Andes message part list and content length.
 */
public class ContentPartHolder {

    private final List<AndesMessagePart> andesMessagePartList;
    private final int contentLength;

    public ContentPartHolder(List<AndesMessagePart> messagePartList, int contentLength) {
        this.andesMessagePartList = messagePartList;
        this.contentLength = contentLength;
    }


    /**
     * Returns andes message part list
     * @return andesMessagePartList
     */
    public List<AndesMessagePart> getAndesMessagePartList() {
        return andesMessagePartList;
    }

    /**
     * Returns content length of the andes message
     * @return content length
     */
    public int getContentLength() {
        return contentLength;
    }




}