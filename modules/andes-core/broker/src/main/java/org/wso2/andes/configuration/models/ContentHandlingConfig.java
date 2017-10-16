/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except 
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.andes.configuration.models;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for Content handling config in Performance tuning section.
 */
@Configuration(description = "Content handling configurations.")
public class ContentHandlingConfig {

    @Element(description = "Within Andes there are content chunk handlers which convert incoming large content\n"
            + "            chunks into max content chunk size allowed by Andes core. These handlers run in parallel\n"
            + "            converting large content chunks to smaller chunks.\n"
            + "            If the protocol specific content chunk size is different from the max chunk size allowed\n"
            + "            by Andes core and there are significant number of large messages published, then having\n"
            + "            multiple handlers will increase performance.")
    private int contentChunkHandlerCount = 3;

    @Element(description = "Andes core will store message content chunks according to this chunk size. Different\n"
            + "            database will have limits and performance gains by tuning this parameter.\n"
            + "            For instance in MySQL the maximum table column size for content is less than 65534, which\n"
            + "            is the default chunk size of AMQP. By changing this parameter to a lesser value we can\n"
            + "            store large content chunks converted to smaller content chunks within the DB with this\n"
            + "            parameter.")
    private int maxContentChunkSize = 65500;

    @Element(description = "This is the configuration to allow compression of message contents, before store messages\n"
            + "            into the database.")
    private boolean allowCompression = false;

    @Element(description = "This is the configuration to change the value of the content compression threshold (in bytes).\n"
            + "            Message contents less than this value will not compress, even compression is enabled. The recommended\n"
            + "            message size of the smallest message before compression is 13bytes. Compress messages smaller than\n"
            + "            13bytes will expand the message size by 0.4%")
    private int contentCompressionThreshold = 1000;

    public int getContentChunkHandlerCount() {
        return contentChunkHandlerCount;
    }

    public int getMaxContentChunkSize() {
        return maxContentChunkSize;
    }

    public boolean isAllowCompression() {
        return allowCompression;
    }

    public int getContentCompressionThreshold() {
        return contentCompressionThreshold;
    }
}