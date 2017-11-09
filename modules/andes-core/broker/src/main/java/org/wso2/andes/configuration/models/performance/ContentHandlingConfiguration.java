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
package org.wso2.andes.configuration.models.performance;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for message content handling related configs
 */
@Configuration(description = "Message content handling related configurations")
public class ContentHandlingConfiguration {

    @Element(description = "Within Andes there are content chunk handlers which convert incoming large content\n"
            + "chunks into max content chunk size allowed by Andes core. These handlers run in parallel\n"
            + "converting large content chunks to smaller chunks.If the protocol specific content chunk size is"
            + " different from the max chunk size allowed by Andes core and there are significant number of large"
            + " messages published, then having multiple handlers will increase performance")
    private int contentChunkHandlerCount = 3;

    @Element(description = "Number of message acknowledgement handlers to process acknowledgements concurrently.\n"
            + "These acknowledgement handlers will batch and process acknowledgements.")
    private int maxContentChunkSize = 65500;

    @Element(description = "Number of message acknowledgement handlers to process acknowledgements concurrently.\n"
            + "These acknowledgement handlers will batch and process acknowledgements.")
    private boolean allowCompression = true;

    @Element(description = "Number of message acknowledgement handlers to process acknowledgements concurrently.\n"
            + "These acknowledgement handlers will batch and process acknowledgements.")
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
