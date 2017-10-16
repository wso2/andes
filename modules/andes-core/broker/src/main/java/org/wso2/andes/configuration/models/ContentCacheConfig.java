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
 * Configuration model for Content caching.
 */
@Configuration(description = "Content caching configuration.")
public class ContentCacheConfig {
    @Element(description = "Specify the maximum number of entries the cache may contain.")
    private int maximumSize = 100;

    @Element(description = "Specify the time in seconds that each entry should be\n"
            + "automatically removed from the cache after the entry's creation.")
    private int expiryTime = 120;

    public int getMaximumSize() {
        return maximumSize;
    }

    public int getExpiryTime() {
        return expiryTime;
    }
}