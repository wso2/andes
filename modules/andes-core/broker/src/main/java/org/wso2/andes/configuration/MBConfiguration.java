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
 * under the License. and limitations under the License.
 */

package org.wso2.andes.configuration;

import org.wso2.andes.kernel.AndesContext;

/**
 * This class will have all the configurations related to the Message Broker. Configurations
 * related to different sub categories will be ordered accordingly within this class.
 */
public class MBConfiguration {

    /**
     * Property to determine whether the message storing should be synchronous or asynchronous
     */
    private static final String PROP_ASYNC_STORING = "asyncStoring";

    /**
     * read from configurations determine is asynchronous storing is enabled
     *
     * @return true if asynchronous storing is enabled and false otherwise
     */
    public static boolean isAsyncStoringEnabled() {
        VirtualHostsConfiguration virtualHostsConfiguration = AndesContext.getInstance()
                .getVirtualHostsConfiguration();
        String isAsyncString = virtualHostsConfiguration.getMessageStoreProperties()
                .getProperty(MBConfiguration.PROP_ASYNC_STORING);
        if (isAsyncString.isEmpty()) { // if value is not set
            isAsyncString = "true"; // default to true
        }
        return Boolean.parseBoolean(isAsyncString);
    }
}
