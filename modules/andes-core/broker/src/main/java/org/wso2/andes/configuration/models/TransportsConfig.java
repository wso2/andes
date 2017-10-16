/*
 * Copyright (c) WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

/**
 * Configuration model for transports related configs.
 */
@Configuration(description = "You can enable/disable specific messaging transports in this section. By default all\n"
        + "transports are enabled. This section also allows you to customize the messaging flows used\n"
        + "within WSO2 MB. NOT performance related, but logic related.")
public class TransportsConfig {

    private AMQPConfigs amqp = new AMQPConfigs();

    public AMQPConfigs getAmqp() {
        return amqp;
    }
}