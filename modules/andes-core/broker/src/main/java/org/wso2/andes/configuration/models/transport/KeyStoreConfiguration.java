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
package org.wso2.andes.configuration.models.transport;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for keystore configuration in ssl connection
 */
@Configuration(description = "Broker keystore based config for ssl connection")
public class KeyStoreConfiguration {

    @Element(description = "Location path of keystore")
    private String location = "repository/resources/security/wso2carbon.jks";

    @Element(description = "Password of keystore")
    private String password = "wso2carbon";

    @Element(description = "Cert type of keystore")
    private String certType = "SunX509";

    public String getLocation() {
        return location;
    }

    public String getPassword() {
        return password;
    }

    public String getCertType() {
        return certType;
    }
}
