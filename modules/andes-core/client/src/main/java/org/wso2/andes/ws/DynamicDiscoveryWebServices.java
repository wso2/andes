/*
* Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.andes.ws;

import java.util.List;

/**
 * Interface that contains dynamic discovery web services
 */
public interface DynamicDiscoveryWebServices {

    /**
     * Get the all live node IP address
     * @param initialAddress web service calling ip address
     * @param userName username for log to server
     * @param passWord password for log to server
     * @param mode SSL or default
     * @param trustStore security key for calling web service
     * @return list of IP address
     */
    List<String> getLocalDynamicDiscoveryDetails(String[] initialAddress, String userName, String passWord , String mode, String trustStore);


}
