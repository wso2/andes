/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.dna.mqtt.moquette.server;

import org.wso2.andes.configuration.enums.MQTTAuthoriztionPermissionLevel;
import org.wso2.andes.mqtt.MQTTAuthorizationSubject;

/**
 * check whether the user is authorized to either publish or subscribe to the topic
 */
public interface IAuthorizer {

	/**
	 *
	 * @param authorizationSubject passed from authentication.
	 * @param topic that the client is requesting for access.
	 * @param permissionLevel request permission level. This is either publish or subscribe.
	 * @return
	 */
	boolean isAuthorized(MQTTAuthorizationSubject authorizationSubject, String topic,
						 MQTTAuthoriztionPermissionLevel permissionLevel);
}
