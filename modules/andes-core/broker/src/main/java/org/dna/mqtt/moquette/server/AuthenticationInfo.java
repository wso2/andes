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

import java.util.HashMap;
import java.util.Map;

/**
 * This is used as a response for authentication check in IAuthenticator
 */
public class AuthenticationInfo {

	/**
	 * this variable is used to check whether the client is authenticated.
	 */
	private boolean authenticated;
	private String username;
	private String tenantDomain;
	/**
	 * This is a property map that provide the flexibility to
	 * pass authentication result to the authorization.
	 * eg: key - "scopes"  value -"List<String> scopesList"
	 * 		key - token_expiry_time  value - long.
	 * 	similarly different properties can be passed for authorization.
	 */
	private Map<String, Object> properties = new HashMap<String, Object>();

	/**
	 * returns whether the client is authenticated
	 */
	public boolean isAuthenticated() {
		return authenticated;
	}

	public void setAuthenticated(boolean authenticated) {
		this.authenticated = authenticated;
	}

	/**
	 * returns the authenticated client username
	 */
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * return the authenticated client tenant domain
	 */
	public String getTenantDomain() {
		return tenantDomain;
	}

	public void setTenantDomain(String tenantDomain) {
		this.tenantDomain = tenantDomain;
	}

	/**
	 * return the authentication related properties
	 */
	public void setProperty(String key, Object value){
		properties.put(key, value);
	}

	public Map<String, Object> getProperties(){
		return properties;
	}

}
