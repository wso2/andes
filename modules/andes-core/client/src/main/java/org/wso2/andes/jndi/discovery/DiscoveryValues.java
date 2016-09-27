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
package org.wso2.andes.jndi.discovery;

/**
 * Define all constants for dynamic discovery.
 * This values use by client for initialize properties.
 */
public final class DiscoveryValues {

    /**
     * Key for getting trust store location.
     */
    public static final String TRUSTSTORE = "javax.net.ssl.trustStore";

    /**
     * Key for getting client username and password.
     */
    public static String AUTHENTICATION_CLIENT = "authentication_client";

    /**
     * Key for getting carbon Properties.
     */
    public static String CARBON_PROPERTIES = "carbon";

    /**
     * Key for getting failover properties.
     */
    public static String FAILOVER_PROPERTIES = "failover";

    /**
     * Key for getting connection Factory.
     */
    public static final String CF_NAME_PREFIX = "connectionfactory.";

    /**
     * Key for getting QPID connection factory.
     */
    public static final String CF_NAME = "qpidConnectionfactory";

    /**
     * Key for getting web service calling time interval.
     */
    public static final String PERIODIC_TIME_PREFIX = "web_service";

}
