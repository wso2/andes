/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * under the License.
 */

package org.wso2.andes.store;


/**
 * Defines contractual obligations for any store which allows its users to know
 * about its operational status
 */
public interface HealthAwareStore {

    /**
     * Determine whether connection is healthy
     * 
     * @param testString
     *            a String value to write to database.
     * @param testTime
     *            a time value ( ideally the current time)
     * @return true of message store is operational.
     */
    boolean isOperational(String testString, long testTime);
}
