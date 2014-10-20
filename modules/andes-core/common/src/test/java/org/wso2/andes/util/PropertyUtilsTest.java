/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.util;

import org.wso2.andes.configuration.PropertyException;
import org.wso2.andes.configuration.PropertyUtils;
import org.wso2.andes.test.utils.QpidTestCase;

public class PropertyUtilsTest extends QpidTestCase
{
    public void testSimpleExpansion() throws PropertyException
    {
        System.setProperty("banana", "fruity");
        String expandedProperty = PropertyUtils.replaceProperties("${banana}");
        assertEquals(expandedProperty, "fruity");
    }

    public void testDualExpansion() throws PropertyException
    {
        System.setProperty("banana", "fruity");
        System.setProperty("concrete", "horrible");
        String expandedProperty = PropertyUtils.replaceProperties("${banana}xyz${concrete}");
        assertEquals(expandedProperty, "fruityxyzhorrible");
    }

    public static junit.framework.Test suite()
    {
        return new junit.framework.TestSuite(PropertyUtilsTest.class);
    }
}
