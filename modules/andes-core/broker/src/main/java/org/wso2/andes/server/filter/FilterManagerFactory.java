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
 * under the License.
 */
package org.wso2.andes.server.filter;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.common.AMQPFilterTypes;
import org.wso2.andes.framing.FieldTable;

import java.util.Map;


public class FilterManagerFactory
{
 
    private final static Logger _logger = Logger.getLogger(FilterManagerFactory.class);

    //fixme move to a common class so it can be refered to from client code.

    public static FilterManager createManager(FieldTable filters) throws AMQException
    {
        FilterManager manager = null;

        if (filters != null)
        {



            if(filters.containsKey(AMQPFilterTypes.JMS_SELECTOR.getValue()))
            {
                String selector =  filters.getString(AMQPFilterTypes.JMS_SELECTOR.getValue());

                if (selector != null && !selector.equals(""))
                {
                    manager = new SimpleFilterManager();
                    manager.add(new JMSSelectorFilter(selector));
                }

            }


        }
        else
        {
            _logger.debug("No Filters found.");
        }


        return manager;

    }
    
    public static FilterManager createManager(Map<String,Object> map) throws AMQException
    {
        return createManager(FieldTable.convertToFieldTable(map));
    }
}
