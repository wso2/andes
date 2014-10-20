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
import org.wso2.andes.AMQInvalidArgumentException;
import org.wso2.andes.server.filter.jms.selector.SelectorParser;
import org.wso2.andes.server.queue.Filterable;


public class JMSSelectorFilter implements MessageFilter
{
    private final static Logger _logger = org.apache.log4j.Logger.getLogger(JMSSelectorFilter.class);

    private String _selector;
    private BooleanExpression _matcher;

    public JMSSelectorFilter(String selector) throws AMQInvalidArgumentException
    {
        _selector = selector;
        _matcher = new SelectorParser().parse(selector);
    }

    public boolean matches(Filterable message)
    {
        boolean match = _matcher.matches(message);
        if(_logger.isDebugEnabled())
        {
            _logger.debug(message + " match(" + match + ") selector(" + System.identityHashCode(_selector) + "):" + _selector);
        }
        return match;
    }

    public String getSelector()
    {
        return _selector;
    }

    @Override
    public String toString()
    {
        return "JMSSelector("+_selector+")";
    }
}
