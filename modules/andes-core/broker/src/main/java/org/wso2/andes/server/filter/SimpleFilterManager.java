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
import org.wso2.andes.server.queue.Filterable;

import java.util.concurrent.ConcurrentLinkedQueue;

public class SimpleFilterManager implements FilterManager
{
    private final Logger _logger = Logger.getLogger(SimpleFilterManager.class);

    private final ConcurrentLinkedQueue<MessageFilter> _filters;
    private String _toString = "";

    public SimpleFilterManager()
    {
        _logger.debug("Creating SimpleFilterManager");
        _filters = new ConcurrentLinkedQueue<MessageFilter>();
    }

    public void add(MessageFilter filter)
    {
        _filters.add(filter);
        updateStringValue();
    }

    public void remove(MessageFilter filter)
    {
        _filters.remove(filter);
        updateStringValue();
    }

    public boolean allAllow(Filterable msg)
    {
        for (MessageFilter filter : _filters)
        {
            if (!filter.matches(msg))
            {
                return false;
            }
        }
        return true;
    }

    public boolean hasFilters()
    {
        return !_filters.isEmpty();
    }


    @Override
    public String toString()
    {
       return _toString;
    }

    private void updateStringValue()
    {
        StringBuilder toString = new  StringBuilder();
        for (MessageFilter filter : _filters)
        {
            toString.append(filter.toString());
            toString.append(",");
        }

        if (_filters.size() > 0)
        {
            //Remove the last ','
            toString.deleteCharAt(toString.length()-1);
        }
        _toString = toString.toString();
    }
}
