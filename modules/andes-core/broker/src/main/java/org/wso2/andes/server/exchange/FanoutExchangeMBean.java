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
package org.wso2.andes.server.exchange;

import org.wso2.andes.management.common.mbeans.annotations.MBeanDescription;
import org.wso2.andes.management.common.mbeans.annotations.MBeanConstructor;
import org.wso2.andes.server.binding.Binding;

import javax.management.JMException;
import javax.management.openmbean.*;
import java.util.ArrayList;

/**
     * MBean class implementing the management interfaces.
 */
@MBeanDescription("Management Bean for Fanout Exchange")
final class FanoutExchangeMBean extends AbstractExchangeMBean<FanoutExchange>
{
    private static final String BINDING_KEY_SUBSTITUTE = "*";

    @MBeanConstructor("Creates an MBean for AMQ fanout exchange")
    public FanoutExchangeMBean(final FanoutExchange exchange) throws JMException
    {
        super(exchange);
        init();
    }

    public TabularData bindings() throws OpenDataException
    {

        TabularDataSupport bindingList = new TabularDataSupport(_bindinglistDataType);


        ArrayList<String> queueNames = new ArrayList<String>();

        for (Binding binding : getExchange().getBindings())
        {
            String queueName = binding.getQueue().getNameShortString().toString();
            queueNames.add(queueName);
        }

        Object[] bindingItemValues = {BINDING_KEY_SUBSTITUTE, queueNames.toArray(new String[0])};
        CompositeData bindingData = new CompositeDataSupport(_bindingDataType,
                COMPOSITE_ITEM_NAMES.toArray(new String[COMPOSITE_ITEM_NAMES.size()]), 
                bindingItemValues);
        bindingList.put(bindingData);

        return bindingList;
    }


} // End of MBean class
