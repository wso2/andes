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
package org.wso2.andes.server.output;

import org.wso2.andes.server.message.MessageTransferMessage;
import org.wso2.andes.framing.BasicContentHeaderProperties;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.transport.Header;
import org.wso2.andes.transport.DeliveryProperties;
import org.wso2.andes.transport.MessageProperties;
import org.wso2.andes.transport.MessageDeliveryMode;
import org.wso2.andes.AMQPInvalidClassException;

import java.util.Map;

public class HeaderPropertiesConverter
{

    public static BasicContentHeaderProperties convert(MessageTransferMessage messageTransferMessage)
    {
        BasicContentHeaderProperties props = new BasicContentHeaderProperties();

        Header header = messageTransferMessage.getHeader();
        DeliveryProperties deliveryProps = header.get(DeliveryProperties.class);
        MessageProperties messageProps = header.get(MessageProperties.class);

        if(deliveryProps != null)
        {
            if(deliveryProps.hasDeliveryMode())
            {
                props.setDeliveryMode((byte)(deliveryProps.getDeliveryMode() == MessageDeliveryMode.PERSISTENT ? BasicContentHeaderProperties.PERSISTENT : BasicContentHeaderProperties.NON_PERSISTENT));
            }
            if(deliveryProps.hasExpiration())
            {
                props.setExpiration(deliveryProps.getExpiration());
            }
            if(deliveryProps.hasPriority())
            {
                props.setPriority((byte)deliveryProps.getPriority().getValue());
            }
            if(deliveryProps.hasTimestamp())
            {
                props.setTimestamp(deliveryProps.getTimestamp());
            }
        }
        if(messageProps != null)
        {
            if(messageProps.hasAppId())
            {
                props.setAppId(new AMQShortString(messageProps.getAppId()));
            }
            if(messageProps.hasContentType())
            {
                props.setContentType(messageProps.getContentType());
            }
            if(messageProps.hasCorrelationId())
            {
                props.setCorrelationId(new AMQShortString(messageProps.getCorrelationId()));
            }
            if(messageProps.hasContentEncoding())
            {
                props.setEncoding(messageProps.getContentEncoding());
            }
            if(messageProps.hasMessageId())
            {
                props.setMessageId(messageProps.getMessageId().toString());
            }

            // TODO Reply-to

            if(messageProps.hasUserId())
            {
                props.setUserId(new AMQShortString(messageProps.getUserId()));
            }

            if(messageProps.hasApplicationHeaders())
            {
                Map<String, Object> appHeaders = messageProps.getApplicationHeaders();
                FieldTable ft = new FieldTable();
                for(Map.Entry<String, Object> entry : appHeaders.entrySet())
                {
                    try
                    {
                        ft.put(new AMQShortString(entry.getKey()), entry.getValue());
                    }
                    catch(AMQPInvalidClassException e)
                    {
                        // TODO
                        // log here, but ignore - just can;t convert
                    }
                }
                props.setHeaders(ft);

            }
        }







        return props;
    }
}
