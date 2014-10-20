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
package org.wso2.andes.server.message;

import org.wso2.andes.transport.DeliveryProperties;
import org.wso2.andes.transport.MessageProperties;
import org.wso2.andes.transport.MessageDeliveryPriority;

import java.util.Set;
import java.util.Map;
import java.util.UUID;

class MessageTransferHeader implements AMQMessageHeader
{


    public static final String JMS_TYPE = "x-jms-type";

    private final DeliveryProperties _deliveryProps;
    private final MessageProperties _messageProps;

    public MessageTransferHeader(DeliveryProperties deliveryProps, MessageProperties messageProps)
    {
        _deliveryProps = deliveryProps;
        _messageProps = messageProps;
    }

    public String getCorrelationId()
    {
        if (_messageProps != null && _messageProps.getCorrelationId() != null)
        {
            return new String(_messageProps.getCorrelationId());
        }
        else
        {
            return null;
        }
    }

    public long getExpiration()
    {
        return _deliveryProps == null ? 0L : _deliveryProps.getExpiration();
    }

    public String getMessageId()
    {
        UUID id = _messageProps == null ? null : _messageProps.getMessageId();
        
        return id == null ? null : String.valueOf(id);
    }

    public String getMimeType()
    {
        return _messageProps == null ? null : _messageProps.getContentType();
    }

    public String getEncoding()
    {
        return _messageProps == null ? null : _messageProps.getContentEncoding();
    }

    public byte getPriority()
    {
        MessageDeliveryPriority priority = _deliveryProps == null
                                           ? MessageDeliveryPriority.MEDIUM
                                           : _deliveryProps.getPriority();
        return (byte) priority.getValue();
    }

    public long getTimestamp()
    {
        return _deliveryProps == null ? 0L : _deliveryProps.getTimestamp();
    }

    public String getType()
    {
        Object type = getHeader(JMS_TYPE);
        return type instanceof String ? (String) type : null; 
    }

    public String getReplyTo()
    {
        if (_messageProps != null && _messageProps.getReplyTo() != null)
        {
            return _messageProps.getReplyTo().toString();
        }
        else
        {
            return null;
        }
    }

    public String getReplyToExchange()
    {
        if (_messageProps != null && _messageProps.getReplyTo() != null)
        {
            return _messageProps.getReplyTo().getExchange();
        }
        else
        {
            return null;
        }
    }

    public String getReplyToRoutingKey()
    {
        if (_messageProps != null && _messageProps.getReplyTo() != null)
        {
            return _messageProps.getReplyTo().getRoutingKey();
        }
        else
        {
            return null;
        }
    }

    public Object getHeader(String name)
    {
        Map<String, Object> appHeaders = _messageProps == null ? null : _messageProps.getApplicationHeaders();
        return appHeaders == null ? null : appHeaders.get(name);
    }

    public boolean containsHeaders(Set<String> names)
    {
        Map<String, Object> appHeaders = _messageProps == null ? null : _messageProps.getApplicationHeaders();
        return appHeaders != null && appHeaders.keySet().containsAll(names);

    }

    public boolean containsHeader(String name)
    {
        Map<String, Object> appHeaders = _messageProps == null ? null : _messageProps.getApplicationHeaders();
        return appHeaders != null && appHeaders.containsKey(name);
    }
}
