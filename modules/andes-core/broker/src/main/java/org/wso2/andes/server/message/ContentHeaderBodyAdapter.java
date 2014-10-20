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

import org.wso2.andes.framing.ContentHeaderBody;
import org.wso2.andes.framing.BasicContentHeaderProperties;
import org.wso2.andes.framing.FieldTable;

import java.util.Set;

public class ContentHeaderBodyAdapter implements AMQMessageHeader
{
    private final ContentHeaderBody _contentHeaderBody;

    public ContentHeaderBodyAdapter(ContentHeaderBody contentHeaderBody)
    {
        _contentHeaderBody = contentHeaderBody;
    }

    private BasicContentHeaderProperties getProperties()
    {
        return (BasicContentHeaderProperties) _contentHeaderBody.getProperties();
    }

    public String getCorrelationId()
    {
        return getProperties().getCorrelationIdAsString();
    }

    public long getExpiration()
    {
        return getProperties().getExpiration();
    }

    public String getMessageId()
    {
        return getProperties().getMessageIdAsString();
    }

    public String getMimeType()
    {
        return getProperties().getContentTypeAsString();
    }

    public String getEncoding()
    {
        return getProperties().getEncodingAsString();
    }

    public byte getPriority()
    {
        return getProperties().getPriority();
    }

    public long getTimestamp()
    {
        return getProperties().getTimestamp();
    }

    public String getType()
    {
        return getProperties().getTypeAsString();
    }

    public String getReplyTo()
    {
        return getProperties().getReplyToAsString();
    }

    public String getReplyToExchange()
    {
        // TODO
        return getReplyTo();
    }

    public String getReplyToRoutingKey()
    {
        // TODO
        return getReplyTo();

    }

    public Object getHeader(String name)
    {
        FieldTable ft = getProperties().getHeaders();
        return ft.get(name);
    }

    public boolean containsHeaders(Set<String> names)
    {
        FieldTable ft = getProperties().getHeaders();
        for(String name : names)
        {
            if(!ft.containsKey(name))
            {
                return false;
            }
        }
        return true;
    }

    public boolean containsHeader(String name)
    {
        FieldTable ft = getProperties().getHeaders();
        return ft.containsKey(name);
    }


}
