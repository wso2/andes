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

package org.wso2.andes.server.exchange.topic;

import org.wso2.andes.framing.AMQShortString;

public final class TopicWord
{
    public static final TopicWord ANY_WORD = new TopicWord("*");
    public static final TopicWord WILDCARD_WORD = new TopicWord("#");
    private String _word;

    public TopicWord()
    {

    }

    public TopicWord(String s)
    {
        _word = s;
    }

    public TopicWord(final AMQShortString name)
    {
        _word = name.toString();
    }

    public String toString()
    {
        return _word;
    }
}
