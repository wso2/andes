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

import java.util.concurrent.ConcurrentHashMap;

public class TopicWordDictionary
{
    private final ConcurrentHashMap<AMQShortString,TopicWord> _dictionary =
            new ConcurrentHashMap<AMQShortString,TopicWord>();



    public TopicWordDictionary()
    {
        _dictionary.put(new AMQShortString("*"), TopicWord.ANY_WORD);
        _dictionary.put(new AMQShortString("#"), TopicWord.WILDCARD_WORD);
    }




    public TopicWord getOrCreateWord(AMQShortString name)
    {
        TopicWord word = _dictionary.putIfAbsent(name, new TopicWord(name));
        if(word == null)
        {
            word = _dictionary.get(name);
        }
        return word;
    }


    public TopicWord getWord(AMQShortString name)
    {
        TopicWord word = _dictionary.get(name);
        if(word == null)
        {
            word = TopicWord.ANY_WORD;
        }
        return word;
    }
}
