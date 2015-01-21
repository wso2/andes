/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class TopicParserUtil {
    /**
     * Class Logger
     */
    private static final Logger log = Logger.getLogger(TopicParserUtil.class);

    /**
     * Character used to identify tokens in a topic name
     */
    public static final String TOPIC_TOKEN_SEPARATOR = "/";

    /**
     * Check whether subscribed topic name with wildcards matches the given subscription topic name
     *
     * @param topicName
     *         topic name
     * @param subscriptionTopicName
     *         subscribed topic name
     * @return true if matching
     */
    public static boolean isMatching(String topicName, String subscriptionTopicName) throws AndesException {
        try {
            List<Token> msgTokens = splitTopic(topicName);
            List<Token> subscriptionTokens = splitTopic(subscriptionTopicName);
            int i = 0;
            Token subToken = null;
            for (; i < subscriptionTokens.size(); i++) {
                subToken = subscriptionTokens.get(i);
                if (subToken != Token.MULTI && subToken != Token.SINGLE) {
                    if (i >= msgTokens.size()) {
                        return false;
                    }
                    Token msgToken = msgTokens.get(i);
                    if (!msgToken.equals(subToken)) {
                        return false;
                    }
                } else {
                    if (subToken == Token.MULTI) {
                        return true;
                    }
                    // if execution reach this point Token is a "SINGLE". Therefore skip a step forward
                }
            }
            //if last token was a SINGLE then treat it as an empty
            if (subToken == Token.SINGLE && (i - msgTokens.size() == 1)) {
                i--;
            }
            return i == msgTokens.size();
        } catch (ParseException ex) {
            log.error("Topic format is incorrect", ex);
            throw new AndesException(ex);
        }
    }

    private static List<Token> splitTopic(String topic) throws ParseException {
        List<Token> res = new ArrayList<Token>();
        String[] tokens = topic.split(TOPIC_TOKEN_SEPARATOR);

        if (tokens.length == 0) {
            res.add(Token.EMPTY);
        }

        for (int i = 0; i < tokens.length; i++) {
            String s = tokens[i];
            if (s.isEmpty()) {
                //                if (i != 0) {
                //                    throw new ParseException("Bad format of topic, expetec topic name between
                // separators", i);
                //                }
                res.add(Token.EMPTY);
            } else if (s.equals("#")) {
                //check that multi is the last symbol
                if (i != tokens.length - 1) {
                    throw new ParseException(
                            "Bad format of topic, the multi symbol (#) has to be the last one after a separator", i);
                }
                res.add(Token.MULTI);
            } else if (s.contains("#")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else if (s.equals("?")) {
                res.add(Token.SINGLE);
            } else if (s.contains("?")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else {
                res.add(new Token(s));
            }
        }

        return res;
    }

    private static class Token {

        static final Token EMPTY = new Token("");
        static final Token MULTI = new Token("#");
        static final Token SINGLE = new Token("?");
        String name;

        protected Token(String s) {
            name = s;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 29 * hash + (this.name != null ? this.name.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Token other = (Token) obj;
            return !((this.name == null) ? (other.name != null) : !this.name.equals(other.name));
        }

        @Override
        public String toString() {
            return name;
        }

        protected String name() {
            return name;
        }
    }
}
