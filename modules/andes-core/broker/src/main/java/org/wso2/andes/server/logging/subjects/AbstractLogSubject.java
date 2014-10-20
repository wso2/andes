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
package org.wso2.andes.server.logging.subjects;

import org.wso2.andes.server.logging.LogSubject;

import java.text.MessageFormat;

/**
 * The LogSubjects all have a similar requriement to format their output and
 * provide the String value.
 *
 * This Abstract LogSubject provides this basic functionality, allowing the
 * actual LogSubjects to provide their formating and data.
 */
public abstract class AbstractLogSubject implements LogSubject
{
    /**
     * The logString that will be returned via toLogString
     */
    protected String _logString;

    /**
     * Set the toString logging of this LogSubject. Based on a format provided
     * by format and the var args.
     * @param format The Message to format
     * @param args The values to put in to the message.
     */
    protected void setLogStringWithFormat(String format, Object... args)
    {
        _logString = "[" + MessageFormat.format(format, args) + "] ";
    }

    /**
     * toLogString is how the Logging infrastructure will get the text for this
     * LogSubject
     *
     * @return String representing this LogSubject
     */
    public String toLogString()
    {
        return _logString;
    }

}
