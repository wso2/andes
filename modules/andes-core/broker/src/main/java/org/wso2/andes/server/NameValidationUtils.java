/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.andes.server;

import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesConstants;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class contains queue name/topic name/subscription ID validations for AMQP
 */
public class NameValidationUtils {

    /**
     * Whether strict validation against queue name or topic name is enabled
     */
    private static Boolean isStrictValidationEnabled = AndesConfigurationManager.readValue
            (AndesConfiguration.ALLOW_STRICT_NAME_VALIDATION);

   /* Pattern to validate the topic name. Pattern can starts with star(*) or any alphanumeric
    character(a-z A-Z 0-9),
  *  0 or more times, delimited by dots. Pattern can ends with star, alphanumeric character or with a hash (#).
  *  Can use the TENANT_SEPARATOR only to separate tenant domain from the topic name, only when create topics from
  *  jms clients. */

    private static Pattern topicNamePattern; static {

        if (isStrictValidationEnabled) {
            topicNamePattern = Pattern.compile("^(((\\*|\\w+)\\.)*(\\*|\\w+|#))$");
        } else {
            topicNamePattern = Pattern.compile("^(((\\*|(\\w|:)+)\\.)*(\\*|(\\w|:)+|#))$");

        }
    }


    /* Pattern to validate the queue name. Queue name cannot contain any of following symbols ~!@#;%^*()+={}|<>"',
    *  and space. Can use the TENANT_SEPARATOR only to separate tenant domain from the queue name. */
    private static final Pattern queueNamePattern = Pattern.compile("[[a-zA-Z]+[^(\\x00-\\x80)]+[0-9_\\-#*:.?&()]+]+");
    /**
     * When creating queues by jms clients, checks whether a given queue name is valid or not.
     *
     * @param queueNameWithTenantDomain Queue name with tenant domain, if creates by a tenant
     * @return true, if it is a valid queue name, false otherwise
     */
    public static boolean isValidQueueName(String queueNameWithTenantDomain) {
        /* Extract the queue name part, from the full queue name.*/
        String queueName = getNameWithoutTenantDomain(queueNameWithTenantDomain);

        /* Store queue name. */
        Matcher nameMatcher = queueNamePattern.matcher(queueName);

        /* After validating the queue name, return if the given name is valid or not.*/
        return nameMatcher.matches();
    }

    /**
     * When creating topics by jms clients, checks whether a given topic name is valid or not.
     *
     * @param topicNameWithTenantDomain Topic name with tenant domain, if creates by a tenant
     * @return true, if it is a valid topic name, false otherwise
     */
    public static boolean isValidTopicName(String topicNameWithTenantDomain) {
        /* Extract the topic name part, from the full topic name.*/
        String topicName = getNameWithoutTenantDomain(topicNameWithTenantDomain);

        /* Store topic name. */
        Matcher nameMatcher = topicNamePattern.matcher(topicName);

        /* After validating the topic name, return if the given name is valid or not.*/
        return nameMatcher.matches();
    }

    /**
     * Get queue/topic name, without the tenant domain
     *
     * @param nameWithTenantDomain queue/topic name with the tenant domain, if creates by a tenant
     * @return String queue/topic name without tenant domain
     */
    public static String getNameWithoutTenantDomain(String nameWithTenantDomain) {
        String nameWithoutTenantDomain = nameWithTenantDomain;

        /* If the queue/topic is creating by a tenant, get the queue/topic name, without tenant domain.*/
        if (nameWithTenantDomain.contains(AndesConstants.TENANT_SEPARATOR)) {
            nameWithoutTenantDomain =
                    nameWithTenantDomain.substring(nameWithTenantDomain.indexOf(AndesConstants.TENANT_SEPARATOR) + 1);
        }

        return nameWithoutTenantDomain;
    }
}
