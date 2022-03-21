/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.jndi;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.regex.Pattern;

public class Utils {

    public static final String SYS_PROPERTY_PLACEHOLDER_PREFIX = "$sys{";
    public static final String ENV_VAR_PLACEHOLDER_PREFIX = "$env{";
    public static final String DYNAMIC_PROPERTY_PLACEHOLDER_PREFIX = "${";
    public static final String PLACEHOLDER_SUFFIX = "}";
    public static final String SEC_PREFIX = "$secret{";
    public static final String TEMP_PLACEHOLDER = "_TEMP_";

    private static Log log = LogFactory.getLog(Utils.class);

    public static String resolveSystemProperty(String text) {
        String sysRefs = StringUtils.substringBetween(text, SYS_PROPERTY_PLACEHOLDER_PREFIX, PLACEHOLDER_SUFFIX);
        String envRefs = StringUtils.substringBetween(text, ENV_VAR_PLACEHOLDER_PREFIX, PLACEHOLDER_SUFFIX);

        // Resolves system property references ($sys{ref}) in an individual string.
        if (sysRefs != null) {
            String property = System.getProperty(sysRefs);
            if (StringUtils.isNotEmpty(property)) {
                text = text.replaceAll(Pattern.quote(SYS_PROPERTY_PLACEHOLDER_PREFIX + sysRefs + PLACEHOLDER_SUFFIX), property);
            } else {
                log.error("System property is not available for " + sysRefs);
            }
            return text;
        }
        // Resolves environment variable references ($env{ref}) in an individual string.
        if (envRefs != null) {
            String resolvedValue = System.getenv(envRefs);
            if (StringUtils.isNotEmpty(resolvedValue)) {
                text = text.replaceAll(Pattern.quote(ENV_VAR_PLACEHOLDER_PREFIX + envRefs + PLACEHOLDER_SUFFIX), resolvedValue);
            } else {
                log.error("Environment variable is not available for " + envRefs);
            }
            return text;
        }

        int indexOfStartingChars = -1;
        int indexOfClosingBrace;
        
        // Temporarily replace the secure vault alias ($secret{alias}) with a placeholder
        String secAlias = StringUtils.substringBetween(text, SEC_PREFIX, PLACEHOLDER_SUFFIX);
        String secAliasRef = SEC_PREFIX + secAlias + PLACEHOLDER_SUFFIX;
        text = text.replace(secAliasRef, TEMP_PLACEHOLDER);

        // The following condition deals with properties.
        // Properties are specified as ${system.property},
        // and are assumed to be System properties
        while (indexOfStartingChars < text.indexOf(DYNAMIC_PROPERTY_PLACEHOLDER_PREFIX)
                && (indexOfStartingChars = text.indexOf(DYNAMIC_PROPERTY_PLACEHOLDER_PREFIX)) != -1
                && (indexOfClosingBrace = text.indexOf(PLACEHOLDER_SUFFIX)) != -1) {
            String sysProp = text.substring(indexOfStartingChars + 2, indexOfClosingBrace);
            String propValue = System.getProperty(sysProp);
            if (propValue == null) {
                propValue = System.getenv(sysProp);
            }
            if (propValue != null) {
                text = text.substring(0, indexOfStartingChars) + propValue
                        + text.substring(indexOfClosingBrace + 1);
            }
            if (sysProp.equals("carbon.home") && propValue != null
                    && propValue.equals(".")) {
                text = new File(".").getAbsolutePath() + File.separator + text;
            }
        }
        
        // Replace the temporary placeholder with the secure vault alias ($secret{alias}) 
        text = text.replace(TEMP_PLACEHOLDER, secAliasRef);
        
        return text;
    }
}
