/*
 *  Copyright (c) 2008, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.wso2.andes.wso2.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This is a declarative service for exposing Qpid attributes to the OSGi runtime.
 */
public class QpidNotificationServiceImpl implements QpidNotificationService {

    private static final Log log = LogFactory.getLog(QpidNotificationServiceImpl.class);

    private String VERSION_INFO_PROP_FILE = "/qpidversion.properties";
    private String KEY_VERSION = "qpid.version";
    private String KEY_BUILD_NUMBER = "qpid.svnversion";

    String version = "";
    String buildNumber = "";

    public QpidNotificationServiceImpl() {
        setVersionInfo();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getBuildNumber() {
        return buildNumber;
    }

    public void setBuildNumber(String buildNumber) {
        this.buildNumber = buildNumber;
    }

    private void setVersionInfo() {
        try {
            Properties props = new Properties();
            InputStream is = this.getClass().getResourceAsStream(VERSION_INFO_PROP_FILE);
            props.load(is);

            // Read Qpid version
            version = props.getProperty(KEY_VERSION);

            // Read build number
            buildNumber =  props.getProperty(KEY_BUILD_NUMBER);
        } catch (IOException e) {
            log.warn("Failed to load " + VERSION_INFO_PROP_FILE);
        }
    }
}
