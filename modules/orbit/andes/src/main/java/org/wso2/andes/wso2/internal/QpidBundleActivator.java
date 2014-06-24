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

package org.wso2.andes.wso2.internal;

import org.wso2.andes.server.plugins.PluginManager;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.wso2.andes.wso2.service.QpidNotificationService;
import org.wso2.andes.wso2.service.QpidNotificationServiceImpl;

/**
 * This class implements the OSGi bundle activator for the Carbonized Qpid.
 */
public final class QpidBundleActivator implements BundleActivator {

    private static final Log log = LogFactory.getLog(QpidBundleActivator.class);
    private ServiceRegistration qpidNotificationServiceReg = null;

    public void start(BundleContext ctx) throws Exception {
        // Setting BundleContext in PluginManager will prevent Felix from starting
        // when Qpid is inside another host OSGi container.
        log.info("Setting BundleContext in PluginManager");
        PluginManager.setBundleContext(ctx);

        // Register service that exposes Qpid properties
        qpidNotificationServiceReg = ctx.registerService(QpidNotificationService.class.getName(),
                                                         new QpidNotificationServiceImpl(), null);
    }

    public void stop(BundleContext ctx) throws Exception {
        // Qpid enables the system property "log4j.defaultInitOverride" to prevent it's logging module
        //  from wondering off and picking up the first log4j.xml/properties file it finds on the classpath.
        // Unless we disable that, Carbon logging gets busted when it is restarted.
        System.clearProperty("log4j.defaultInitOverride");

        // Unregister notification service
        if (null != qpidNotificationServiceReg) {
            qpidNotificationServiceReg.unregister();
        }
    }
}
