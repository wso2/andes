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
package org.wso2.andes.server.configuration.plugins;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SlowConsumerDetectionConfiguration extends ConfigurationPlugin
{
    public static class SlowConsumerDetectionConfigurationFactory implements ConfigurationPluginFactory
    {
        public ConfigurationPlugin newInstance(String path, Configuration config) throws ConfigurationException
        {
            SlowConsumerDetectionConfiguration slowConsumerConfig = new SlowConsumerDetectionConfiguration();
            slowConsumerConfig.setConfiguration(path, config);
            return slowConsumerConfig;
        }

        public List<String> getParentPaths()
        {
            return Arrays.asList("virtualhosts.virtualhost.slow-consumer-detection");
        }
    }

    //Set Default time unit to seconds
    TimeUnit _timeUnit = TimeUnit.SECONDS;

    public String[] getElementsProcessed()
    {
        return new String[]{"delay",
                            "timeunit"};
    }

    public long getDelay()
    {
        return getLongValue("delay", 10);
    }

    public TimeUnit getTimeUnit()
    {
        return  _timeUnit;
    }

    @Override
    public void validateConfiguration() throws ConfigurationException
    {
        validatePositiveLong("delay");

        String timeUnit = getStringValue("timeunit");

        if (timeUnit != null)
        {
            try
            {
                _timeUnit = TimeUnit.valueOf(timeUnit.toUpperCase());
            }
            catch (IllegalArgumentException iae)
            {
                throw new ConfigurationException("Unable to configure Slow Consumer Detection invalid TimeUnit:" + timeUnit);
            }
        }

    }
}
