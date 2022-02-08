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
package org.wso2.andes.server.logging.management;

import static org.apache.log4j.xml.QpidLog4JConfigurator.LOCK;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.wso2.andes.management.common.mbeans.LoggingManagement;
import org.wso2.andes.management.common.mbeans.annotations.MBeanDescription;
import org.wso2.andes.server.management.AMQManagedObject;
import org.wso2.andes.util.FileUtils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.JMException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;


/** MBean class for BrokerLoggingManagerMBean. It implements all the management features exposed for managing logging. */
@MBeanDescription("Logging Management Interface")
public class LoggingManagementMBean extends AMQManagedObject implements LoggingManagement
{

    private static final Logger _logger = LogManager.getLogger(LoggingManagementMBean.class);
    private String _log4jConfigFileName;
    private int _log4jLogWatchInterval;
    private static final String INHERITED = "INHERITED";
    private static final String[] LEVELS = new String[]{Level.ALL.toString(), Level.TRACE.toString(), 
                                                        Level.DEBUG.toString(), Level.INFO.toString(), 
                                                        Level.WARN.toString(), Level.ERROR.toString(), 
                                                        Level.FATAL.toString(),Level.OFF.toString(),
                                                        INHERITED};   
    static TabularType _loggerLevelTabularType;
    static CompositeType _loggerLevelCompositeType;

    static
    {
        try
        {
            OpenType[] loggerLevelItemTypes = new OpenType[]{SimpleType.STRING, SimpleType.STRING};

            _loggerLevelCompositeType = new CompositeType("LoggerLevelList", "Logger Level Data", 
                                                         COMPOSITE_ITEM_NAMES.toArray(new String[COMPOSITE_ITEM_NAMES.size()]),
                                                         COMPOSITE_ITEM_DESCRIPTIONS.toArray(new String[COMPOSITE_ITEM_DESCRIPTIONS.size()]),
                                                         loggerLevelItemTypes);

            _loggerLevelTabularType = new TabularType("LoggerLevel", "List of loggers with levels",
                                                       _loggerLevelCompositeType, 
                                                       TABULAR_UNIQUE_INDEX.toArray(new String[TABULAR_UNIQUE_INDEX.size()]));
        }
        catch (OpenDataException e)
        {
            _logger.error("Tabular data setup for viewing logger levels was incorrect.");
            _loggerLevelTabularType = null;
        }
    }
    
    public LoggingManagementMBean(String log4jConfigFileName, int log4jLogWatchInterval) throws JMException
    {
        super(LoggingManagement.class, LoggingManagement.TYPE);
        _log4jConfigFileName = log4jConfigFileName;
        _log4jLogWatchInterval = log4jLogWatchInterval;
    }

    public String getObjectInstanceName()
    {
        return LoggingManagement.TYPE;
    }
    
    public Integer getLog4jLogWatchInterval()
    {
        return _log4jLogWatchInterval;
    }
    
    public String[] getAvailableLoggerLevels()
    {
        return LEVELS;
    }
    @SuppressWarnings("unchecked")
    public synchronized boolean setRuntimeLoggerLevel(String logger, String level)
    {   
        //check specified level is valid
        Level newLevel;
        try
        {
            newLevel = getLevel(level);
        }
        catch (Exception e)
        {
            return false;
        }
        
        //set the logger to the new level
        _logger.info("Setting level to " + level + " for logger: " + logger);
        
        Logger log = LogManager.getLogger(logger);
        if (log == null) {
            return false;
        }
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(logger);
        loggerConfig.setLevel(newLevel);
        ctx.updateLoggers(config);
        
        return true;
    }
    
    @SuppressWarnings("unchecked")
    public synchronized TabularData viewEffectiveRuntimeLoggerLevels()
    {
        if (_loggerLevelTabularType == null)
        {
            _logger.warn("TabluarData type not set up correctly");
            return null;
        }

        _logger.info("Getting levels for currently active log4j loggers");

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Collection<? extends Logger> loggers = ctx.getLoggers();

        TabularData loggerLevelList = new TabularDataSupport(_loggerLevelTabularType);

        String loggerName;
        String level;
        
        try
        {
            for (Logger logger: loggers) {
                loggerName = logger.getName();
                level = logger.getLevel().toString();
                Object[] itemData = {loggerName, level};
                CompositeData loggerData = new CompositeDataSupport(_loggerLevelCompositeType,
                        COMPOSITE_ITEM_NAMES.toArray(new String[COMPOSITE_ITEM_NAMES.size()]), itemData);
                loggerLevelList.put(loggerData);
            }
        }
        catch (OpenDataException e)
        {
            _logger.warn("Unable to create logger level list due to :" + e);
            return null;
        }

        return loggerLevelList;
        
    }
    
    public synchronized String getRuntimeRootLoggerLevel()
    {
        Logger rootLogger = LogManager.getRootLogger();

        return rootLogger.getLevel().toString();
    }

    @Override
    public void reloadConfigFile() throws IOException {
        ConfigurationSource source = new ConfigurationSource(new FileInputStream(_log4jConfigFileName),
                new File(_log4jConfigFileName));
        Configurator.initialize(null, source);
        Configurator.reconfigure();
    }

    public synchronized boolean setRuntimeRootLoggerLevel(String level)
    {
        Level newLevel;
        try
        {
            newLevel = getLevel(level);
        }
        catch (Exception e)
        {
            return false;
        }
        
        if(newLevel == null)
        {
            //A null Level reference implies inheritance. Setting the runtime RootLogger 
            //to null is catastrophic (and prevented by Log4J at startup and runtime anyway).
            return false;
        }

        _logger.info("Setting RootLogger level to " + level);

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(newLevel);
        ctx.updateLoggers(config);

        return true;
    }
    
    //method to convert from a string to a log4j Level, throws exception if the given value is invalid
    private Level getLevel(String level) throws Exception
    {
        if("null".equalsIgnoreCase(level) || INHERITED.equalsIgnoreCase(level))
        {
            //the string "null" or "inherited" signals to inherit from a parent logger,
            //using a null Level reference for the logger.
            return null;
        }
        
        Level newLevel = Level.toLevel(level);
        
        //above Level.toLevel call returns a DEBUG Level if the request fails. Check the result.
        if (newLevel.equals(Level.DEBUG) && !(level.equalsIgnoreCase("debug")))
        {
            //received DEBUG but we did not ask for it, the Level request failed.
            throw new Exception("Invalid level name");
        }
        
        return newLevel;
    }


    /* The log4j XML configuration file DTD defines three possible element
     * combinations for specifying optional logger+level settings.
     * Must account for the following:
     * 
     * <category name="x"> <priority value="y"/> </category>    OR
     * <category name="x"> <level value="y"/> </category>    OR
     * <logger name="x"> <level value="y"/> </logger>
     *
     * Noting also that the level/priority child element is optional too,
     * and not the only possible child element.
     */
    
    public static synchronized Map<String,String> retrieveConfigFileLoggersLevels(String fileName) throws IOException
    {
        try
        {
            LOCK.lock();

            ConfigurationSource source = new ConfigurationSource(new FileInputStream(fileName), new File(fileName));
            Configurator.initialize(null, source);

            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration config = ctx.getConfiguration();
            Map<String, LoggerConfig> loggers = config.getLoggers();

            HashMap<String,String> loggerLevelList = new HashMap<String,String>();


            for (Map.Entry<String, LoggerConfig> entry : loggers.entrySet()) {
                loggerLevelList.put(entry.getKey(), entry.getValue().getLevel().toString());
            }

            return loggerLevelList;
        }
        finally
        {
            LOCK.unlock();
        }
    }

    public synchronized TabularData viewConfigFileLoggerLevels() throws IOException
    {
        try
        {
            LOCK.lock();    

            if (_loggerLevelTabularType == null)
            {
                _logger.warn("TabluarData type not set up correctly");
                return null;
            }

            _logger.info("Getting logger levels from log4j configuration file");

            TabularData loggerLevelList = new TabularDataSupport(_loggerLevelTabularType);

            Map<String,String> levels = retrieveConfigFileLoggersLevels(_log4jConfigFileName);

            for (Map.Entry<String,String> entry : levels.entrySet())
            {
                String loggerName = entry.getKey();
                String level = entry.getValue();

                try
                {
                    Object[] itemData = {loggerName, level.toUpperCase()};
                    CompositeData loggerData = new CompositeDataSupport(_loggerLevelCompositeType, 
                            COMPOSITE_ITEM_NAMES.toArray(new String[COMPOSITE_ITEM_NAMES.size()]), itemData);
                    loggerLevelList.put(loggerData);
                }
                catch (OpenDataException e)
                {
                    _logger.warn("Unable to create logger level list due to :" + e);
                    return null;
                }
            }

            return loggerLevelList;
        }
        finally
        {
            LOCK.unlock();
        }
    }

    public synchronized boolean setConfigFileLoggerLevel(String logger, String level) throws IOException
    {
        try
        {
            LOCK.lock();

            //check that the specified level is a valid log4j Level
            try
            {
                getLevel(level);
            }
            catch (Exception e)
            {
                //it isnt a valid level
                return false;
            }

            _logger.info("Setting level to " + level + " for logger '" + logger
                    + "' in log4j xml configuration file: " + _log4jConfigFileName);

            ConfigurationSource source = new ConfigurationSource(new FileInputStream(_log4jConfigFileName),
                    new File(_log4jConfigFileName));
            Configurator.initialize(null, source);

            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration config = ctx.getConfiguration();
            LoggerConfig loggerConfig = config.getLoggerConfig(logger);

            if (loggerConfig == null) {
                //no loggers/categories with given name found, does not exist to update
                _logger.warn("Specified logger does not exist in the configuration file: " +logger);
                return false;
            }

            loggerConfig.setLevel(Level.getLevel(level));

            //output the new file
            return true;
        }
        finally
        {
            LOCK.unlock();
        }
    }

    
    /* The log4j XML configuration file DTD defines 2 possible element
     * combinations for specifying the optional root logger level settings
     * Must account for the following:
     * 
     * <root> <priority value="y"/> </root>    OR
     * <root> <level value="y"/> </root> 
     *
     * Noting also that the level/priority child element is optional too,
     * and not the only possible child element.
     */
    
    public static synchronized String retrieveConfigFileRootLoggerLevel(String fileName) throws IOException
    {
        try
        {
            LOCK.lock();

            ConfigurationSource source = new ConfigurationSource(new FileInputStream(fileName), new File(fileName));
            Configurator.initialize(null, source);

            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration config = ctx.getConfiguration();
            return config.getRootLogger().getLevel().toString();
        }
        finally
        {
            LOCK.unlock();
        }
    }
    
    public synchronized String getConfigFileRootLoggerLevel() throws IOException
    {
        return retrieveConfigFileRootLoggerLevel(_log4jConfigFileName).toUpperCase();
    }
    
    public synchronized boolean setConfigFileRootLoggerLevel(String level) throws IOException
    {
        try
        {
            LOCK.lock();

            //check that the specified level is a valid log4j Level
            try
            {
                Level newLevel = getLevel(level);
                if(newLevel == null)
                {
                    //A null Level reference implies inheritance. Setting the config file RootLogger 
                    //to "null" or "inherited" just ensures it defaults to DEBUG at startup as Log4J 
                    //prevents this catastrophic situation at startup and runtime anyway.
                    return false;
                }
            }
            catch (Exception e)
            {
                //it isnt a valid level
                return false;
            }

            _logger.info("Setting level to " + level + " for the Root logger in " +
                    "log4j xml configuration file: " + _log4jConfigFileName);

            ConfigurationSource source = new ConfigurationSource(new FileInputStream(_log4jConfigFileName),
                    new File(_log4jConfigFileName));
            Configurator.initialize(null, source);

            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration config = ctx.getConfiguration();
            config.getRootLogger().setLevel(Level.getLevel(level));
            return true;
        }
        finally
        {
            LOCK.unlock();
        }
    }
}
