/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.server.configuration;

import static org.wso2.andes.transport.ConnectionSettings.WILDCARD_ADDRESS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Locale;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.registry.ConfigurationFileApplicationRegistry;
import org.wso2.andes.server.util.TestApplicationRegistry;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.server.virtualhost.VirtualHostRegistry;
import org.wso2.andes.test.utils.QpidTestCase;

public class ServerConfigurationTest extends QpidTestCase
{
    private XMLConfiguration _config = new XMLConfiguration();
    private ServerConfiguration _serverConfig = null;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _serverConfig = new ServerConfiguration(_config);
        ApplicationRegistry.initialise(new TestApplicationRegistry(_serverConfig));
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        ApplicationRegistry.remove();
    }

    public void testSetJMXManagementPort() throws ConfigurationException
    {
        _serverConfig.initialise();
        _serverConfig.setJMXManagementPort(23);
        assertEquals(23, _serverConfig.getJMXManagementPort());
    }

    public void testGetJMXManagementPort() throws ConfigurationException
    {
        _config.setProperty("management.jmxport", 42);
        _serverConfig.initialise();
        assertEquals(42, _serverConfig.getJMXManagementPort());
    }

    public void testGetPlatformMbeanserver() throws ConfigurationException
    {
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getPlatformMbeanserver());

        // Check value we set
        _config.setProperty("management.platform-mbeanserver", false);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getPlatformMbeanserver());
    }

    public void testGetPluginDirectory() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(null, _serverConfig.getPluginDirectory());

        // Check value we set
        _config.setProperty("plugin-directory", "/path/to/plugins");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals("/path/to/plugins", _serverConfig.getPluginDirectory());
    }

    public void testGetCacheDirectory() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(null, _serverConfig.getCacheDirectory());

        // Check value we set
        _config.setProperty("cache-directory", "/path/to/cache");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals("/path/to/cache", _serverConfig.getCacheDirectory());
    }

    public void testGetFrameSize() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(65536, _serverConfig.getFrameSize());

        // Check value we set
        _config.setProperty("advanced.framesize", "23");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(23, _serverConfig.getFrameSize());
    }

    public void testGetStatusEnabled() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(ServerConfiguration.DEFAULT_STATUS_UPDATES.equalsIgnoreCase("on"),
                     _serverConfig.getStatusUpdatesEnabled());

        // Check disabling we set
        _config.setProperty(ServerConfiguration.STATUS_UPDATES, "off");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getStatusUpdatesEnabled());

        // Check invalid values don't cause error but result in disabled
        _config.setProperty(ServerConfiguration.STATUS_UPDATES, "Yes Please");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getStatusUpdatesEnabled());

    }
    public void testGetSynchedClocks() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getSynchedClocks());

        // Check value we set
        _config.setProperty("advanced.synced-clocks", true);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getSynchedClocks());
    }

    public void testGetLocale() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();

        // The Default is what ever the VMs default is
        Locale defaultLocale = Locale.getDefault();

        assertEquals(defaultLocale, _serverConfig.getLocale());


        //Test Language only
        Locale update = new Locale("es");
        _config.setProperty(ServerConfiguration.ADVANCED_LOCALE, "es");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(update, _serverConfig.getLocale());

        //Test Language and Country
        update = new Locale("es","ES");
        _config.setProperty(ServerConfiguration.ADVANCED_LOCALE, "es_ES");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(update, _serverConfig.getLocale());

        //Test Language and Country and Variant
        update = new Locale("es","ES", "Traditional_WIN");
        _config.setProperty(ServerConfiguration.ADVANCED_LOCALE, "es_ES_Traditional_WIN");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(update, _serverConfig.getLocale());
    }


    public void testGetMsgAuth() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getMsgAuth());

        // Check value we set
        _config.setProperty("security.msg-auth", true);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getMsgAuth());
    }

    public void testGetManagementKeyStorePath() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(null, _serverConfig.getManagementKeyStorePath());

        // Check value we set
        _config.setProperty("management.ssl.keyStorePath", "a");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals("a", _serverConfig.getManagementKeyStorePath());
    }

    public void testGetManagementSSLEnabled() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getManagementSSLEnabled());

        // Check value we set
        _config.setProperty("management.ssl.enabled", false);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getManagementSSLEnabled());
    }

    public void testGetManagementKeyStorePassword() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(null, _serverConfig.getManagementKeyStorePassword());

        // Check value we set
        _config.setProperty("management.ssl.keyStorePassword", "a");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals("a", _serverConfig.getManagementKeyStorePassword());
    }

    public void testGetQueueAutoRegister() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getQueueAutoRegister());

        // Check value we set
        _config.setProperty("queue.auto_register", false);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getQueueAutoRegister());
    }

    public void testGetManagementEnabled() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getManagementEnabled());

        // Check value we set
        _config.setProperty("management.enabled", false);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getManagementEnabled());
    }

    public void testSetManagementEnabled() throws ConfigurationException
    {
        // Check value we set
        _serverConfig.initialise();
        _serverConfig.setManagementEnabled(false);
        assertEquals(false, _serverConfig.getManagementEnabled());
    }

    public void testGetHeartBeatDelay() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(5, _serverConfig.getHeartBeatDelay());

        // Check value we set
        _config.setProperty("heartbeat.delay", 23);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(23, _serverConfig.getHeartBeatDelay());
    }

    public void testGetHeartBeatTimeout() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(2.0, _serverConfig.getHeartBeatTimeout());

        // Check value we set
        _config.setProperty("heartbeat.timeoutFactor", 2.3);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(2.3, _serverConfig.getHeartBeatTimeout());
    }

    public void testGetMaximumMessageAge() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(0, _serverConfig.getMaximumMessageAge());

        // Check value we set
        _config.setProperty("maximumMessageAge", 10L);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(10, _serverConfig.getMaximumMessageAge());
    }

    public void testGetMaximumMessageCount() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(0, _serverConfig.getMaximumMessageCount());

        // Check value we set
        _config.setProperty("maximumMessageCount", 10L);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(10, _serverConfig.getMaximumMessageCount());
    }

    public void testGetMaximumQueueDepth() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(0, _serverConfig.getMaximumQueueDepth());

        // Check value we set
        _config.setProperty("maximumQueueDepth", 10L);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(10, _serverConfig.getMaximumQueueDepth());
    }

    public void testGetMaximumMessageSize() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(0, _serverConfig.getMaximumMessageSize());

        // Check value we set
        _config.setProperty("maximumMessageSize", 10L);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(10, _serverConfig.getMaximumMessageSize());
    }

    public void testGetMinimumAlertRepeatGap() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(0, _serverConfig.getMinimumAlertRepeatGap());

        // Check value we set
        _config.setProperty("minimumAlertRepeatGap", 10L);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(10, _serverConfig.getMinimumAlertRepeatGap());
    }

    public void testGetProcessors() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(4, _serverConfig.getConnectorProcessors());

        // Check value we set
        _config.setProperty("connector.processors", 10);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(10, _serverConfig.getConnectorProcessors());
        }

    public void testGetPorts() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertNotNull(_serverConfig.getPorts());
        assertEquals(1, _serverConfig.getPorts().size());
        assertEquals(5672, _serverConfig.getPorts().get(0));


        // Check value we set
        _config.setProperty("connector.port", "10");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertNotNull(_serverConfig.getPorts());
        assertEquals(1, _serverConfig.getPorts().size());
        assertEquals("10", _serverConfig.getPorts().get(0));
    }

    public void testGetBind() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(WILDCARD_ADDRESS, _serverConfig.getBind());

        // Check value we set
        _config.setProperty("connector.bind", "a");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals("a", _serverConfig.getBind());
    }

    public void testGetReceiveBufferSize() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(ServerConfiguration.DEFAULT_BUFFER_SIZE, _serverConfig.getReceiveBufferSize());

        // Check value we set
        _config.setProperty("connector.socketReceiveBuffer", "23");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(23, _serverConfig.getReceiveBufferSize());
    }

    public void testGetWriteBufferSize() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(ServerConfiguration.DEFAULT_BUFFER_SIZE, _serverConfig.getWriteBufferSize());

        // Check value we set
        _config.setProperty("connector.socketWriteBuffer", "23");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(23, _serverConfig.getWriteBufferSize());
    }

    public void testGetTcpNoDelay() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getTcpNoDelay());

        // Check value we set
        _config.setProperty("connector.tcpNoDelay", false);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getTcpNoDelay());
    }

    public void testGetEnableExecutorPool() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getEnableExecutorPool());

        // Check value we set
        _config.setProperty("advanced.filterchain[@enableExecutorPool]", true);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getEnableExecutorPool());
    }

    public void testGetEnableSSL() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getEnableSSL());

        // Check value we set
        _config.setProperty("connector.ssl.enabled", true);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getEnableSSL());
    }

    public void testGetSSLOnly() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getSSLOnly());

        // Check value we set
        _config.setProperty("connector.ssl.sslOnly", true);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getSSLOnly());
    }

    public void testGetSSLPorts() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertNotNull(_serverConfig.getSSLPorts());
        assertEquals(1, _serverConfig.getSSLPorts().size());
        assertEquals(ServerConfiguration.DEFAULT_SSL_PORT, _serverConfig.getSSLPorts().get(0));


        // Check value we set
        _config.setProperty("connector.ssl.port", "10");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertNotNull(_serverConfig.getSSLPorts());
        assertEquals(1, _serverConfig.getSSLPorts().size());
        assertEquals("10", _serverConfig.getSSLPorts().get(0));
    }

    public void testGetKeystorePath() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals("none", _serverConfig.getKeystorePath());

        // Check value we set
        _config.setProperty("connector.ssl.keystorePath", "a");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals("a", _serverConfig.getKeystorePath());
    }

    public void testGetKeystorePassword() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals("none", _serverConfig.getKeystorePassword());

        // Check value we set
        _config.setProperty("connector.ssl.keystorePassword", "a");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals("a", _serverConfig.getKeystorePassword());
    }

    public void testGetCertType() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals("SunX509", _serverConfig.getCertType());

        // Check value we set
        _config.setProperty("connector.ssl.certType", "a");
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals("a", _serverConfig.getCertType());
    }

    public void testGetUseBiasedWrites() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(false, _serverConfig.getUseBiasedWrites());

        // Check value we set
        _config.setProperty("advanced.useWriteBiasedPool", true);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(true, _serverConfig.getUseBiasedWrites());
    }

    public void testGetHousekeepingExpiredMessageCheckPeriod() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();
        assertEquals(30000, _serverConfig.getHousekeepingCheckPeriod());

        // Check value we set
        _config.setProperty("housekeeping.expiredMessageCheckPeriod", 23L);
        _serverConfig = new ServerConfiguration(_config);
        _serverConfig.initialise();
        assertEquals(23, _serverConfig.getHousekeepingCheckPeriod());
        _serverConfig.setHousekeepingExpiredMessageCheckPeriod(42L);
        assertEquals(42, _serverConfig.getHousekeepingCheckPeriod());
    }

    public void testSingleConfiguration() throws IOException, ConfigurationException
    {
        File fileA = File.createTempFile(getClass().getName(), null);
        fileA.deleteOnExit();
        FileWriter out = new FileWriter(fileA);
        out.write("<broker><connector><port>2342</port><ssl><port>4235</port></ssl></connector></broker>");
        out.close();
        ServerConfiguration conf = new ServerConfiguration(fileA);
        conf.initialise();
        assertEquals("4235", conf.getSSLPorts().get(0));
    }

    public void testCombinedConfiguration() throws IOException, ConfigurationException
    {
        File mainFile = File.createTempFile(getClass().getName(), null);
        File fileA = File.createTempFile(getClass().getName(), null);
        File fileB = File.createTempFile(getClass().getName(), null);

        mainFile.deleteOnExit();
        fileA.deleteOnExit();
        fileB.deleteOnExit();

        FileWriter out = new FileWriter(mainFile);
        out.write("<configuration><system/>");
        out.write("<xml fileName=\"" + fileA.getAbsolutePath() + "\"/>");
        out.write("<xml fileName=\"" + fileB.getAbsolutePath() + "\"/>");
        out.write("</configuration>");
        out.close();

        out = new FileWriter(fileA);
        out.write("<broker><connector><port>2342</port><ssl><port>4235</port></ssl></connector></broker>");
        out.close();

        out = new FileWriter(fileB);
        out.write("<broker><connector><ssl><port>2345</port></ssl></connector></broker>");
        out.close();

        ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
        config.initialise();
        assertEquals("4235", config.getSSLPorts().get(0)); // From first file, not
                                                 // overriden by second
        assertNotNull(config.getPorts());
        assertEquals(1, config.getPorts().size());
        assertEquals("2342", config.getPorts().get(0)); // From the first file, not
                                              // present in the second
    }

    public void testVariableInterpolation() throws Exception
    {
        File mainFile = File.createTempFile(getClass().getName(), null);

        mainFile.deleteOnExit();

        FileWriter out = new FileWriter(mainFile);
        out.write("<broker>\n");
        out.write("\t<work>foo</work>\n");
        out.write("\t<management><ssl><keyStorePath>${work}</keyStorePath></ssl></management>\n");
        out.write("</broker>\n");
        out.close();

        ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
        config.initialise();
        assertEquals("Did not get correct interpolated value",
                "foo", config.getManagementKeyStorePath());
    }

    private void writeConfigFile(File mainFile, boolean allow) throws IOException {
        writeConfigFile(mainFile, allow, true, null, "test");
    }

    private void writeConfigFile(File mainFile, boolean allow, boolean includeVhosts, File vhostsFile, String name) throws IOException {
        FileWriter out = new FileWriter(mainFile);
        out.write("<broker>\n");
        out.write("\t<management><enabled>false</enabled></management>\n");
        out.write("\t<security>\n");
        out.write("\t\t<pd-auth-manager>\n");
        out.write("\t\t\t<principal-database>\n");
        out.write("\t\t\t\t<class>org.wso2.andes.server.security.auth.database.PlainPasswordFilePrincipalDatabase</class>\n");
        out.write("\t\t\t\t<attributes>\n");
        out.write("\t\t\t\t\t<attribute>\n");
        out.write("\t\t\t\t\t\t<name>passwordFile</name>\n");
        out.write("\t\t\t\t\t\t<value>/dev/null</value>\n");
        out.write("\t\t\t\t\t</attribute>\n");
        out.write("\t\t\t\t</attributes>\n");
        out.write("\t\t\t</principal-database>\n");
        out.write("\t\t</pd-auth-manager>\n");
        out.write("\t\t<firewall>\n");
        out.write("\t\t\t<rule access=\""+ ((allow) ? "allow" : "deny") +"\" network=\"127.0.0.1\"/>");
        out.write("\t\t</firewall>\n");
        out.write("\t</security>\n");
        if (includeVhosts)
        {
	        out.write("\t<virtualhosts>\n");
            out.write("\t\t<default>test</default>\n");
	        out.write("\t\t<virtualhost>\n");
	        out.write(String.format("\t\t\t<name>%s</name>\n", name));
	        out.write(String.format("\t\t<%s> \n", name));
	        out.write("\t\t\t<exchanges>\n");
	        out.write("\t\t\t\t<exchange>\n");
	        out.write("\t\t\t\t\t<type>topic</type>\n");
	        out.write(String.format("\t\t\t\t\t<name>%s.topic</name>\n", name));
	        out.write("\t\t\t\t\t<durable>true</durable>\n");
	        out.write("\t\t\t\t</exchange>\n");
	        out.write("\t\tt</exchanges>\n");
	        out.write(String.format("\t\t</%s> \n", name));
	        out.write("\t\t</virtualhost>\n");
	        out.write("\t</virtualhosts>\n");
        }
        if (vhostsFile != null)
        {
        	out.write("\t<virtualhosts>"+vhostsFile.getAbsolutePath()+"</virtualhosts>\n");	
        }
        out.write("</broker>\n");
        out.close();
    }

    private void writeTestFishConfigFile(File mainFile) throws IOException {
        FileWriter out = new FileWriter(mainFile);
        out.write("<broker>\n");
        out.write("\t<management><enabled>false</enabled></management>\n");
        out.write("\t<security>\n");
        out.write("\t\t<pd-auth-manager>\n");
        out.write("\t\t\t<principal-database>\n");
        out.write("\t\t\t\t<class>org.wso2.andes.server.security.auth.database.PlainPasswordFilePrincipalDatabase</class>\n");
        out.write("\t\t\t\t<attributes>\n");
        out.write("\t\t\t\t\t<attribute>\n");
        out.write("\t\t\t\t\t\t<name>passwordFile</name>\n");
        out.write("\t\t\t\t\t\t<value>/dev/null</value>\n");
        out.write("\t\t\t\t\t</attribute>\n");
        out.write("\t\t\t\t</attributes>\n");
        out.write("\t\t\t</principal-database>\n");
        out.write("\t\t</pd-auth-manager>\n");
        out.write("\t\t<firewall>\n");
        out.write("\t\t\t<rule access=\"allow\" network=\"127.0.0.1\"/>");
        out.write("\t\t</firewall>\n");
        out.write("\t</security>\n");
        out.write("\t<virtualhosts>\n");
        out.write("\t\t<virtualhost>\n");
        out.write("\t\t\t<name>test</name>\n");
        out.write("\t\t<test> \n");
        out.write("\t\t\t<exchanges>\n");
        out.write("\t\t\t\t<exchange>\n");
        out.write("\t\t\t\t\t<type>topic</type>\n");
        out.write("\t\t\t\t\t<name>test.topic</name>\n");
        out.write("\t\t\t\t\t<durable>true</durable>\n");
        out.write("\t\t\t\t</exchange>\n");
        out.write("\t\tt</exchanges>\n");
        out.write("\t\t</test> \n");
        out.write("\t\t</virtualhost>\n");
        out.write("\t\t<virtualhost>\n");
        out.write("\t\t\t<name>fish</name>\n");
        out.write("\t\t<fish> \n");
        out.write("\t\t\t<exchanges>\n");
        out.write("\t\t\t\t<exchange>\n");
        out.write("\t\t\t\t\t<type>topic</type>\n");
        out.write("\t\t\t\t\t<name>fish.topic</name>\n");
        out.write("\t\t\t\t\t<durable>false</durable>\n");
        out.write("\t\t\t\t</exchange>\n");
        out.write("\t\tt</exchanges>\n");
        out.write("\t\t</fish> \n");
        out.write("\t\t</virtualhost>\n");
        out.write("\t</virtualhosts>\n");
        out.write("</broker>\n");
        out.close();
    }

    private void writeVirtualHostsFile(File vhostsFile, String name) throws IOException {
        FileWriter out = new FileWriter(vhostsFile);
        out.write("<virtualhosts>\n");
        out.write(String.format("\t\t<default>%s</default>\n", name));
        out.write("\t<virtualhost>\n");
        out.write(String.format("\t\t<name>%s</name>\n", name));
        out.write(String.format("\t\t<%s>\n", name));
        out.write("\t\t\t<exchanges>\n");
        out.write("\t\t\t\t<exchange>\n");
        out.write("\t\t\t\t\t<type>topic</type>\n");
        out.write("\t\t\t\t\t<name>test.topic</name>\n");
        out.write("\t\t\t\t\t<durable>true</durable>\n");
        out.write("\t\t\t\t</exchange>\n");
        out.write("\t\tt</exchanges>\n");
        out.write(String.format("\t\t</%s>\n", name));
        out.write("\t</virtualhost>\n");
        out.write("</virtualhosts>\n");
        out.close();
    }

    private void writeMultiVirtualHostsFile(File vhostsFile) throws IOException {
        FileWriter out = new FileWriter(vhostsFile);
        out.write("<virtualhosts>\n");
        out.write("\t<virtualhost>\n");
        out.write("\t\t<name>topic</name>\n");
        out.write("\t\t<topic>\n");
        out.write("\t\t\t<exchanges>\n");
        out.write("\t\t\t\t<exchange>\n");
        out.write("\t\t\t\t\t<type>topic</type>\n");
        out.write("\t\t\t\t\t<name>test.topic</name>\n");
        out.write("\t\t\t\t\t<durable>true</durable>\n");
        out.write("\t\t\t\t</exchange>\n");
        out.write("\t\tt</exchanges>\n");
        out.write("\t\t</topic>\n");
        out.write("\t</virtualhost>\n");
        out.write("\t<virtualhost>\n");
        out.write("\t\t<name>fanout</name>\n");
        out.write("\t\t<fanout>\n");
        out.write("\t\t\t<exchanges>\n");
        out.write("\t\t\t\t<exchange>\n");
        out.write("\t\t\t\t\t<type>fanout</type>\n");
        out.write("\t\t\t\t\t<name>test.fanout</name>\n");
        out.write("\t\t\t\t\t<durable>true</durable>\n");
        out.write("\t\t\t\t</exchange>\n");
        out.write("\t\tt</exchanges>\n");
        out.write("\t\t</fanout>\n");
        out.write("\t</virtualhost>\n");
        out.write("</virtualhosts>\n");
        out.close();
    }

    private void writeMultipleVhostsConfigFile(File mainFile, File[] vhostsFileArray) throws IOException {
        FileWriter out = new FileWriter(mainFile);
        out.write("<broker>\n");
        out.write("\t<management><enabled>false</enabled></management>\n");
        out.write("\t<security>\n");
        out.write("\t\t<pd-auth-manager>\n");
        out.write("\t\t\t<principal-database>\n");
        out.write("\t\t\t\t<class>org.wso2.andes.server.security.auth.database.PlainPasswordFilePrincipalDatabase</class>\n");
        out.write("\t\t\t\t<attributes>\n");
        out.write("\t\t\t\t\t<attribute>\n");
        out.write("\t\t\t\t\t\t<name>passwordFile</name>\n");
        out.write("\t\t\t\t\t\t<value>/dev/null</value>\n");
        out.write("\t\t\t\t\t</attribute>\n");
        out.write("\t\t\t\t</attributes>\n");
        out.write("\t\t\t</principal-database>\n");
        out.write("\t\t</pd-auth-manager>\n");
        out.write("\t\t<firewall>\n");
        out.write("\t\t\t<rule access=\"allow\" network=\"127.0.0.1\"/>");
        out.write("\t\t</firewall>\n");
        out.write("\t</security>\n");
        for (File vhostsFile : vhostsFileArray)
        {
        	out.write("\t<virtualhosts>"+vhostsFile.getAbsolutePath()+"</virtualhosts>\n");
        }
	    out.write("</broker>\n");
        out.close();
    }

    private void writeCombinedConfigFile(File mainFile, File fileA, File fileB) throws Exception
    {
        FileWriter out = new FileWriter(mainFile);
        out.write("<configuration><system/>");
        out.write("<xml fileName=\"" + fileA.getAbsolutePath() + "\"/>");
        out.write("<xml fileName=\"" + fileB.getAbsolutePath() + "\"/>");
        out.write("</configuration>");
        out.close();
    }
    
    /**
     * Test that configuration loads correctly when virtual hosts are specified in the main
     * configuration file only.
     * <p>
     * Test for QPID-2361
     */
    public void testInternalVirtualhostConfigFile() throws Exception
    {
        // Write out config
        File mainFile = File.createTempFile(getClass().getName(), "config");
        mainFile.deleteOnExit();
        writeConfigFile(mainFile, false, true, null, "test");

        // Load config
        ApplicationRegistry.remove();
        ApplicationRegistry reg = new ConfigurationFileApplicationRegistry(mainFile);
        ApplicationRegistry.initialise(reg);

        // Test config
        VirtualHostRegistry virtualHostRegistry = reg.getVirtualHostRegistry();
        String defaultVirtualHost = reg.getConfiguration().getDefaultVirtualHost();
        VirtualHost virtualHost = virtualHostRegistry.getVirtualHost("test");
        Exchange exchange = virtualHost.getExchangeRegistry().getExchange(new AMQShortString("test.topic"));

        assertEquals("Incorrect default host", "test", defaultVirtualHost);
        assertEquals("Incorrect virtualhost count", 1, virtualHostRegistry.getVirtualHosts().size());
        assertEquals("Incorrect virtualhost name", "test", virtualHost.getName());
        assertEquals("Incorrect exchange type", "topic", exchange.getType().getName().toString());
    }
    
    /**
     * Test that configuration loads correctly when virtual hosts are specified in an external
     * configuration file only.
     * <p>
     * Test for QPID-2361
     */
    public void testExternalVirtualhostXMLFile() throws Exception
    {
        // Write out config
        File mainFile = File.createTempFile(getClass().getName(), "config");
        mainFile.deleteOnExit();
        File vhostsFile = File.createTempFile(getClass().getName(), "vhosts");
        vhostsFile.deleteOnExit();
        writeConfigFile(mainFile, false, false, vhostsFile, null);    
        writeVirtualHostsFile(vhostsFile, "test");

        // Load config
        ApplicationRegistry.remove();
        ApplicationRegistry reg = new ConfigurationFileApplicationRegistry(mainFile);
        ApplicationRegistry.initialise(reg);

        // Test config
        VirtualHostRegistry virtualHostRegistry = reg.getVirtualHostRegistry();
        String defaultVirtualHost = reg.getConfiguration().getDefaultVirtualHost();
        VirtualHost virtualHost = virtualHostRegistry.getVirtualHost("test");
        Exchange exchange = virtualHost.getExchangeRegistry().getExchange(new AMQShortString("test.topic"));

        assertEquals("Incorrect default host", "test", defaultVirtualHost);
        assertEquals("Incorrect virtualhost count", 1, virtualHostRegistry.getVirtualHosts().size());
        assertEquals("Incorrect virtualhost name", "test", virtualHost.getName());
        assertEquals("Incorrect exchange type", "topic", exchange.getType().getName().toString());
    }
    
    /**
     * Test that configuration loads correctly when virtual hosts are specified in an external
     * configuration file only, with two vhosts that have different properties.
     * <p>
     * Test for QPID-2361
     */
    public void testExternalMultiVirtualhostXMLFile() throws Exception
    {
        // Write out vhosts
        File vhostsFile = File.createTempFile(getClass().getName(), "vhosts-multi");
        vhostsFile.deleteOnExit();
        writeMultiVirtualHostsFile(vhostsFile);
        
        // Write out config
        File mainFile = File.createTempFile(getClass().getName(), "config");
        mainFile.deleteOnExit();
        writeConfigFile(mainFile, false, false, vhostsFile, null);

        // Load config
        ApplicationRegistry.remove();
        ApplicationRegistry reg = new ConfigurationFileApplicationRegistry(mainFile);
        ApplicationRegistry.initialise(reg);

        // Test config
        VirtualHostRegistry virtualHostRegistry = reg.getVirtualHostRegistry();

        assertEquals("Incorrect virtualhost count", 2, virtualHostRegistry.getVirtualHosts().size());
        
        // test topic host
        VirtualHost topicVirtualHost = virtualHostRegistry.getVirtualHost("topic");
        Exchange topicExchange = topicVirtualHost.getExchangeRegistry().getExchange(new AMQShortString("test.topic"));
        
        assertEquals("Incorrect topic virtualhost name", "topic", topicVirtualHost.getName());
        assertEquals("Incorrect topic exchange type", "topic", topicExchange.getType().getName().toString());
        
        // Test fanout host
        VirtualHost fanoutVirtualHost = virtualHostRegistry.getVirtualHost("fanout");
        Exchange fanoutExchange = fanoutVirtualHost.getExchangeRegistry().getExchange(new AMQShortString("test.fanout"));
        
        assertEquals("Incorrect fanout virtualhost name", "fanout", fanoutVirtualHost.getName());
        assertEquals("Incorrect fanout exchange type", "fanout", fanoutExchange.getType().getName().toString());
    }
    
    /**
     * Test that configuration does not load when virtual hosts are specified in both the main
     * configuration file and an external file. Should throw a {@link ConfigurationException}.
     * <p>
     * Test for QPID-2361
     */
    public void testInternalAndExternalVirtualhostXMLFile() throws Exception
    {
        // Write out vhosts
        File vhostsFile = File.createTempFile(getClass().getName(), "vhosts");
        vhostsFile.deleteOnExit();
        writeVirtualHostsFile(vhostsFile, "test");
        
        // Write out config
        File mainFile = File.createTempFile(getClass().getName(), "config");
        mainFile.deleteOnExit();
        writeConfigFile(mainFile, false, true, vhostsFile, "test");
        
        // Load config
        try
        {
            ApplicationRegistry.remove();
            ApplicationRegistry reg = new ConfigurationFileApplicationRegistry(mainFile);
            ApplicationRegistry.initialise(reg);
            fail("Different virtualhost XML configurations not allowed");
        }
        catch (ConfigurationException ce)
        {
            assertEquals("Incorrect error message", "Only one of external or embedded virtualhosts configuration allowed.", ce.getMessage());
        }
    }
    
    /**
     * Test that configuration does not load when virtual hosts are specified in multiple external
     * files. Should throw a {@link ConfigurationException}.
     * <p>
     * Test for QPID-2361
     */
    public void testMultipleInternalVirtualhostXMLFile() throws Exception
    {
        // Write out vhosts
        File vhostsFileOne = File.createTempFile(getClass().getName(), "vhosts-one");
        vhostsFileOne.deleteOnExit();
        writeVirtualHostsFile(vhostsFileOne, "one");
        File vhostsFileTwo = File.createTempFile(getClass().getName(), "vhosts-two");
        vhostsFileTwo.deleteOnExit();
        writeVirtualHostsFile(vhostsFileTwo, "two");
        
        // Write out config
        File mainFile = File.createTempFile(getClass().getName(), "config");
        mainFile.deleteOnExit();
        writeMultipleVhostsConfigFile(mainFile, new File[] { vhostsFileOne, vhostsFileTwo });
        
        // Load config
        try
        {
            ApplicationRegistry.remove();
            ApplicationRegistry reg = new ConfigurationFileApplicationRegistry(mainFile);
            ApplicationRegistry.initialise(reg);
            fail("Multiple virtualhost XML configurations not allowed");
        }
        catch (ConfigurationException ce)
        {
            assertEquals("Incorrect error message",
                    "Only one external virtualhosts configuration file allowed, multiple filenames found.",
                    ce.getMessage());
        }
    }
    
    /**
     * Test that configuration loads correctly when virtual hosts are specified in an external
     * configuration file in the first of two configurations and embedded in the second. This
     * will throe a {@link ConfigurationException} since the configurations have different 
     * types.
     * <p>
     * Test for QPID-2361
     */
    public void testCombinedDifferentVirtualhostConfig() throws Exception
    {
        // Write out vhosts config
        File vhostsFile = File.createTempFile(getClass().getName(), "vhosts");
        vhostsFile.deleteOnExit();  
        writeVirtualHostsFile(vhostsFile, "external");
        
        // Write out combined config file
        File mainFile = File.createTempFile(getClass().getName(), "main");
        File fileA = File.createTempFile(getClass().getName(), "a");
        File fileB = File.createTempFile(getClass().getName(), "b");
        mainFile.deleteOnExit();
        fileA.deleteOnExit();
        fileB.deleteOnExit();
        writeCombinedConfigFile(mainFile, fileA, fileB);
        writeConfigFile(fileA, false, false, vhostsFile, null);  
        writeConfigFile(fileB, false);

        // Load config
        try
        {
            ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
            config.initialise();
            fail("Different virtualhost XML configurations not allowed");
        }
        catch (ConfigurationException ce)
        {
            assertEquals("Incorrect error message", "Only one of external or embedded virtualhosts configuration allowed.", ce.getMessage());
        }
    }

    /**
     * Test that configuration loads correctly when virtual hosts are specified two overriding configurations
     * each with an embedded virtualhost section. The first configuration section should be used.
     * <p>
     * Test for QPID-2361
     */
    public void testCombinedConfigEmbeddedVirtualhost() throws Exception
    {
        // Write out combined config file
        File mainFile = File.createTempFile(getClass().getName(), "main");
        File fileA = File.createTempFile(getClass().getName(), "a");
        File fileB = File.createTempFile(getClass().getName(), "b");
        mainFile.deleteOnExit();
        fileA.deleteOnExit();
        fileB.deleteOnExit();
        writeCombinedConfigFile(mainFile, fileA, fileB);
        writeConfigFile(fileA, false, true, null, "a");
        writeConfigFile(fileB, false, true, null, "b"); 

        // Load config
        ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
        config.initialise();
        
        // Test config
        VirtualHostConfiguration virtualHost = config.getVirtualHostConfig("a");

        assertEquals("Incorrect virtualhost count", 1, config.getVirtualHosts().length);
        assertEquals("Incorrect virtualhost name", "a", virtualHost.getName());
    }

    /**
     * Test that configuration loads correctly when virtual hosts are specified two overriding configurations
     * each with an external virtualhost XML file. The first configuration file should be used.
     * <p>
     * Test for QPID-2361
     */
    public void testCombinedConfigExternalVirtualhost() throws Exception
    {
        // Write out vhosts config
        File vhostsOne = File.createTempFile(getClass().getName(), "vhosts-one");
        vhostsOne.deleteOnExit();
        writeVirtualHostsFile(vhostsOne, "one");
        File vhostsTwo = File.createTempFile(getClass().getName(), "vhosts-two");
        vhostsTwo.deleteOnExit();
        writeVirtualHostsFile(vhostsTwo, "two");
        
        // Write out combined config file
        File mainFile = File.createTempFile(getClass().getName(), "main");
        File fileA = File.createTempFile(getClass().getName(), "a");
        File fileB = File.createTempFile(getClass().getName(), "b");
        mainFile.deleteOnExit();
        fileA.deleteOnExit();
        fileB.deleteOnExit();
        writeCombinedConfigFile(mainFile, fileA, fileB);
        writeConfigFile(fileA, false, false, vhostsOne, null);
        writeConfigFile(fileB, false, false, vhostsTwo, null);

        // Load config
        ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
        config.initialise();
        
        // Test config
        VirtualHostConfiguration virtualHost = config.getVirtualHostConfig("one");

        assertEquals("Incorrect virtualhost count", 1, config.getVirtualHosts().length);
        assertEquals("Incorrect virtualhost name", "one", virtualHost.getName());
    }

    /**
     * Test that configuration loads correctly when an overriding virtualhost configuration resets
     * a property of an embedded virtualhost section. The overriding configuration property value
     * should be used.
     * <p>
     * Test for QPID-2361
     */
    public void testCombinedConfigEmbeddedVirtualhostOverride() throws Exception
    {
        // Write out combined config file
        File mainFile = File.createTempFile(getClass().getName(), "main");
        File fileA = File.createTempFile(getClass().getName(), "override");
        File fileB = File.createTempFile(getClass().getName(), "config");
        mainFile.deleteOnExit();
        fileA.deleteOnExit();
        fileB.deleteOnExit();
        writeCombinedConfigFile(mainFile, fileA, fileB);
        writeTestFishConfigFile(fileB);
        
        // Write out overriding virtualhosts section
        FileWriter out = new FileWriter(fileA);
        out.write("<broker>\n");
        out.write("<virtualhosts>\n");
        out.write("\t<virtualhost>\n");
        out.write("\t\t<test>\n");
        out.write("\t\t\t<exchanges>\n");
        out.write("\t\t\t\t<exchange>\n");
        out.write("\t\t\t\t\t<durable>false</durable>\n");
        out.write("\t\t\t\t</exchange>\n");
        out.write("\t\tt</exchanges>\n");
        out.write("\t\t</test>\n");
        out.write("\t\t<fish>\n");
        out.write("\t\t\t<exchanges>\n");
        out.write("\t\t\t\t<exchange>\n");
        out.write("\t\t\t\t\t<durable>true</durable>\n");
        out.write("\t\t\t\t</exchange>\n");
        out.write("\t\tt</exchanges>\n");
        out.write("\t\t</fish>\n");
        out.write("\t</virtualhost>\n");
        out.write("</virtualhosts>\n");
        out.write("</broker>\n");
        out.close();

        // Load config
        ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
        config.initialise();
        
        // Test config
        VirtualHostConfiguration testHost = config.getVirtualHostConfig("test");
        ExchangeConfiguration testExchange = testHost.getExchangeConfiguration("test.topic");
        VirtualHostConfiguration fishHost = config.getVirtualHostConfig("fish");
        ExchangeConfiguration fishExchange = fishHost.getExchangeConfiguration("fish.topic");

        assertEquals("Incorrect virtualhost count", 2, config.getVirtualHosts().length);
        assertEquals("Incorrect virtualhost name", "test", testHost.getName());
        assertFalse("Incorrect exchange durable property", testExchange.getDurable());
        assertEquals("Incorrect virtualhost name", "fish", fishHost.getName());
        assertTrue("Incorrect exchange durable property", fishExchange.getDurable());
    }

    /**
     * Test that configuration loads correctly when the virtualhost configuration is a set of overriding
     * configuration files that resets a property of a virtualhost. The opmost overriding configuration
     * property value should be used.
     * <p>
     * Test for QPID-2361
     */
    public void testCombinedVirtualhostOverride() throws Exception
    {
        // Write out combined config file
        File mainFile = File.createTempFile(getClass().getName(), "main");
        File vhostsFile = File.createTempFile(getClass().getName(), "vhosts");
        File fileA = File.createTempFile(getClass().getName(), "vhosts-override");
        File fileB = File.createTempFile(getClass().getName(), "vhosts-base");
        mainFile.deleteOnExit();
        vhostsFile.deleteOnExit();
        fileA.deleteOnExit();
        fileB.deleteOnExit();
        writeConfigFile(mainFile, true, false, vhostsFile, null);
        writeCombinedConfigFile(vhostsFile, fileA, fileB);

        // Write out overriding virtualhosts sections
        FileWriter out = new FileWriter(fileA);
        out.write("<virtualhosts>\n");
        out.write("\t<virtualhost>\n");
        out.write("\t\t<test>\n");
        out.write("\t\t\t<exchanges>\n");
        out.write("\t\t\t\t<exchange>\n");
        out.write("\t\t\t\t\t<durable>false</durable>\n");
        out.write("\t\t\t\t</exchange>\n");
        out.write("\t\tt</exchanges>\n");
        out.write("\t\t</test>\n");
        out.write("\t</virtualhost>\n");
        out.write("</virtualhosts>\n");
        out.close();
        writeVirtualHostsFile(fileB, "test");

        // Load config
        ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
        config.initialise();
        
        // Test config
        VirtualHostConfiguration testHost = config.getVirtualHostConfig("test");
        ExchangeConfiguration testExchange = testHost.getExchangeConfiguration("test.topic");

        assertEquals("Incorrect virtualhost count", 1, config.getVirtualHosts().length);
        assertEquals("Incorrect virtualhost name", "test", testHost.getName());
        assertFalse("Incorrect exchange durable property", testExchange.getDurable());
    }

    /**
     * Test that configuration loads correctly when the virtualhost configuration is a set of overriding
     * configuration files that define multiple virtualhosts, one per file. Only the virtualhosts defined in
     * the topmost file should be used.
     * <p>
     * Test for QPID-2361
     */
    public void testCombinedMultipleVirtualhosts() throws Exception
    {
        // Write out combined config file
        File mainFile = File.createTempFile(getClass().getName(), "main");
        File vhostsFile = File.createTempFile(getClass().getName(), "vhosts");
        File fileA = File.createTempFile(getClass().getName(), "vhosts-one");
        File fileB = File.createTempFile(getClass().getName(), "vhosts-two");
        mainFile.deleteOnExit();
        vhostsFile.deleteOnExit();
        fileA.deleteOnExit();
        fileB.deleteOnExit();
        writeConfigFile(mainFile, true, false, vhostsFile, null);
        writeCombinedConfigFile(vhostsFile, fileA, fileB);

        // Write both virtualhosts definitions
        writeVirtualHostsFile(fileA, "test-one");
        writeVirtualHostsFile(fileB, "test-two");

        // Load config
        ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
        config.initialise();
        
        // Test config
        VirtualHostConfiguration oneHost = config.getVirtualHostConfig("test-one");

        assertEquals("Incorrect virtualhost count", 1, config.getVirtualHosts().length);
        assertEquals("Incorrect virtualhost name", "test-one", oneHost.getName());
    }

    /**
     * Test that a non-existant virtualhost file throws a {@link ConfigurationException}.
     * <p>
     * Test for QPID-2624
     */
    public void testNonExistantVirtualhosts() throws Exception
    {
        // Write out combined config file
        File mainFile = File.createTempFile(getClass().getName(), "main");
        File vhostsFile = new File("doesnotexist");
        mainFile.deleteOnExit();
        writeConfigFile(mainFile, true, false, vhostsFile, null);

        // Load config
        try
        {
            ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
            config.initialise();
        }
        catch (ConfigurationException ce)
        {
            assertEquals("Virtualhosts file does not exist", ce.getMessage());
        }
        catch (Exception e)
        {
            fail("Should throw a ConfigurationException");
        }
    }
    
    /*
     * Tests that the old element security.jmx.access (that used to be used
     * to define JMX access rights) is rejected.
     */
    public void testManagementAccessRejected() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();

        // Check value we set
        _config.setProperty("security.jmx.access(0)", "jmxremote.access");
        _serverConfig = new ServerConfiguration(_config);
        
        try
        {
            _serverConfig.initialise();
            fail("Exception not thrown");
        }
        catch (ConfigurationException ce)
        {
            assertEquals("Incorrect error message",
                    "Validation error : security/jmx/access is no longer a supported element within the configuration xml.",
                    ce.getMessage());
        }
    }

    /*
     * Tests that the old element security.jmx.principal-database (that used to define the
     * principal database used for JMX authentication) is rejected.
     */
    public void testManagementPrincipalDatabaseRejected() throws ConfigurationException
    {
        // Check default
        _serverConfig.initialise();

        // Check value we set
        _config.setProperty("security.jmx.principal-database(0)", "mydb");
        _serverConfig = new ServerConfiguration(_config);

        try
        {
            _serverConfig.initialise();
            fail("Exception not thrown");
        }
        catch (ConfigurationException ce)
        {
            assertEquals("Incorrect error message",
                    "Validation error : security/jmx/principal-database is no longer a supported element within the configuration xml.",
                    ce.getMessage());
        }
    }

    /*
     * Tests that the old element security.principal-databases. ... (that used to define 
     * principal databases) is rejected.
     */
    public void testPrincipalDatabasesRejected() throws ConfigurationException
    {
        _serverConfig.initialise();

        // Check value we set
        _config.setProperty("security.principal-databases.principal-database.class", "myclass");
        _serverConfig = new ServerConfiguration(_config);

        try
        {
            _serverConfig.initialise();
            fail("Exception not thrown");
        }
        catch (ConfigurationException ce)
        {
            assertEquals("Incorrect error message",
                    "Validation error : security/principal-databases is no longer supported within the configuration xml.",
                    ce.getMessage());
        }
    }
}
