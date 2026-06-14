/*
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
 */
package org.wso2.andes.configuration.qpid;

import junit.framework.TestCase;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLClassLoader;

public class ServerConfigurationTcclTest extends TestCase {

    private ClassLoader originalTccl;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        originalTccl = Thread.currentThread().getContextClassLoader();
    }

    @Override
    protected void tearDown() throws Exception {
        // Always restore the TCCL so that later tests are not polluted.
        Thread.currentThread().setContextClassLoader(originalTccl);
        super.tearDown();
    }

    /**
     * Verifies that the TCCL is restored to the caller's original value after
     * ServerConfiguration successfully parses a flat XML broker config file.
     *
     * Before the fix the TCCL switch was absent, so in an OSGi environment the
     * ServiceLoader used by commons-configuration2 could not find its providers.
     * The try/finally restoration is what this test guards.
     */
    public void testTcclRestoredAfterSuccessfulFlatXmlParse() throws IOException, ConfigurationException {
        File configFile = createTempXml(
                "<broker><connector><port>5672</port></connector></broker>");

        ClassLoader sentinelLoader = new URLClassLoader(new java.net.URL[0],
                Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(sentinelLoader);

        new ServerConfiguration(configFile.getAbsoluteFile());

        assertSame(
                "TCCL must be restored to the sentinel loader after successful parseConfig()",
                sentinelLoader,
                Thread.currentThread().getContextClassLoader());
    }

    /**
     * Verifies that the TCCL is restored to the caller's original value after
     * ServerConfiguration successfully parses a CombinedConfiguration XML file.
     */
    public void testTcclRestoredAfterSuccessfulCombinedConfigParse() throws IOException, ConfigurationException {
        File included = createTempXml(
                "<broker><connector><port>5672</port></connector></broker>");
        File mainFile = createTempXml(
                "<configuration><system/>" +
                "<xml fileName=\"" + included.getAbsolutePath() + "\"/>" +
                "</configuration>");

        ClassLoader sentinelLoader = new URLClassLoader(new java.net.URL[0],
                Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(sentinelLoader);

        new ServerConfiguration(mainFile.getAbsoluteFile());

        assertSame(
                "TCCL must be restored to the sentinel loader after CombinedConfiguration parseConfig()",
                sentinelLoader,
                Thread.currentThread().getContextClassLoader());
    }

    /**
     * Verifies that the TCCL is restored even when the config file cannot be found,
     * causing parseConfig() to throw ConfigurationException.
     *
     * This guards the finally block in the TCCL-switch pattern added by the fix.
     */
    public void testTcclRestoredAfterMissingConfigFile() {
        File nonExistentFile = new File(System.getProperty("java.io.tmpdir"),
                "does-not-exist-17211-" + System.nanoTime() + ".xml");

        ClassLoader sentinelLoader = new URLClassLoader(new java.net.URL[0],
                Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(sentinelLoader);

        try {
            new ServerConfiguration(nonExistentFile.getAbsoluteFile());
            fail("Expected ConfigurationException for a missing config file");
        } catch (ConfigurationException expected) {
            // expected
        }

        assertSame(
                "TCCL must be restored to the sentinel loader even when parseConfig() throws",
                sentinelLoader,
                Thread.currentThread().getContextClassLoader());
    }

    public void testFlatXmlConfigValuesReadCorrectly() throws IOException, ConfigurationException {
        File configFile = createTempXml(
                "<broker><housekeeping><checkPeriod>12345</checkPeriod></housekeeping></broker>");

        ServerConfiguration config = new ServerConfiguration(configFile.getAbsoluteFile());
        config.initialise();

        assertEquals("housekeeping.checkPeriod should be read from flat XML config",
                12345L, config.getHousekeepingCheckPeriod());
    }

    public void testCombinedConfigValuesReadCorrectly() throws IOException, ConfigurationException {
        File fileA = createTempXml(
                "<broker><housekeeping><checkPeriod>12345</checkPeriod></housekeeping></broker>");
        File fileB = createTempXml(
                "<broker><housekeeping><checkPeriod>99999</checkPeriod></housekeeping></broker>");
        File mainFile = createTempXml(
                "<configuration><system/>" +
                "<xml fileName=\"" + fileA.getAbsolutePath() + "\"/>" +
                "<xml fileName=\"" + fileB.getAbsolutePath() + "\"/>" +
                "</configuration>");

        ServerConfiguration config = new ServerConfiguration(mainFile.getAbsoluteFile());
        config.initialise();

        // Values from fileA take precedence over fileB (first file wins).
        assertEquals("housekeeping.checkPeriod from first included file should win",
                12345L, config.getHousekeepingCheckPeriod());
    }

    // -----------------------------------------------------------------------

    private File createTempXml(String content) throws IOException {
        File tmp = File.createTempFile("ServerConfigurationTcclTest-", ".xml");
        tmp.deleteOnExit();
        try (FileWriter fw = new FileWriter(tmp)) {
            fw.write(content);
        }
        return tmp;
    }
}
