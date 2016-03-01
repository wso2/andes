package org.wso2.andes.kernel;

import org.apache.commons.lang.StringUtils;

/**
 * The transport protocol type which connects with Andes core.
 */
public class ProtocolType {

    /**
     * The name of the protocol.
     */
    private String protocolName;

    /**
     * The version of the protocol.
     */
    private String version;

    private static String separator = "-";

    /**
     * Constructor with only protocol name.
     * Character '-' is not allowed within a protocol name as it is used as the separator between protocol and version.
     *
     * @param protocolName The name of the protocol
     * @param version The version of the protocol
     * @throws AndesException
     */
    public ProtocolType(String protocolName, String version) throws AndesException {
        setProtocolName(protocolName);
        setVersion(version);
    }

    /**
     * Create a ProtocolType by passing a string.
     *
     * @param protocolType The protocol type as a string.
     * @throws AndesException
     */
    public ProtocolType(String protocolType) throws AndesException {
        if (StringUtils.isNotEmpty(protocolType)) {
            String[] properties = protocolType.split(separator, 2);

            if (properties.length == 2) {
                setProtocolName(properties[0]);
                setVersion(properties[1]);
            } else {
                throw new AndesException("An empty string is given for creating a ProtocolType");
            }
        }
    }

    public String getProtocolName() {
        return protocolName;
    }

    /**
     * Validate and set protocol name.
     * Character '- is not allowed within a protocol name as it is used as the separator within this class.
     *
     * @param protocolName The protocol name to set
     * @throws AndesException
     */
    private void setProtocolName(String protocolName) throws AndesException {
        if (StringUtils.isNotEmpty(protocolName)) {
            if (protocolName.contains(separator)) {
                throw new AndesException("Character '" + separator + "' is not allowed within a protocol name");
            }
            this.protocolName = protocolName;
        } else {
            throw new AndesException("Protocol name cannot be empty");
        }
    }

    public String getVersion() {
        return version;
    }

    private void setVersion(String version) throws AndesException {
        if (StringUtils.isNotEmpty(version)) {
            this.version = version;
        } else {
            throw new AndesException("Protocol version cannot be empty");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProtocolType that = (ProtocolType) o;

        if (!protocolName.equals(that.protocolName)) return false;
        if (!version.equals(that.version)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = protocolName.hashCode();
        result = 31 * result + version.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return protocolName + separator + version;
    }
}
