package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;

public interface ManagedAMQChannel {

    static final String TYPE = "AMQChannel";

    @MBeanAttribute(name = "Name", description = TYPE + " Name")
    String getName();

}
