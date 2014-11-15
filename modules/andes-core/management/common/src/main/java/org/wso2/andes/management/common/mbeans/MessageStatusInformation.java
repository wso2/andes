package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanAttribute;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;

public interface MessageStatusInformation {
    static final String TYPE = "MessageStatusInformation";

    @MBeanAttribute(name="Dump Message Status Info" ,description = "Dumping all status of messages processed")
    void dumpMessageStatusInfo(@MBeanOperationParameter(name = "filePath" ,
                                              description = "path to create file") String filePath);
}
