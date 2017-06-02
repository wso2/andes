package org.wso2.andes.server.information.management;

import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.management.common.mbeans.MessageStatusInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanConstructor;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.management.AMQManagedObject;

import javax.management.JMException;
import java.io.File;
import java.io.IOException;

public class MessageStatusInformationMBean extends AMQManagedObject
        implements MessageStatusInformation {


    @MBeanConstructor("Creates an MBean exposing an Cluster Manager")
    public MessageStatusInformationMBean() throws JMException {
        super(MessageStatusInformation.class, MessageStatusInformation.TYPE);
    }

    @Override
    public String getObjectInstanceName() {
        return MessageStatusInformation.TYPE;
    }

    @Override
    public void dumpMessageStatusInfo(
            @MBeanOperationParameter(name = "filePath", description = "path to dump " +
                                                                      "message status info")
                                                                       String filePath) {
        try {
            File fileToWriteMessageStatus = new File(filePath);
            if (fileToWriteMessageStatus.exists()) {
                fileToWriteMessageStatus.delete();
            }
            fileToWriteMessageStatus.getParentFile().mkdirs();
            fileToWriteMessageStatus.createNewFile();


        } catch (IOException e) {
            throw new RuntimeException("Cannot create a file to dump message status", e);
        }

    }
}
