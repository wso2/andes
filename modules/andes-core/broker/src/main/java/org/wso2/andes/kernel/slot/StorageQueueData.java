package org.wso2.andes.kernel.slot;

import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;

public class StorageQueueData {

    private String storageQueueName;

    private String destinationName;

    private ProtocolType protocolType;

    private DestinationType destinationType;

    public StorageQueueData(String storageQueueName, String destinationName, ProtocolType protocolType, DestinationType destinationType) {
        this.storageQueueName = storageQueueName;
        this.destinationName = destinationName;
        this.protocolType = protocolType;
        this.destinationType = destinationType;
    }

    public String getStorageQueueName() {
        return storageQueueName;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public ProtocolType getProtocolType() {
        return protocolType;
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }
}
