package org.wso2.andes.kernel.distrupter;

import java.util.ArrayList;
import java.util.List;

import org.wso2.andes.kernel.AndesAckData;

import com.lmax.disruptor.EventHandler;
import org.wso2.andes.kernel.MessageStoreManager;

/**
 * We do this to make Listener take turns while running. So we can run many copies of these and control number
 * of IO threads through that.
 */

public class AckHandler implements EventHandler<AndesAckData> {
    private MessageStoreManager messageStoreManager;
    private List<AndesAckData> ackList = new ArrayList<AndesAckData>();

    public AckHandler(MessageStoreManager messageStoreManager) {
        this.messageStoreManager = messageStoreManager;
    }

    public void onEvent(final AndesAckData event, final long sequence, final boolean endOfBatch) throws Exception {
        ackList.add(event);
        if (endOfBatch) {
            messageStoreManager.ackReceived(ackList);
            ackList.clear();
        }
    }
}
