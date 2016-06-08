/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel.management;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.management.mbeans.MessageStatusInformation;
import org.wso2.andes.kernel.slot.SlotDeliveryWorkerManager;

import java.io.File;
import java.io.IOException;

public class MessageStatusInformationMBean implements MessageStatusInformation {


    public MessageStatusInformationMBean() {
    }


    @Override
    public void dumpMessageStatusInfo(String filePath) {
        try {
            File fileToWriteMessageStatus = new File(filePath);
            if (fileToWriteMessageStatus.exists()) {
                fileToWriteMessageStatus.delete();
            }
            fileToWriteMessageStatus.getParentFile().mkdirs();
            fileToWriteMessageStatus.createNewFile();


            //Get slot delivery workers
            SlotDeliveryWorkerManager.getInstance().dumpAllSlotInformationToFile(fileToWriteMessageStatus);

        } catch (AndesException e) {
            throw new RuntimeException("Internal error while dumping message status", e);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create a file to dump message status", e);
        }

    }
}
