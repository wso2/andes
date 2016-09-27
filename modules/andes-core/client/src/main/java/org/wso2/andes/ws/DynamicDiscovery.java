/*
* Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.andes.ws;

import com.sun.org.apache.xerces.internal.parsers.DOMParser;
import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.namespace.QName;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dynamic discovery class implements DynamicDiscoveryWebServices. Web service is calling inside this class.
 */

public class DynamicDiscovery implements DynamicDiscoveryWebServices {

    private static HashMap<String, List<String>> hashMap = new HashMap<>();
    private final Logger log = LoggerFactory.getLogger(DynamicDiscovery.class);

    @Override
    public List<String> getLocalDynamicDiscoveryDetails(String[] initialAddress, String userName, String passWord,
                                                        String mode, String trustStore) {
         List<String> brokerDetailsList = new ArrayList<>();
         List<String> checkerList = new ArrayList<>();

        int count = 0;
        int maxTries = initialAddress.length;
        String decision = "port";

        //Selecting mode ssl or default
        if (mode.equals("ssl")) {
            decision = "ssl-port";
        }


        for (int i = count; i < maxTries; ) {
            try {
                String endpoint = "https://" + initialAddress[count] + "/services/AndesManagerService";
                System.setProperty(
                        "javax.net.ssl.trustStore", trustStore);
                Service service = new Service();
                Call call = (Call) service.createCall();
                call.setTargetEndpointAddress(new java.net.URL(endpoint));
                call.setOperationName(new QName("http://mgt.cluster.andes.carbon.wso2.org", "getBrokerInformation"));
                call.setUsername(userName);
                call.setPassword(passWord);

                String xmlResponse = (String) call.invoke(new Object[]{});
                DOMParser parser = new DOMParser();
                parser.parse(new InputSource(new java.io.StringReader(xmlResponse)));
                Document doc = parser.getDocument();


                for (int j = 0; j < doc.getDocumentElement().getElementsByTagName("Address").getLength(); ) {

                    try {
                        String nodeId = doc.getDocumentElement().getElementsByTagName("Address").item(j).getParentNode
                                ().getParentNode().getAttributes().getNamedItem("id").getNodeValue();

                        String Ip = doc.getDocumentElement().getElementsByTagName("Address").item(j).getAttributes()
                                .getNamedItem("ip").getNodeValue();

                        int port = Integer.parseInt(doc.getDocumentElement().getElementsByTagName("Address").item(j)
                                .getAttributes()
                                .getNamedItem(decision).getNodeValue());

                        String interface_name = doc.getDocumentElement().getElementsByTagName("Address").item(j).getAttributes()
                                .getNamedItem("interface-name").getNodeValue();

                        if (hashMap.containsKey(nodeId)) {
                            for (Object object : hashMap.entrySet()) {
                                Map.Entry entry = (Map.Entry) object;
                                String key = entry.getKey().toString();

                                if (key.equals(nodeId)) {
                                    List entryValue = (List) entry.getValue();

                                    if (entryValue.contains(interface_name)) {
                                        brokerDetailsList.add(Ip + ":" + port);
                                        j++;
                                    } else {
                                        Socket socket = new Socket();
                                        socket.connect(new InetSocketAddress(Ip, port), 1000);
                                        brokerDetailsList.add(Ip + ":" + port);
                                        checkerList.add(interface_name);
                                        hashMap.put(nodeId, checkerList);
                                        j++;
                                    }
                                }
                            }
                        } else {
                            Socket socket = new Socket();
                            socket.connect(new InetSocketAddress(Ip, port), 1000);
                            brokerDetailsList.add(Ip + ":" + port);
                            checkerList.add(interface_name);
                            hashMap.put(nodeId, checkerList);
                            j++;
                        }
                    } catch (Exception e) {
                        j++;
                    }
                }
                break;
            } catch (Exception e) {

                count++;
                if (count == maxTries) {
                    log.error("Error while calling dynamic discovery web service", e);
                }
            }
        }
        return brokerDetailsList;
    }
}
