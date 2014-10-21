package org.wso2.andes.server.smoke;

import javax.jms.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadedTest {
    private static final AtomicInteger sentCount = new AtomicInteger(); 
    private static final AtomicInteger receivedCount = new AtomicInteger(); 
    int messageCountPerThread = 1000;
    private boolean closeSesionPerMessage = true; 

    
    protected TextMessage createMessage(int threadIndex, int i, Session session, int globalMsgIndex) throws JMSException{
        return session.createTextMessage(threadIndex+ ":" +String.valueOf(i));
    }
    
    protected void postProcessesReceivedMessage(int threadIndex, int i, TextMessage message, int globalMsgReceivedIndex){
        
    }
    

    public  class SimpleProducer extends org.wso2.andes.server.smoke.AbstractJMSClient {
        
        private int threadIndex; 
        public SimpleProducer(String queueName, int threadIndex) {
            super(queueName);
            this.threadIndex = threadIndex;
        }

        @Override
        public void run() {
            int count = 0;
            MessageProducer producer = null; 
            long start = System.currentTimeMillis();

            try {
                if(closeSesionPerMessage){
                    session.close();
                    for (int i = 0; i < messageCountPerThread; i++) {
                        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        producer = session.createProducer(destination);
                        int globalMsgIndex = sentCount.incrementAndGet();
                        TextMessage textMessage = createMessage(threadIndex, i, session, globalMsgIndex); 
                        producer.send(textMessage);
                        if(count%10 == 0){
                            System.out.println("Thread : " + threadIndex + "   sending message = " + i);
                        }
                        count++;
                        producer.close();
                        session.close();
                    }
                }else{
                    producer = session.createProducer(destination);
                    for (int i = 0; i < messageCountPerThread; i++) {
                        int globalMsgIndex = sentCount.incrementAndGet();

                        TextMessage textMessage = createMessage(threadIndex, i, session, globalMsgIndex); 
                        producer.send(textMessage);
                        if(count%10 == 0){
                            System.out.println("Thread : " + threadIndex + "   sending message = " + i);
                        }
                        count++;
                    }
                    producer.close();
                    session.close();
                }
                double throughput = messageCountPerThread * 1000d / (System.currentTimeMillis() - start);
                System.out.println("Thread : " + threadIndex + " Successfully finished, thoughput=" + throughput);
            } catch (Exception e) {
                e.printStackTrace(); // To change body of catch statement
                                     // use File | Settings | File
                                     // Templates.
            } finally {
                try {
                    con.close();
                } catch (JMSException e) {
                    e.printStackTrace(); // To change body of catch
                                         // statement use File | Settings |
                                         // File Templates.
                }
            }
        }
    };

    public  class SimpleConsumer extends AbstractJMSClient{
        private int  threadIndex; 
        public SimpleConsumer(String queueName, int threadIndex) {
            super(queueName);
            this.threadIndex = threadIndex;
        }
        @Override
        public void run() {
            MessageConsumer messageConsumer = null;
            try {
                int count = 0;

                long start = System.currentTimeMillis();
                messageConsumer = session.createConsumer(destination);
                while (true) {

                    try {
                        TextMessage textMessage = (TextMessage) messageConsumer.receive();
                        int globalreceivedIndex = receivedCount.incrementAndGet();


                        //if (count % 100 == 0) {
                          String msgAsStr = textMessage.getText(); 
                            System.out.println(threadIndex+ " got(" + count + ")" + msgAsStr.substring(0, (int)Math.min(7, msgAsStr.length())) + " "+ receivedCount+ "/" + sentCount);
                        //}
                        if (count % 200 == 0) {
                            double throughput = count * 1000d / (System.currentTimeMillis() - start);
                            System.out.println("Received " + count + " mesages, throughput=" + throughput);
                        }
                        count++;
                        postProcessesReceivedMessage(threadIndex, count, textMessage, globalreceivedIndex);

                    } catch (JMSException e) {
                        e.printStackTrace();
                    } finally {
                    }

                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }finally{
                try {
                    messageConsumer.close();
                    session.close();
                    con.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }
    };
    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws Exception {
        MultiThreadedTest base = new MultiThreadedTest();
        int producerCount = 10; 
        int consumerCount = 10; 
        String queueName = "queue1";

        for(int i = 0;i< consumerCount;i++){
            new Thread(base.new SimpleConsumer(queueName, i)).start(); 
        }
        Thread.sleep(10000);
        
        for(int i = 0;i< producerCount;i++){
            new Thread(base.new SimpleProducer(queueName, i)).start();
        }
    }

}
