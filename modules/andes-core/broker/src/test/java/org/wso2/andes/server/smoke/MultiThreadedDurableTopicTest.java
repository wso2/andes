package org.wso2.andes.server.smoke;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class MultiThreadedDurableTopicTest {
    private static final AtomicInteger sentCount = new AtomicInteger(); 
    private static final AtomicInteger receivedCount = new AtomicInteger(); 
    int messageCountPerThread = 1000;

    
    protected TextMessage createMessage(int threadIndex, int i, Session session, int globalMsgIndex) throws JMSException{
        return session.createTextMessage(String.valueOf(globalMsgIndex));
    }
    
    protected void postProcessesReceivedMessage(int threadIndex, int i, TextMessage message, int globalMsgReceivedIndex){
        
    }
    

    public  class SimpleProducer extends AbstractJMSTopicClient{
        
        private int threadIndex; 
        public SimpleProducer(String queueName, int threadIndex) {
            super(queueName);
            this.threadIndex = threadIndex;
        }

        @Override
        public void run() {
            int count = 0;
            MessageProducer producer = null; 
            try {
                producer = topicSession.createProducer(topic);
                long start = System.currentTimeMillis();
                for (int i = 0; i < messageCountPerThread; i++) {
                    int globalMsgIndex = sentCount.incrementAndGet();

                    TextMessage textMessage = createMessage(threadIndex, i, topicSession, globalMsgIndex); 
                    producer.send(textMessage);
                    if(count%10 == 0){
                        System.out.println("Thread : " + threadIndex + "   sending message = " + i);
                    }
                    count++;
                }
                double throughput = messageCountPerThread * 1000d / (System.currentTimeMillis() - start);
                System.out.println("Thread : " + threadIndex + " Successfully finished, thoughput=" + throughput);
            } catch (Exception e) {
                e.printStackTrace(); // To change body of catch statement
                                     // use File | Settings | File
                                     // Templates.
            } finally {
                try {
                    topicConnection.close();
                    topicSession.close();
                    producer.close();
                } catch (JMSException e) {
                    e.printStackTrace(); // To change body of catch
                                         // statement use File | Settings |
                                         // File Templates.
                }
            }
        }
    };

    public  class SimpleConsumer extends AbstractJMSTopicClient{
        private int  threadIndex; 
        private int count = 0; 
        private long start = System.currentTimeMillis(); 
        private String queueName; 
        
        public SimpleConsumer(String queueName, int threadIndex) {
            super(queueName);
            this.threadIndex = threadIndex;
            this.queueName = queueName;
        }
        @Override
        public void run() {
            
            
            try {
                // start the connection
                topicConnection.start();

                // create a topic subscriber
                javax.jms.TopicSubscriber topicSubscriber = topicSession.createDurableSubscriber(topic, "Client"+queueName+threadIndex);

                topicSubscriber.setMessageListener(new MessageListener() {
                    
                    @Override
                    public void onMessage(Message message) {
                        try {
                            TextMessage textMessage = (TextMessage) message;
                            int globalreceivedIndex = receivedCount.incrementAndGet();


                            //if (count % 10 == 0) {
                                String messageAsStr = textMessage.getText(); 
                                System.out.println(threadIndex + " got(" + threadIndex + ")" + messageAsStr.substring(0,Math.min(messageAsStr.length(), 4)) + " "+ receivedCount+ "/" + sentCount + " #"+ textMessage.getText().length());
                            //}
                            if (count % 200 == 0) {
                                double throughput = count * 1000d / (System.currentTimeMillis() - start);
                                System.out.println("Received " + count + " mesages, throughput=" + throughput);
                            }
                            count++;
                            postProcessesReceivedMessage(threadIndex, count, textMessage, globalreceivedIndex);

                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                        }                    
                    }
                });

                Thread.sleep(Integer.MAX_VALUE);

         /* TextMessage textMessage = (TextMessage) topicSubscriber.receive();
                System.out.println("Got message ==> " + textMessage.getText());*/

                topicSubscriber.close();
                topicSession.close();
            }catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    };
    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws Exception {
        System.setProperty(org.wso2.andes.configuration.ClientProperties.AMQP_VERSION, "0-91");
        System.setProperty("qpid.dest_syntax" , "BURL");
        
        
        MultiThreadedDurableTopicTest base = new MultiThreadedDurableTopicTest();
        int producerCount = 1; 
        int consumerCount = 1; 
        String queueName = "topic1";

        for(int i = 0;i< consumerCount;i++){
            new Thread(base.new SimpleConsumer(queueName, i)).start(); 
        }
        Thread.sleep(10000);
        
        for(int i = 0;i< producerCount;i++){
            new Thread(base.new SimpleProducer(queueName, i)).start();
        }
    }

}
