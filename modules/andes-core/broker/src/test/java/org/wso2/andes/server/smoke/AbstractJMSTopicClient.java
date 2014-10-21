package org.wso2.andes.server.smoke;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public abstract class AbstractJMSTopicClient implements Runnable {
    private String initialContextFactory = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='tcp://localhost:5672'";
    //private String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='tcp://10.100.3.125:5672'";
    protected TopicConnection topicConnection = null;
    TopicSession topicSession = null;
    int threadIndex = -1;
    ConnectionFactory connectionFactory;
    InitialContext initialContext;
    protected Topic topic;

    public AbstractJMSTopicClient(String topicName) {
        Properties properties = new Properties();
        properties.put("java.naming.factory.initial", initialContextFactory);
        properties.put("connectionfactory.QueueConnectionFactory", connectionString);
        properties.put("topic." + topicName, topicName);

        try {
            // initialize
            // the required connection factories
            InitialContext ctx = new InitialContext(properties);
            TopicConnectionFactory topicConnectionFactory = (TopicConnectionFactory) ctx
                    .lookup("QueueConnectionFactory");
            topicConnection = topicConnectionFactory.createTopicConnection();

            topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            topic = (Topic) ctx.lookup(topicName);

            if (topic == null) {
                throw new RuntimeException("Destination null");
            }
            System.out.println("Seding to topic " + topic.toString());
        } catch (NamingException e) {
            throw new RuntimeException("Unable to send jms messages", e);
        } catch (JMSException e) {
            throw new RuntimeException("Unable to send jms messages", e);
        }
    }
}
