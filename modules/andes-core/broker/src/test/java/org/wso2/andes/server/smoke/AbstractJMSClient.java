package org.wso2.andes.server.smoke;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public abstract class AbstractJMSClient implements Runnable{
    protected String jmsurl = "tcp://localhost:5672"; 
    Connection con = null;
    Session session = null;
    int threadIndex  = -1;
    ConnectionFactory connectionFactory;
    InitialContext initialContext;
    protected Destination destination; 
    
    public AbstractJMSClient(String queueName) {
        Properties initialContextProperties = new Properties();
        initialContextProperties.put("java.naming.factory.initial",
                "org.wso2.andes.jndi.PropertiesFileInitialContextFactory");
        String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='"+jmsurl+"'";
        initialContextProperties.put("connectionfactory.qpidConnectionfactory", connectionString);
        initialContextProperties.put("queue." + queueName, queueName);


        try {
            initialContext = new InitialContext(initialContextProperties);
            connectionFactory = (ConnectionFactory) initialContext.lookup("qpidConnectionfactory");
            con = connectionFactory.createConnection();
            con.start();
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = (Destination) initialContext.lookup(queueName);
            if (destination == null) {
                throw new RuntimeException("Destination null");
            }
            System.out.println("Seding to queue "+ destination.toString());
        } catch (NamingException e) {
            throw new RuntimeException("Unable to send jms messages", e);
        } catch (JMSException e) {
            throw new RuntimeException("Unable to send jms messages", e);
        }
    }
    
    
    
    public static Connection createConnection(String qeueName, String jmsUrl){
        Properties initialContextProperties = new Properties();
        initialContextProperties.put("java.naming.factory.initial",
                "org.wso2.andes.jndi.PropertiesFileInitialContextFactory");
        String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='"+jmsUrl+"'";
        initialContextProperties.put("connectionfactory.qpidConnectionfactory", connectionString);
        initialContextProperties.put("queue." + qeueName, qeueName);


        try {
            InitialContext initialContext = new InitialContext(initialContextProperties);
            ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("qpidConnectionfactory");
            Connection con = connectionFactory.createConnection();
            con.start();
            return con;
        } catch (NamingException e) {
            throw new RuntimeException("Unable to send jms messages", e);
        } catch (JMSException e) {
            throw new RuntimeException("Unable to send jms messages", e);
        }

    } 
}
