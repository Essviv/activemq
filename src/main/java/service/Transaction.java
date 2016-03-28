package service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;
import java.util.Date;

/**
 * Created by sunyiwei on 2016/3/27.
 */
public class Transaction extends BaseClass {
    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //produce
        producer(connection);

        //consumer
        consumer(connection);
    }

    private static void producer(Connection connection) throws JMSException {
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Destination destination = new ActiveMQQueue(queueName);

        final int COUNT = 10;
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < COUNT; i++) {
            producer.send(session.createTextMessage("Hello world"));
        }
        session.rollback();

        for (int i = 0; i < COUNT; i++) {
            producer.send(session.createTextMessage("Hello world"));
        }
        session.commit();
        session.close();
    }

    private static void consumer(Connection connection) throws JMSException {
        Session session = connection.createSession(false , Session.CLIENT_ACKNOWLEDGE);
        Destination destination = new ActiveMQQueue(queueName);
        MessageConsumer consumer = session.createConsumer(destination);

        while (true) {
            Message message = consumer.receive();

            if (message instanceof TextMessage) {
                System.out.println(new Date() + ": " + ((TextMessage) message).getText());
            }
        }
    }
}
