package service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;

/**
 * Created by sunyiwei on 2016/3/27.
 */
public class NonPersistentMsgQueue extends BaseClass {
    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //noPersistentProducer
        noPersistentProducer(connection);

        //persistentProducer
        persistentProducer(connection);

        //browser
        browser(connection);

        //close
        connection.close();
    }

    private static void noPersistentProducer(Connection connection) throws JMSException {
        produce(connection, DeliveryMode.NON_PERSISTENT);
    }

    private static void persistentProducer(Connection connection) throws JMSException {
        produce(connection, DeliveryMode.PERSISTENT);
    }

    private static void produce(Connection connection, int mode) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = new ActiveMQQueue(queueName);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(mode);

        final int COUNT = 10;
        for (int i = 0; i < COUNT; i++) {
            TextMessage textMessage = session.createTextMessage("Non-persistent");
            producer.send(textMessage);
        }

        session.close();
    }
}
