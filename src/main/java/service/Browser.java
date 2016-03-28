package service;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

/**
 * Created by sunyiwei on 2016/3/27.
 */
public class Browser extends BaseClass {

    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        final int COUNT = 100;

        //producer
        producer(connection, COUNT);

        //browser
        browser(connection);

        System.out.println("DONE!");
        connection.close();
    }
}
