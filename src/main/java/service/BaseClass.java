package service;

import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by sunyiwei on 2016/3/25.
 */
public class BaseClass {
    final protected static String url = "tcp://localhost:61616";
    final protected static String username = "1091";
    final protected static String password = "10911091";
    final protected static String queueName = "sdywform.queue.1073";
    final protected static int COUNT = 10;

    protected static void stop(ExecutorService executors){
        executors.shutdown();
        while (!executors.isTerminated()) {
            try {
                executors.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected static void producer(Connection connection, int count) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = new ActiveMQQueue(queueName);
        MessageProducer messageProducer = session.createProducer(destination);

        for (int i = 0; i < count; i++) {
            TextMessage textMessage = session.createTextMessage("Hello world");
            messageProducer.send(textMessage);
        }

        session.close();
    }

    protected static void browser(Connection connection) throws JMSException {
        //create session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //create browser
        QueueBrowser browser = session.createBrowser(new ActiveMQQueue(queueName));

        //walk through
        Enumeration enumeration = browser.getEnumeration();
        while (enumeration.hasMoreElements()) {
            Message message = (Message) enumeration.nextElement();
            System.out.println(message.getJMSDeliveryMode());
        }

        session.close();
    }
}
