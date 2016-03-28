package service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by sunyiwei on 2016/3/25.
 */
public class SelectMessage {
    final private static String url = "tcp://localhost:61616";
    final private static String username = "1091";
    final private static String password = "10911091";
    final private static String queueName = "sdywform.queue.1073";

    public static void main(String[] args) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        final int COUNT = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(COUNT);

        //produce
        producer(connection, COUNT);

        //consume
        consumer(executorService, connection, COUNT);

        //stop
        stop(executorService);

        //stop connection
        connection.stop();
    }

    private static void stop(ExecutorService executors) {
        executors.shutdown();
        while (!executors.isTerminated()) {
            try {
                executors.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void producer(Connection connection, int count) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = new ActiveMQQueue(queueName);
        MessageProducer messageProducer = session.createProducer(destination);
        for (int i = 0; i < count; i++) {
            TextMessage textMessage = session.createTextMessage("Hello world");
            textMessage.setIntProperty("selector", i);

            messageProducer.send(textMessage);
        }

        session.close();
    }

    private static void consumer(ExecutorService executorService, final Connection connection, int count) {
        for (int i = 0; i < count; i++) {
            final int INDEX = i;
            executorService.submit(new Runnable() {
                public void run() {
                    try {
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        Destination destination = new ActiveMQQueue(queueName);

                        MessageConsumer consumer = session.createConsumer(destination, "selector = " + INDEX);
                        while (true) {
                            Message message = consumer.receive();
                            if (message instanceof TextMessage) {
                                System.out.format("%s: Msg = %s, selector = %s. %n", Thread.currentThread().getName(),
                                        ((TextMessage) message).getText(),
                                        message.getStringProperty("selector"));
                            }
                        }
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
