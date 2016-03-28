package service;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;

import javax.jms.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName: SkyFormConnectionServiceImpl
 * @Description: 云平台连接服务实现类
 * @author: wujiamin
 */

public class SkyFormConnectionServiceImpl implements SkyFormConnectionService {
//	final private String url = "failover:(tcp://192.168.16.33:61616?wireFormat.maxInactivityDuration=0,tcp://192.168.16.33:61616?wireFormat.maxInactivityDuration=0)";
//
//	final private String username = "1091";
//
//	final private String password = "10911091";
//
//	final private String queueName = "sdywform.queue.1073";

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SkyFormConnectionServiceImpl.class);
    final AtomicInteger counter = new AtomicInteger(0);
    final private String url = "tcp://192.168.16.33:61616";
    final private String username = "1091";
    final private String password = "10911091";
    final private String queueName = "sdywform.queue.1073";

    public static void main(String[] args) {
        LOGGER.info("开始监听...");

//        LOGGER.info("开始生产数据...");
//
//        final int THREAD_COUNT = 10;
//        ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
//
//        for (int i = 0; i < THREAD_COUNT; i++) {
//            final int INDEX = i;
//            es.submit(new Runnable() {
//                public void run() {
//                    final int COUNT = 1000;
//                    SkyFormConnectionServiceImpl service = new SkyFormConnectionServiceImpl();
//
//                    for (int j = INDEX * COUNT; j < (INDEX + 1) * COUNT; j++) {
//                        service.messageSend("Hello_" + j);
//                    }
//                }
//            });
//        }
//
//        es.shutdown();
//        while(!es.isTerminated()){
//            try {
//                es.awaitTermination(5, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//
//        LOGGER.info("数据已经生产完毕，开始准备消费...");
        LOGGER.info("准备开始消费数据！！！！");
        new Thread(new Runnable() {
            public void run() {
                new SkyFormConnectionServiceImpl().messageReceive();
            }
        }).start();
    }

    public void messageSend(String request) {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        Connection connection = null;
        Session session = null;
        try {
            //根据用户名和密码创建连接消息队列服务器的连接
            connection = factory.createConnection(username, password);//参数：userName,password

            //创建客户端确认的会话
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            //确定目标队列
            Destination dest = new ActiveMQQueue(queueName);
            MessageProducer sender = session.createProducer(dest);

            //发送请求消息
            TextMessage outMessage = session.createTextMessage();
            outMessage.setText(request);
            sender.send(outMessage);
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /*
     * 从云平台接收消息
     */
    public void messageReceive() {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        Connection connection = null;
        Session session = null;
        try {
            //根据用户名和密码创建连接消息队列服务器的连接
            connection = factory.createConnection(username, password);//参数：userName,password
            connection.start();

            //创建客户端确认的会话
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            //确定目标队列
            Destination dest = new ActiveMQQueue(queueName);
            MessageConsumer consumer = session.createConsumer(dest);

            LOGGER.info("接收云平台推送消息开始执行等待！时间：" + new Date());

            //接受处理消息
            while (true) {
                connection.start();
                Message msg = consumer.receive();

                if (msg instanceof TextMessage) {
                    String body = null;

                    try {
                        body = ((TextMessage) msg).getText();
                        //接受到消息以后的业务处理
                        LOGGER.info("接收到云平台消息开始操作：" + body);

                        //应答
                        msg.acknowledge();
                        LOGGER.info("当前已经接收{}条消息.", counter.incrementAndGet());
                    } catch (JMSException e) {
                        e.printStackTrace();
                        LOGGER.info("JMS连接异常" + e.getMessage());
                    }
                } else {
                    throw new RuntimeException("非法的消息");
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
            LOGGER.info("JMS连接异常" + e.getMessage());
        } catch (Exception e) {
            LOGGER.info("JMS中出现其他异常：" + e.getMessage());
        } finally {
            if (session != null) {
                try {
                    LOGGER.info("云平台推送session已关闭！时间：" + new Date());
                    session.close();
                } catch (Exception e) {
                    LOGGER.info("session关闭异常：" + e.getMessage());
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    LOGGER.info("云平台推送connection已关闭！时间：" + new Date());
                    connection.close();
                } catch (Exception e) {
                    LOGGER.info("connection关闭异常：" + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}
