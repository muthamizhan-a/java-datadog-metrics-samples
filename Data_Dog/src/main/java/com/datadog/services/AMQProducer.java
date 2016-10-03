package com.datadog.services;

import java.util.Date;
import java.util.HashMap;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import com.datadog.main.AppMain;

public class AMQProducer {

	private static Session session;
	private static AMQProducer instance;
	private HashMap<String, MessageProducer> producers = new HashMap<>();

	public static AMQProducer getInstance( String amqHost )
	{
		if( instance == null )
		{
			instance = new AMQProducer(amqHost);
		}
		return instance;
	}

	private AMQProducer( String amqHost )
	{
		try
		{
			String url = "tcp://" + amqHost + ":61616";
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
			// Create a Connection
			Connection connection = connectionFactory.createConnection();
			connection.start();

			// Create a Session
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}

	}

	public void sendMessage( String queueName, String msg ) throws Exception
	{
		MessageProducer producer = null;

		if( !producers.containsKey(queueName) )
		{
			// Create the destination (Topic or Queue)
			Destination destination = session.createQueue(queueName);

			// Create a MessageProducer from the Session to the Topic or Queue
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			producers.put(queueName, producer);
			AppMain.STATS_METRIC.increment("amq.records.count", "PRODUCER");

		}
		else
		{
			producer = producers.get(queueName);
		}

		try
		{
			TextMessage message = session.createTextMessage(msg);
			producer.send(message);
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}
		// Clean up
		//		session.close();
	}

}
