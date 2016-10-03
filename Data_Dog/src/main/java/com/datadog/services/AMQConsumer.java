package com.datadog.services;

import java.util.HashMap;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import com.datadog.main.AppMain;
import com.razorthink.blazentcommon.service.ActiveMQConsumer;

public class AMQConsumer extends Thread {

	private HashMap<String, MessageConsumer> messageConsumer = new HashMap<>();
	ActiveMQConsumer amqConsumer;
	private static AMQConsumer instance = null;
	MessageConsumer consumer = null;

	private AMQConsumer( String host )
	{
		amqConsumer = new ActiveMQConsumer(host, "61616", "1000");
	}

	public static AMQConsumer getInstance( String hostIp )
	{
		if( instance == null )
			instance = new AMQConsumer(hostIp);
		return instance;
	}

	public void run()
	{
		try
		{
			while( true )
			{
				Long startTime = System.currentTimeMillis();
				Message message = consumer.receive(1000);
				if( message instanceof TextMessage )
				{

					TextMessage textMessage = (TextMessage) message;
					AppMain.STATS_METRIC.increment("amq.records.count", "CONSUMER");
					Thread.sleep(1000);

					AppMain.STATS_METRIC.gauge("amq.records" + "messages.waiting.size",
							startTime - System.currentTimeMillis());
				}
			}
		}
		catch( Exception e )
		{
			e.printStackTrace();
		}
	}

	public void initializeConsumerQueue( String queueName )
	{
		System.out.println("CONSUMER STARTED!!!!!");
		if( !messageConsumer.containsKey(queueName) )
		{
			consumer = amqConsumer.getQueueConsumer(queueName);
			messageConsumer.put(queueName, consumer);
		}
		else
		{
			consumer = messageConsumer.get(queueName);
		}
	}

}
