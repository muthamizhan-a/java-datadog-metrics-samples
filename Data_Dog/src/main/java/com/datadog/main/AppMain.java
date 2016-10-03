package com.datadog.main;

import java.util.UUID;
import com.datadog.services.AMQConsumer;
import com.datadog.services.AMQProducer;
import com.datadog.services.CassandraWriter;
import com.datadog.services.RedisWriter;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class AppMain {

	private static String HOST = "localhost";

	public static final StatsDClient STATS_METRIC = new NonBlockingStatsDClient("DataDogApp.App", HOST, 8125);

	private static CassandraWriter cassandraWriter = CassandraWriter.getInstance(HOST);
	private static RedisWriter redisWriter = RedisWriter.getInstance(HOST);
	private static AMQProducer producer = AMQProducer.getInstance(HOST);
	private static AMQConsumer consumer = AMQConsumer.getInstance(HOST);

	public static void main( String[] args ) throws Exception
	{
		runTasks();
	}

	private static void runTasks() throws Exception
	{
		String msg = "RANDOM MESSAGE:";

		consumer.initializeConsumerQueue("SAMPLE");

		consumer.start();
		while( true )
		{
			Long startTime = System.currentTimeMillis();

			cassandraWriter.insertDataToCassandra(msg + UUID.randomUUID());
			System.out.println("Cassandra insert success.");

			redisWriter.startWriting("SAMPLE", UUID.randomUUID() + "", msg);
			System.out.println("Redis insert success.");

			producer.sendMessage("SAMPLE", msg);
			System.out.println("AMQ success.");

			Thread.sleep(500);
			STATS_METRIC.time("main.time", startTime - System.currentTimeMillis(), "DATA_DOG.AppMain");
		}
	}
}
