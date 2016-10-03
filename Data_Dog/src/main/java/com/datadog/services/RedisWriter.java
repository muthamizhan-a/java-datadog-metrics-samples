package com.datadog.services;

import com.datadog.main.AppMain;
import redis.clients.jedis.Jedis;

public class RedisWriter {

	private static Jedis jedis;
	private static RedisWriter instance;

	public static RedisWriter getInstance( String host )
	{
		if( instance == null )
		{
			instance = new RedisWriter(host);
		}
		return instance;
	}

	private RedisWriter( String host )
	{

		jedis = new Jedis(host, 6379, 1000000);

	}

	public void startWriting( String mapName, String key, String val )
	{
		jedis.hset(mapName, key, val);
		AppMain.STATS_METRIC.increment("redis.insert.count", "INSERT");

	}

}
