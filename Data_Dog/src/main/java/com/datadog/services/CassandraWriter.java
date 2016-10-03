package com.datadog.services;

import java.util.UUID;
import com.datadog.main.AppMain;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.razorthink.blazentcommon.config.CassandraConfig;

public class CassandraWriter {

	static Session cassandraSession;
	static PreparedStatement statement;
	static BoundStatement boundStatement;

	static CassandraWriter instance;

	public static CassandraWriter getInstance( String cassandraHost )
	{

		if( instance == null )
		{

			instance = new CassandraWriter(cassandraHost);
		}
		return instance;

	}

	private CassandraWriter( String host )
	{

		cassandraSession = Cluster.builder().addContactPoints(host).build().connect();

		// To Create keyspace
		String createKeyspaceCommand = "CREATE KEYSPACE IF NOT EXISTS DEMO"
				+ " WITH replication =  {'class':'NetworkTopologyStrategy', 'datacenter1':'"
				+ CassandraConfig.CASSANDRA_REPLICATION_FACTOR.trim() + "'};";

		statement = cassandraSession.prepare(createKeyspaceCommand);
		BoundStatement boundStatement = new BoundStatement(statement);
		cassandraSession.execute(boundStatement);

		// To create table if not exist
		String createTableCommand = "CREATE TABLE IF NOT EXISTS DEMO.data_dog ( id text, message text, PRIMARY KEY (id, message))";

		statement = cassandraSession.prepare(createTableCommand);
		boundStatement = new BoundStatement(statement);
		cassandraSession.execute(boundStatement);

	}

	public void insertDataToCassandra( String msg )
	{

		// To insert config
		statement = cassandraSession.prepare("insert into DEMO.data_dog(id,message) values(?,?)");// +
		boundStatement = new BoundStatement(statement);
		boundStatement.bind(UUID.randomUUID() + "", msg);
		cassandraSession.execute(boundStatement);

		AppMain.STATS_METRIC.increment("cassandra.insert.count", "INSERT");
	}
}
