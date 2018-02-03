package org.opendedup.cassandra;

import java.net.InetSocketAddress;

import org.opendedup.logging.SDFSLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraMain {
	private InetSocketAddress[] contactPoints = { new InetSocketAddress("localhost",9042)};
	private Cluster cluster = null;
	private Session session = null;
	private String dataCenter ="datacenter1";
	private int replicationFactor = 3;
	private String serviceName = "opendedupe";
	
	public CassandraMain(InetSocketAddress[] contactPoints,String dataCenter,int replicationFactor,String serviceName) {
		this.dataCenter=dataCenter;
		this.contactPoints=contactPoints;
		this.replicationFactor=replicationFactor;
		this.serviceName=serviceName;
	}
	
	public void init() {
		if (cluster == null) {
			cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).build();
			SDFSLogger.getLog().info("Connected Cassandra to cluster: " + cluster.getMetadata().getClusterName());
			session = cluster.connect();
		}
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + serviceName + " WITH replication "
				+ "= {'class':'NetworkTopologyStrategy', '"+this.dataCenter+"': "+this.replicationFactor+"};");
	}
	
	public Session getSession() {
		return this.session;
	}
}
