package org.opendedup.cassandra;

import org.apache.ignite.Ignite;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Apache Ignite usage on Kubernetes requires that each node (Pod in the
 * Kubernetes lingo) is able to access other pods directly. Proxies will not
 * work.
 * <p>
 * In order to find the desired pods IP addresses, one could query the
 * Kubernetes API for endpoints of a Service but that may prove too much of a
 * hurdle to maintain. Since DNS integration is a common best-practice when
 * assembling Kubernetes clusters, we will rely on SRV lookups.
 */
public class CassandraIpFinder extends TcpDiscoveryVmIpFinder {

	InetSocketAddress[] contactPoints = { new InetSocketAddress("localhost",9042)};
	private Cluster cluster = null;
	private Session session = null;
	private String dataCenter ="datacenter1";
	private int replicationFactor = 3;

	/**
	 * Grid logger.
	 */
	@LoggerResource
	private IgniteLogger log;

	@GridToStringInclude
	private String serviceName = "opendedupe";

	public CassandraIpFinder() {
		super(true);
	}

	/**
	 * Parses provided service name.
	 *
	 * @param serviceName
	 *            the service name to use in lookup queries.
	 * @throws IgniteSpiException
	 */
	@IgniteSpiConfiguration(optional = true)
	public synchronized void setServiceName(String serviceName) throws IgniteSpiException {
		this.serviceName = serviceName;
	}

	@IgniteSpiConfiguration(optional = true)
	public synchronized void setCassandraContactPoints(InetSocketAddress[] contactPoints) throws IgniteSpiException {
		this.contactPoints = contactPoints;
	}
	
	@IgniteSpiConfiguration(optional = true)
	public synchronized void setCassandraDataCenter(String dataCenter) throws IgniteSpiException {
		this.dataCenter = dataCenter;
	}
	
	private void createTableSpace() {
		if (cluster == null) {
			cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).build();

			log.info("Connected Cassandra to cluster: " + cluster.getMetadata().getClusterName());

			session = cluster.connect();
		}
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + serviceName + " WITH replication "
				+ "= {'class':'NetworkTopologyStrategy', '"+this.dataCenter+"': "+this.replicationFactor+"};");
		session.execute("CREATE TABLE IF NOT EXISTS " + serviceName + ".igep (" + "id text PRIMARY KEY," + "host text,"
				+ "port int);");
	}

	public synchronized void registerAddresses(Collection<InetSocketAddress> addrs) {
		this.createTableSpace();
		for(InetSocketAddress addr : addrs) {
			session.execute("INSERT INTO " + serviceName + ".igep (id, host,port) VALUES ('"+addr.getHostString()+":" + addr.getPort()+"','"+addr.getHostString()+"',"+addr.getPort()+") IF NOT EXISTS");
		}
		
		
	}
	
	public synchronized void unregisterAddresses(Collection<InetSocketAddress> addrs) {
		this.createTableSpace();
		for(InetSocketAddress addr : addrs) {
			session.execute("DELETE FROM " + serviceName + ".igep where id='"+addr.getHostString()+":" + addr.getPort()+"'");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized Collection<InetSocketAddress> getRegisteredAddresses() {
		this.createTableSpace();
		ResultSet set = session.execute("SELECT * from " + serviceName + ".igep;");
		List<Row> rows = set.all();
		// resolve configured addresses
		final Collection<InetSocketAddress> inets = new CopyOnWriteArrayList<>();
		for(Row r : rows) {
			log.info("adding " + r.getString(0));
			inets.add(new InetSocketAddress(r.getString("host"),r.getInt("port")));
		}
		return inets;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return S.toString(CassandraIpFinder.class, this, "super", super.toString());
	}
	
	public static void main(String [] args) {
		CassandraIpFinder cip = new CassandraIpFinder();
		TcpDiscoverySpi spi = new TcpDiscoverySpi();
		spi.setIpFinder(cip);
		InetSocketAddress [] cipep = {new InetSocketAddress("192.168.0.105",9042)};
		cip.setCassandraContactPoints(cipep);
		IgniteConfiguration cfg = new IgniteConfiguration();
		 
		// Override default discovery SPI.
		cfg.setDiscoverySpi(spi);
		 
		// Start Ignite node.
		Ignite ig= Ignition.start(cfg);
		ig.close();
		
		
	}

}