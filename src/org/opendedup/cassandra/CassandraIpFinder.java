package org.opendedup.cassandra;

import org.apache.ignite.IgniteLogger;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.opendedup.sdfs.Main;

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

	String[] contactPoints = { "127.0.0.1" };
	int port = 9042;
	private Cluster cluster = null;
	private Session session = null;

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

	public CassandraIpFinder(boolean shared) {
		super(shared);
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
	public synchronized void setCassandraContactPoints(String[] contactPoints) throws IgniteSpiException {
		this.contactPoints = contactPoints;
	}

	public synchronized void setCassandraPort(int port) throws IgniteSpiException {
		this.port = port;
	}

	public synchronized void registerAddresses(Collection<InetSocketAddress> addrs) {
		if (cluster == null) {
			cluster = Cluster.builder().addContactPoints(contactPoints).withPort(port).build();

			log.info("Connected Cassandra to cluster: " + cluster.getMetadata().getClusterName());

			session = cluster.connect();
		}
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + serviceName + " WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor': 3};");
		session.execute("CREATE TABLE IF NOT EXISTS " + serviceName + ".igep (" + "id uuid PRIMARY KEY," + "host text,"
				+ "port int);");
		
		
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized Collection<InetSocketAddress> getRegisteredAddresses() {
		if (cluster == null) {
			cluster = Cluster.builder().addContactPoints(contactPoints).withPort(port).build();

			log.info("Connected to cluster: " + cluster.getMetadata().getClusterName());

			session = cluster.connect();
		}
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + serviceName + " WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor': 3};");
		session.execute("CREATE TABLE IF NOT EXISTS " + serviceName + ".igep (" + "id uuid PRIMARY KEY," + "host text,"
				+ "port int);");
		// resolve configured addresses
		final Collection<InetSocketAddress> inets = new CopyOnWriteArrayList<>();
		//final String fqdn = "_" + containerPortName + "._tcp." + serviceName;
		//log.debug("Looking up SRV records with FQDN [" + fqdn + "].");

		/*
		 * final List<LookupResult> nodes = resolver.resolve(fqdn); for (LookupResult
		 * node : nodes) { inets.add(InetSocketAddress.createUnresolved(node.host(),
		 * node.port())); } log.debug("Found " + nodes.size() + " nodes.");
		 */
		return inets;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return S.toString(CassandraIpFinder.class, this, "super", super.toString());
	}

}