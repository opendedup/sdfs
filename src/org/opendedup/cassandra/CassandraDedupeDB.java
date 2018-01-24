package org.opendedup.cassandra;

import java.net.InetSocketAddress;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import javax.cache.configuration.FactoryBuilder;

import org.apache.curator.shaded.com.google.common.base.Joiner;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.io.BaseEncoding;

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
public class CassandraDedupeDB {

	private IgniteCache<Long, Long> idb = null;
	private Ignite ig = null;
	InetSocketAddress[] contactPoints = { new InetSocketAddress("127.0.0.1", 9042) };
	private Cluster cluster = null;
	private Session session = null;
	private String dataCenter = "datacenter1";
	private int replicationFactor = 3;
	// BoundStatement hdbStatement = null;
	// BoundStatement hdbStatementUpdate = null;
	// BoundStatement getHdbStatement = null;
	// BoundStatement insertRefHashes = null;
	// BoundStatement getRefHasheStrings = null;
	// BoundStatement delRefHdb = null;
	// BoundStatement delHdb = null;
	// BoundStatement delRmRef = null;
	// BoundStatement getRmRef = null;
	// BoundStatement insertRmRef = null;
	// BoundStatement getAllRmRef = null;
	// BoundStatement getClaims = null;
	// BoundStatement addClaim = null;
	// BoundStatement rmClaim = null;
	private String serviceName = "opendedupe";

	public CassandraDedupeDB(InetSocketAddress[] contactPoints, String dataCenter, String serviceName,
			int replicationFactor,boolean useIgnite) {
		this.contactPoints = contactPoints;
		this.dataCenter = dataCenter;
		this.serviceName = serviceName;
		this.createTableSpace();
		
		this.replicationFactor = replicationFactor;
		if(useIgnite)
			this.initIgnite(contactPoints);
	}

	public int getRefCt(long archive) {
		ResultSet rs = session.execute("Select claims from " + serviceName + ".refhashesdb where archive=" + archive);
		int ct = -1;
		Row r = rs.one();
		if (r != null)
			ct = r.getSet(0, Long.class).size();
		return ct;
	}

	public void claimArchive(long archive, long serialnumber) {
		session.execute("UPDATE " + serviceName + ".refhashesdb set claims = claims + {" + serialnumber
				+ "} WHERE archive = " + archive);
	}

	public void unClaimArchive(long archive, long serialnumber) {
		session.execute("UPDATE " + serviceName + ".refhashesdb set claims = claims - {" + serialnumber
				+ "} WHERE archive =" + archive);
	}

	private void initIgnite(InetSocketAddress[] contactPoints) {
		CassandraIpFinder cip = new CassandraIpFinder();
		TcpDiscoverySpi spi = new TcpDiscoverySpi();
		spi.setIpFinder(cip);

		cip.setCassandraContactPoints(contactPoints);
		cip.setCassandraDataCenter(Main.volume.getDataCenter());
		cip.setServiceName(Main.volume.getUuid());
		IgniteConfiguration cfg = new IgniteConfiguration();

		// Override default discovery SPI.
		cfg.setDiscoverySpi(spi);

		// Start Ignite node.

		CacheConfiguration<Long, Long> cacheCfg = new CacheConfiguration<Long, Long>();
		cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(IgniteLongLongDBPersistence.class));
		cacheCfg.setReadThrough(true);
		cacheCfg.setWriteThrough(true);
		cacheCfg.setName("rmarchives");
		cacheCfg.setCacheMode(CacheMode.REPLICATED);
		cfg.setCacheConfiguration(cacheCfg);
		ig = Ignition.start(cfg);
		idb = ig.getOrCreateCache(cacheCfg);
		// Thread.sleep(5000);
		while (ig.cluster().forOldest().node() == null) {
			try {
				Thread.sleep(3000);
				System.out.println("waiting for cluster to come up");
			} catch (Exception e) {
			}
		}
		if (ig.cluster().forOldest().node().equals(ig.cluster().localNode())) {
			Object[] obj = new Object[2];
			obj[0] = this;
			obj[1] = new AtomicLong();
			idb.loadCache(null, obj);
			System.out.println(" Loaded " + idb.sizeLong() + " objects into the rm cache");
		} else {
			//Object[] obj = new Object[1];
			//obj[0] = this;
			//obj[1] = new AtomicLong();
			//idb.loadCache(null, obj);
			System.out.println(" Loaded " + idb.sizeLong() + " objects into the rm cache");
		}
	}

	public void addRMRef(long archiveid) {
		this.idb.put(archiveid, System.currentTimeMillis());
	}
	
	public boolean isMaster() {
		return ig.cluster().forOldest().node().equals(ig.cluster().localNode());
	}

	private void createTableSpace() {
		if (cluster == null) {
			cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).build();
			SDFSLogger.getLog().info("Connected Cassandra to cluster: " + cluster.getMetadata().getClusterName());
			session = cluster.connect();
		}
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + serviceName + " WITH replication "
				+ "= {'class':'NetworkTopologyStrategy', '" + this.dataCenter + "': " + this.replicationFactor + "};");
		session.execute(
				"CREATE TABLE IF NOT EXISTS " + serviceName + ".hashdb (key blob PRIMARY KEY, archive bigint);");
		session.execute("CREATE TABLE IF NOT EXISTS " + serviceName
				+ ".refhashesdb (archive bigint PRIMARY KEY, hashes list<text>,claims set<bigint>);");
		session.execute(
				"CREATE TABLE IF NOT EXISTS " + serviceName + ".rmref (archive bigint PRIMARY KEY, timestamp bigint);");
	}

	public long getHash(byte[] key) {
		long id = -1;
		ResultSet rs = session.execute("Select archive from " + serviceName + ".hashdb WHERE key = 0x"
				+ BaseEncoding.base16().encode(key) + ";");
		Row r = rs.one();
		if (r != null && !idb.containsKey(r.getLong(0)))
			id = r.getLong(0);
		return id;
	}

	public long getRMRef(long archive) {
		ResultSet rs = session
				.execute("Select timestamp from " + serviceName + ".rmref WHERE archive =" + archive + ";");
		long id = -1;
		Row r = rs.one();
		if (r != null)
			id = r.getLong(0);
		return id;
	}

	public void _setRMRef(long id, long ts) {
		session.execute("INSERT INTO " + serviceName + ".rmref (archive, timestamp) VALUES (" + id + "," + ts
				+ ") IF NOT EXISTS;");
	}

	public void delRMRef(long id) {
		this.idb.remove(id);
	}

	public void _delRMRef(long id) {
		session.execute("DELETE from " + serviceName + ".rmref WHERE archive = " + id + ";");
	}

	public void setHash(byte[] key, long id) {
		session.execute("INSERT INTO " + serviceName + ".hashdb (key, archive) VALUES (0x"
				+ BaseEncoding.base16().encode(key) + "," + id + ");");
	}
	
	public void setHashes(String [] keys,long id) {
		ArrayList<ResultSetFuture> alr = new ArrayList<ResultSetFuture>();
		for (String k : keys) {
			byte[] key = BaseEncoding.base64Url().decode(k);
			
				alr.add(session.executeAsync("INSERT INTO " + serviceName + ".hashdb (key, archive) VALUES (0x"
						+ BaseEncoding.base16().encode(key) + "," + id + ");"));
		}
		int ds = 0;
		while (ds < alr.size()) {
			ds = 0;
			for (ResultSetFuture _r : alr) {
				if (_r.isDone())
					ds++;
			}
			if(ds != alr.size()) {
				try {
					Thread.sleep(1L);
				} catch (InterruptedException e) {
					
				}
			}
		}
	}

	protected Iterator<Row> _getAllRmrf() {
		ResultSet rs = session.execute("Select * from " + serviceName + ".rmref;");
		return rs.iterator();
	}

	public Iterator<javax.cache.Cache.Entry<Long, Long>> getAllRmrg() {
		return this.idb.iterator();
	}

	public void insertHashes(long id, String[] hashes, long volid) {
		String keys = Joiner.on("','").join(hashes);
		keys = "'" + keys + "'";
		session.execute("INSERT INTO " + serviceName + ".refhashesdb (archive, hashes,claims) VALUES (" + id + ",["
				+ keys + "],{" + volid + "});");
	}

	public void deleteRef(long id) {
		ResultSet rs = session.execute("Select hashes from " + serviceName + ".refhashesdb WHERE archive = " + id);
		Row r = rs.one();
		if (r != null) {
			List<String> keys = r.getList(0, String.class);
			ArrayList<ResultSetFuture> alr = new ArrayList<ResultSetFuture>();
			for (String k : keys) {
				byte[] key = BaseEncoding.base64Url().decode(k);
				ResultSet _rs = session.execute("Select archive from " + serviceName + ".hashdb WHERE key = 0x"
						+ BaseEncoding.base16().encode(key) + ";");
				Row _r = _rs.one();
				if (_r == null || _r.getLong(0) == id) {
					alr.add(session.executeAsync("DELETE from " + serviceName + ".hashdb WHERE key = 0x"
							+ BaseEncoding.base16().encode(key) + ";"));
				}
			}
			int ds = 0;
			while (ds < alr.size()) {
				ds = 0;
				for (ResultSetFuture _r : alr) {
					if (_r.isDone())
						ds++;
				}
				if(ds != alr.size()) {
					try {
						Thread.sleep(10L);
					} catch (InterruptedException e) {
						
					}
				}
			}
		}
		session.execute("DELETE from " + serviceName + ".refhashesdb WHERE archive = " + id);
		this.idb.remove(id);
	}

	public void close() {
		this.session.close();
		this.cluster.close();
		this.idb.close();
		this.ig.close();
	}

	public static void main(String[] args) {
		byte ib = 3;
		System.out.println(new Byte(ib).intValue());
		InetSocketAddress[] cipep = { new InetSocketAddress("192.168.0.105", 9042) };
		CassandraDedupeDB db = new CassandraDedupeDB(cipep, "datacenter1", "sam2", 1,false);

		Random rnd = new Random();

		long id = 78;
		ArrayList<String> al = new ArrayList<String>();
		
		for (int i = 0; i < 1000; i++) {
			byte[] key = new byte[16];
			rnd.nextBytes(key);
			al.add(BaseEncoding.base64Url().encode(key));
		}
		
		String[] keys = new String[al.size()];
		al.toArray(keys);
		long tm = System.currentTimeMillis();
		db.setHashes(keys, id);
		System.out.println("Insert Time = " + (System.currentTimeMillis() - tm));
		
		//System.out.println(keys);
		tm = System.currentTimeMillis();
		db.insertHashes(id, keys, 6442L);
		System.out.println("Ref Insert Time = " + (System.currentTimeMillis() - tm));
		db.close();
	}
}