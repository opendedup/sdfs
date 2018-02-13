package org.opendedup.cassandra;

import java.net.InetSocketAddress;

import java.nio.ByteBuffer;
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
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.StorageUnit;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
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
	InetSocketAddress[] contactPoints = { new InetSocketAddress("localhost", 9042) };
	private Cluster cluster = null;
	private Session session = null;
	private String dataCenter = "datacenter1";
	private int replicationFactor = 3;
	PreparedStatement addHashStatement = null;
	PreparedStatement getHashStatement = null;
	PreparedStatement addRmRefStatement = null;
	PreparedStatement getRmRefStatement = null;
	PreparedStatement getAllRmRefStatement = null;
	PreparedStatement getRefCtStatement = null;
	PreparedStatement getRefHashesStatement = null;
	PreparedStatement deleteHashStatement = null;
	private String serviceName = "opendedupe";
	private long cacheSize = 2L * 1024L * 1024L *1024L;

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
			PoolingOptions poolingOptions = new PoolingOptions();
			poolingOptions.setMaxQueueSize(2048);
			cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).withPoolingOptions(poolingOptions).build();
			SDFSLogger.getLog().info("Connected Cassandra to cluster: " + cluster.getMetadata().getClusterName());
			session = cluster.connect();
		}
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + serviceName + " WITH replication "
				+ "= {'class':'NetworkTopologyStrategy', '" + this.dataCenter + "': " + this.replicationFactor + "};");
		long rowCache = this.cacheSize/(long)(HashFunctionPool.hashLength + 8);
		SDFSLogger.getLog().info("Cassandra Row Cache is " + rowCache + " Size in memory= " + StorageUnit.of(this.cacheSize));
		session.execute(
				"CREATE TABLE IF NOT EXISTS " + serviceName + ".hashdb (key blob PRIMARY KEY, archive bigint) WITH  caching ="
						+ "{'keys' : 'NONE' , 'rows_per_partition' : '"+rowCache+"' } and compression = { 'enabled' : false };");
		session.execute("CREATE TABLE IF NOT EXISTS " + serviceName
				+ ".refhashesdb (archive bigint PRIMARY KEY, hashes list<text>,claims set<bigint>) WITH compression = { 'enabled' : false };");
		session.execute(
				"CREATE TABLE IF NOT EXISTS " + serviceName + ".rmref (archive bigint PRIMARY KEY, timestamp bigint) WITH compression = { 'enabled' : false };");
		addHashStatement = session.prepare((RegularStatement) new SimpleStatement("INSERT INTO " + serviceName + ".hashdb"
				+ " (key, archive) VALUES (?,?)").setConsistencyLevel(ConsistencyLevel.ONE));
		getHashStatement = session.prepare((RegularStatement) new SimpleStatement("Select archive from " + serviceName + 
				".hashdb WHERE key = ?").setConsistencyLevel(ConsistencyLevel.ONE));
		addRmRefStatement = session.prepare((RegularStatement) new SimpleStatement("INSERT INTO " + serviceName + ".rmref"
				+ " (archive, timestamp) VALUES (?,?) IF NOT EXISTS").setConsistencyLevel(ConsistencyLevel.ALL));
		getRmRefStatement = session.prepare("Select timestamp from " + serviceName + ".rmref WHERE archive =?").setConsistencyLevel(ConsistencyLevel.ALL);
		getAllRmRefStatement = session.prepare("Select archive,timestamp from " + serviceName + ".rmref");
		getRefCtStatement = session.prepare("Select claims from " + serviceName + ".refhashesdb where archive=?");
		getRefHashesStatement = session.prepare("Select hashes from " + serviceName + ".refhashesdb WHERE archive = ?");
		deleteHashStatement = session.prepare("DELETE from " + serviceName + ".hashdb WHERE key = ?");
	}

	public long getHash(byte[] key) {
		long id = -1;
		BoundStatement bound = this.getHashStatement.bind(ByteBuffer.wrap(key));
		ResultSet rs = session.execute(bound);
		Row r = rs.one();
		if (r != null && !idb.containsKey(r.getLong(0)))
			id = r.getLong(0);
		return id;
	}

	public long getRMRef(long archive) {
		BoundStatement bound = this.getHashStatement.bind(archive);
		ResultSet rs = session.execute(bound);
		long id = -1;
		Row r = rs.one();
		if (r != null)
			id = r.getLong(0);
		return id;
	}

	public void _setRMRef(long id, long ts) {
		BoundStatement bound = this.addRmRefStatement.bind(id,ts);
		session
		.execute(bound);
	}

	public void delRMRef(long id) {
		this.idb.remove(id);
	}

	public void _delRMRef(long id) {
		session.execute("DELETE from " + serviceName + ".rmref WHERE archive = " + id + ";");
	}

	public void setHash(byte[] key, long id) {
		BoundStatement bound = this.addHashStatement.bind(ByteBuffer.wrap(key),id);
		session.execute(bound);
	}
	
	public void setHashes(String [] keys,long id) {
		ArrayList<ResultSetFuture> alr = new ArrayList<ResultSetFuture>();
		for (String k : keys) {
				byte[] key = BaseEncoding.base64Url().decode(k);
				BoundStatement bound = this.addHashStatement.bind(ByteBuffer.wrap(key),id);
				alr.add(session.executeAsync(bound));
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
	
	public int getRefCt(long archive) {
		BoundStatement bound = this.addHashStatement.bind(archive);
		ResultSet rs = session.execute(bound);
		int ct = -1;
		Row r = rs.one();
		if (r != null)
			ct = r.getSet(0, Long.class).size();
		return ct;
	}

	protected Iterator<Row> _getAllRmrf() {
		BoundStatement bound = this.getAllRmRefStatement.bind();
		ResultSet rs = session.execute(bound);
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
		BoundStatement bound = this.getRefHashesStatement.bind(id);
		ResultSet rs = session.execute(bound);
		Row r = rs.one();
		if (r != null) {
			List<String> keys = r.getList(0, String.class);
			ArrayList<ResultSetFuture> alr = new ArrayList<ResultSetFuture>();
			for (String k : keys) {
				byte[] key = BaseEncoding.base64Url().decode(k);
				BoundStatement _bound = this.getHashStatement.bind(ByteBuffer.wrap(key));
				ResultSet _rs = session.execute(_bound);
				Row _r = _rs.one();
				if (_r == null || _r.getLong(0) == id) {
					BoundStatement _b2 = this.deleteHashStatement.bind(ByteBuffer.wrap(key));
					alr.add(session.executeAsync(_b2));
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
		InetSocketAddress[] cipep = { new InetSocketAddress("192.168.0.215", 9042) };
		CassandraDedupeDB db = new CassandraDedupeDB(cipep, "datacenter1", "sam3", 1,false);

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