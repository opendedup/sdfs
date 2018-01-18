package org.opendedup.cassandra;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.cache.Cache.Entry;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.opendedup.logging.SDFSLogger;

import com.datastax.driver.core.Row;

public class IgniteLongLongDBPersistence implements org.apache.ignite.cache.store.CacheStore<Long, Long> {

	CassandraDedupeDB db = null;

	public IgniteLongLongDBPersistence() {
		super();

	}

	@Override
	public void loadCache(IgniteBiInClosure<Long, Long> clo, Object... args) {
		this.db = (CassandraDedupeDB) args[0];
		if (args.length > 1 ) {
			Iterator<Row> iter = db._getAllRmrf();
			AtomicLong k = (AtomicLong) args[1];
			while (iter.hasNext()) {
				Row r = iter.next();
				clo.apply(r.getLong(0), r.getLong(1));
				k.incrementAndGet();
			}
		}
	}

	@Override
	public Long load(Long key) throws CacheLoaderException {
		try {
			return db.getRMRef(key);
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to load hash", e);
			throw new CacheLoaderException(e);
		}

	}

	@Override
	public void delete(Object key) throws CacheWriterException {
		try {
			Long bw = (Long) key;
			this.db._delRMRef(bw);
		} catch (Exception e) {
			throw new CacheWriterException(e);
		}

	}

	@Override
	public void write(Entry<? extends Long, ? extends Long> entry) throws CacheWriterException {
		try {
			this.db._setRMRef(entry.getKey(), entry.getValue());
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to persist hash", e);
			throw new CacheWriterException(e);
		}
	}

	@Override
	public Map<Long, Long> loadAll(Iterable<? extends Long> arg0) throws CacheLoaderException {
		return null;
	}

	@Override
	public void deleteAll(Collection<?> arg0) throws CacheWriterException {

	}

	@Override
	public void writeAll(Collection<Entry<? extends Long, ? extends Long>> arg0) throws CacheWriterException {
		for (Entry<? extends Long, ? extends Long> e : arg0) {
			this.write(e);
		}

	}

	@Override
	public void sessionEnd(boolean arg0) throws CacheWriterException {

	}

	public void setDataSource(CassandraDedupeDB db) {
		this.db = db;
	}

	public static void main(String[] args) throws InterruptedException {
		CassandraIpFinder cip = new CassandraIpFinder();
		TcpDiscoverySpi spi = new TcpDiscoverySpi();
		spi.setIpFinder(cip);
		InetSocketAddress[] cipep = { new InetSocketAddress("192.168.0.105", 9042) };
		CassandraDedupeDB cdb = new CassandraDedupeDB(cipep, "datacenter1", "sdfscluster", 3,false);

		cip.setCassandraContactPoints(cipep);
		IgniteConfiguration cfg = new IgniteConfiguration();

		// Override default discovery SPI.
		cfg.setDiscoverySpi(spi);

		// Start Ignite node.

		CacheConfiguration<Long, Long> cacheCfg = new CacheConfiguration<Long, Long>();
		cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(IgniteLongLongDBPersistence.class));
		cacheCfg.setReadThrough(true);
		cacheCfg.setWriteThrough(true);
		cacheCfg.setName("sdfscluster");
		cacheCfg.setReadThrough(true);
		cacheCfg.setCacheMode(CacheMode.REPLICATED);
		cfg.setCacheConfiguration(cacheCfg);
		Ignite ig = Ignition.start(cfg);
		IgniteCache<Long, Long> db = ig.getOrCreateCache(cacheCfg);
		// Thread.sleep(5000);
		while (ig.cluster().forOldest().node() == null) {
			Thread.sleep(3000);
			System.out.println("waiting for system to come up");
		}
		if (ig.cluster().forOldest().node().equals(ig.cluster().localNode())) {
			Object[] obj = new Object[2];
			obj[0] = cdb;
			obj[1] = new AtomicLong();
			db.loadCache(null, obj);
			System.out.println(" Loaded " + db.sizeLong() + " objects into the rm cache");
		} else {
			Object[] obj = new Object[1];
			obj[0] = cdb;
			db.loadCache(null, obj);
			System.out.println(" Loaded " + db.sizeLong() + " objects into the rm cache");
		}

		System.out.println(db.get(5L));
		db.put(5L, System.currentTimeMillis());
		ig.close();
	}

}
