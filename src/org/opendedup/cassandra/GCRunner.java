package org.opendedup.cassandra;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.opendedup.collections.ByteArrayWrapper;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.ChunkData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;


public class GCRunner implements IgniteCallable<Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@IgniteInstanceResource
	private Ignite ignite;

	@Override
	public Long call() {
		// Get a reference to node local.
		ConcurrentMap<String, Object> nodeLocalMap = ignite.cluster().nodeLocalMap();

		RocksDB rmdb = (RocksDB) nodeLocalMap.get("rmdbraw");
		@SuppressWarnings("unchecked")
		IgniteCache<ByteArrayWrapper, ByteArrayWrapper> db = (IgniteCache<ByteArrayWrapper, ByteArrayWrapper>) nodeLocalMap.get("db");
		@SuppressWarnings("unchecked")
		IgniteCache<ByteArrayWrapper, ByteArrayWrapper> rdb = (IgniteCache<ByteArrayWrapper, ByteArrayWrapper>) nodeLocalMap.get("rmdb");
		IgniteTransactions transactions =(IgniteTransactions) nodeLocalMap.get("transaction");
		if(rmdb==null) {
			rmdb = RMDBPersistence.rmdb;
			nodeLocalMap.put("rmdbraw", RMDBPersistence.rmdb);
		}

		long rmk = 0;
		try {
			RocksIterator iter = rmdb.newIterator();
			SDFSLogger.getLog().info("Removing hashes");
			ByteBuffer bk = ByteBuffer.allocateDirect(16);
			for (iter.seekToFirst(); iter.isValid(); iter.next()) {
				byte [] key = iter.key();
				byte [] value = iter.value();
				

				bk.position(0);
				bk.put(value);
				bk.position(0);
				long pos = bk.getLong();
				long tm = bk.getLong() + rmthreashold;
				
				if (tm < System.currentTimeMillis()) {
					try (Transaction tx = transactions.txStart(TransactionConcurrency.OPTIMISTIC,
							TransactionIsolation.SERIALIZABLE)) {
						ByteArrayWrapper w = new ByteArrayWrapper(key);
						ByteArrayWrapper pv = db.get(w);
						if (pv != null) {
							ByteBuffer nbk = ByteBuffer.wrap(pv.getData());
							long oval = nbk.getLong();
							long ct = nbk.getLong();
							if (ct <= 0 && oval == pos) {
								ChunkData ck = new ChunkData(pos, w.getData());
								ck.setmDelete(true);
								db.remove(w);
							}
							rmdb.delete(key);
							rdb.remove(w);
						} else {
							rmdb.delete(key);
							rdb.remove(w);
							ChunkData ck = new ChunkData(pos, w.getData());
							ck.setmDelete(true);
						}
						rmk++;
					} catch (TransactionOptimisticException e) {
						SDFSLogger.getLog().warn("Transaction Failed.", e);
					}
				} 
			}

		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to finish Garbage Collection", e);
		}
		return rmk;
	}

}
