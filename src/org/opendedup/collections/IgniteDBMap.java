/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.collections;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.cache.Cache.Entry;
import javax.cache.configuration.FactoryBuilder;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.ignite.RMDBPersistence;
import org.opendedup.ignite.RocksDBPersistence;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.StringUtils;

public class IgniteDBMap implements AbstractMap, AbstractHashesMap  {
	IgniteCache<ByteArrayWrapper, ByteArrayWrapper> db = null;
	IgniteCache<ByteArrayWrapper, ByteArrayWrapper> rmdb = null;
	Ignite ig = null;

	boolean closed = false;
	private long size = 0;
	IgniteTransactions transactions = null;
	IgniteTransactions rtransactions = null;
	private static final long rmthreashold = 5 * 60 * 1000;
	Lock gclock = null;
	private final static ByteArrayWrapper lobj =new ByteArrayWrapper("gclock".getBytes());

	@Override
	public void init(long maxSize, String fileName, double fpp) throws IOException, HashtableFullException {

		try {
			this.size = maxSize;
			CacheConfiguration<ByteArrayWrapper, ByteArrayWrapper> cacheCfg = new CacheConfiguration<ByteArrayWrapper, ByteArrayWrapper>();
			cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(RocksDBPersistence.class));
			cacheCfg.setReadThrough(true);
			cacheCfg.setWriteThrough(true);
			cacheCfg.setName(Main.volume.getUuid());
			cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
			IgniteConfiguration cfg = new IgniteConfiguration();
			MemoryConfiguration memCfg = new MemoryConfiguration();
			MemoryPolicyConfiguration memPlc = new MemoryPolicyConfiguration();

			memPlc.setName("Standard Eviction");
			memPlc.setMaxSize(maxSize);

			// Enabling RANDOM_LRU eviction.
			memPlc.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
			memCfg.setMemoryPolicies(memPlc);
			cfg.setMemoryConfiguration(memCfg);
			cfg.setCacheConfiguration(cacheCfg);
			// Optional transaction configuration. Configure TM lookup here.
			TransactionConfiguration txCfg = new TransactionConfiguration();
			cfg.setTransactionConfiguration(txCfg);
			cacheCfg.setCacheMode(CacheMode.PARTITIONED);
			cacheCfg.setBackups(Main.volume.getClusterCopies());
			// Start Ignite node.
			ig = Ignition.start(cfg);
			db = ig.getOrCreateCache(cacheCfg);
			transactions = ig.transactions();

			CacheConfiguration<ByteArrayWrapper, ByteArrayWrapper> rcacheCfg = new CacheConfiguration<ByteArrayWrapper, ByteArrayWrapper>();
			rcacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(RMDBPersistence.class));
			rcacheCfg.setReadThrough(true);
			rcacheCfg.setWriteThrough(true);
			rcacheCfg.setName(Main.volume.getUuid() + "rmdb");
			rcacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
			rmdb = ig.getOrCreateCache(rcacheCfg);
			gclock =rmdb.lock(lobj);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean claimKey(byte[] hash, long val, long ct) throws IOException {
		ByteArrayWrapper hv = new ByteArrayWrapper(hash);

		try (Transaction tx = transactions.txStart(TransactionConcurrency.OPTIMISTIC,
				TransactionIsolation.SERIALIZABLE)) {
			ByteArrayWrapper v = db.get(hv);
			if (v != null) {
				ByteBuffer bk = ByteBuffer.wrap(v.getData());
				long oval = bk.getLong();
				if (oval != val) {
					SDFSLogger.getLog().warn("When updating reference count for key [" + StringUtils.getHexString(hash)
							+ "] hash locations didn't match stored val=" + oval + " request value=" + val);
					tx.close();
					return false;
				}
				ct += bk.getLong();
				if (ct <= 0) {
					bk.position(8);
					bk.putLong(System.currentTimeMillis());
					rmdb.put(hv, new ByteArrayWrapper(bk.array()));
					SDFSLogger.getLog().info("removed");
				} else if (rmdb.get(hv) != null) {
					rmdb.remove(hv);
				}
				bk.putLong(v.getData().length - 8, ct);
				db.put(hv, v);
				tx.commit();
				return true;
			}
			SDFSLogger.getLog()
					.warn("When updating reference count. Key [" + StringUtils.getHexString(hash) + "] not found");
			return false;
		} catch (TransactionOptimisticException e) {
			SDFSLogger.getLog().warn("Transaction Failed.", e);
			return this.claimKey(hash, val, ct);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {

		}
	}

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	@Override
	public long getSize() {
		long sz = 0;
		return sz;

	}

	@Override
	public long getUsedSize() {
		return 0;
	}

	@Override
	public long getMaxSize() {
		return this.size;
	}

	@Override
	public synchronized long claimRecords(SDFSEvent evt) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable is close");
		long rmk = 0;
		if(gclock.tryLock()) {
		try {
			Iterator<Entry<ByteArrayWrapper, ByteArrayWrapper>> iter = rmdb.iterator();
			SDFSLogger.getLog().info("Removing hashes");
			ByteBuffer bk = ByteBuffer.allocateDirect(16);
			while (iter.hasNext()) {
				Entry<ByteArrayWrapper, ByteArrayWrapper> ent = iter.next();
				ByteArrayWrapper w = ent.getKey();
				ByteArrayWrapper v = ent.getValue();

				bk.position(0);
				bk.put(v.getData());
				bk.position(0);
				long pos = bk.getLong();
				long tm = bk.getLong() + rmthreashold;
				if (tm > System.currentTimeMillis()) {
					try (Transaction tx = transactions.txStart(TransactionConcurrency.OPTIMISTIC,
							TransactionIsolation.SERIALIZABLE)) {
						ByteArrayWrapper pv = this.db.get(w);
						if (pv != null) {
							ByteBuffer nbk = ByteBuffer.wrap(pv.getData());
							long oval = nbk.getLong();
							long ct = nbk.getLong();
							if (ct <= 0 && oval == pos) {
								ChunkData ck = new ChunkData(pos, w.getData());
								ck.setmDelete(true);
								this.db.remove(w);
							}
							this.rmdb.remove(w);

						} else {
							rmdb.remove(w);
							ChunkData ck = new ChunkData(pos, w.getData());
							ck.setmDelete(true);
						}
					} catch (TransactionOptimisticException e) {
						SDFSLogger.getLog().warn("Transaction Failed.", e);
					}
				}
			}

		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to finish Garbage Collection", e);
		} finally {
			gclock.unlock();
		}
		} else {
			SDFSLogger.getLog().info("Garbage Collection already running on another node");
		}
		return rmk;
	}

	@Override
	public synchronized void clearRefMap() throws IOException {
		throw new IOException("not supported");
	}

	AtomicLong csz = new AtomicLong(0);

	@Override
	public synchronized long claimRecords(SDFSEvent evt, LargeBloomFilter bf) throws IOException {
		throw new IOException("not supported");
	}

	public void setMaxSize(long maxSz) throws IOException {

	}

	/**
	 * initializes the Object set of this hash table.
	 * 
	 * @param initialCapacity
	 *            an <code>int</code> value
	 * @return an <code>int</code> value
	 * @throws HashtableFullException
	 * @throws FileNotFoundException
	 */
	public long setUp() throws Exception {
		return 0;
	}

	@Override
	public long endStartingPosition() {
		return -1;
	}

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 * @throws IOException
	 */
	@Override
	public boolean containsKey(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable is close");
		}
		try {
			ByteArrayWrapper v = this.db.get(new ByteArrayWrapper(key));
			if (v == null)
				return false;
			else
				return true;
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public InsertRecord put(ChunkData cm) throws IOException, HashtableFullException {
		if (this.isClosed())
			throw new HashtableFullException("Hashtable is close");
		// this.flushFullBuffer();
		return this.put(cm, true);
	}

	@Override
	public InsertRecord put(ChunkData cm, boolean persist) throws IOException, HashtableFullException {
		// persist = false;
		if (this.isClosed())
			throw new HashtableFullException("Hashtable is close");
		// if (persist)
		// this.flushFullBuffer();
		try (Transaction tx = transactions.txStart(TransactionConcurrency.OPTIMISTIC,
				TransactionIsolation.SERIALIZABLE)) {

			byte[] v = null;
			ByteArrayWrapper hv = new ByteArrayWrapper(cm.getHash());
			ByteArrayWrapper bv = db.get(hv);
			if (bv == null) {
				try {
					cm.persistData(true);
				} catch (org.opendedup.collections.HashExistsException e) {
					cm.setcPos(e.getPos());
				}
				v = new byte[16];
				ByteBuffer bf = ByteBuffer.wrap(v);
				bf.putLong(cm.getcPos());
				if (cm.references <= 0)
					bf.putLong(1);
				else
					bf.putLong(cm.references);
				db.put(hv, new ByteArrayWrapper(v));
				tx.commit();
				return new InsertRecord(true, cm.getcPos());
			} else {
				// SDFSLogger.getLog().info("Hash Found");
				v = bv.getData();
				ByteBuffer bk = ByteBuffer.wrap(v);
				long pos = bk.getLong();
				long ct = bk.getLong();
				if (cm.references <= 0)
					ct++;
				else
					ct += cm.references;
				bk.putLong(8, ct);
				db.put(hv, bv);
				tx.commit();
				return new InsertRecord(false, pos);
			}
		} catch (TransactionOptimisticException e) {
			SDFSLogger.getLog().warn("Transaction Failed.", e);
			return this.put(cm, persist);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#update(org.opendedup.sdfs
	 * .filestore.ChunkData)
	 */
	@Override
	public boolean update(ChunkData cm) throws IOException {
		try (Transaction tx = transactions.txStart(TransactionConcurrency.OPTIMISTIC,
				TransactionIsolation.SERIALIZABLE)) {

			byte[] v = null;
			ByteArrayWrapper hv = new ByteArrayWrapper(cm.getHash());
			ByteArrayWrapper bv = db.get(hv);
			if (bv == null) {
				tx.commit();
				return false;
			} else {
				v = bv.getData();
				ByteBuffer bk = ByteBuffer.wrap(v);
				bk.putLong(0, cm.getcPos());
				db.put(hv, bv);
				tx.commit();
				return true;
			}
		} catch (TransactionOptimisticException e) {
			SDFSLogger.getLog().warn("Transaction Failed.", e);
			return this.update(cm);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public boolean isClaimed(ChunkData cm) throws KeyNotFoundException, IOException {
		throw new IOException("not implemented");
	}

	@Override
	public long get(byte[] key) throws IOException {
		ByteArrayWrapper hv = new ByteArrayWrapper(key);
		ByteArrayWrapper bv = db.get(hv);
		try {
			if (bv == null) {

				return -1;
			} else {
				ByteBuffer bk = ByteBuffer.wrap(bv.getData());
				return bk.getLong();
			}
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public byte[] getData(byte[] key) throws IOException, DataArchivedException {
		if (this.isClosed())
			throw new IOException("Hashtable is close");
		long ps = this.get(key);
		if (ps != -1) {
			return ChunkData.getChunk(key, ps);
		} else {
			SDFSLogger.getLog().error("found no data for key [" + StringUtils.getHexString(key) + "]");
			return null;
		}

	}

	@Override
	public byte[] getData(byte[] key, long pos) throws IOException, DataArchivedException {
		if (this.isClosed())
			throw new IOException("Hashtable is close");
		boolean direct = false;
		long zp = pos;
		if (pos == -1) {
			pos = this.get(key);
		} else {
			direct = true;
		}
		if (pos != -1) {
			byte[] data = null;
			try {
				data = ChunkData.getChunk(key, pos);
			} catch (DataArchivedException e) {
				throw e;
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to get key [" + StringUtils.getHexString(key) + "] [" + pos + "]", e);
			}
			if (direct && (data == null || data.length == 0)) {
				SDFSLogger.getLog().warn(" miss for [" + StringUtils.getHexString(key) + "] [" + pos + "] ");
				return null;
			} else {
				return data;
			}
		} else {
			SDFSLogger.getLog()
					.error("found no data for key [" + StringUtils.getHexString(key) + "] [" + pos + "] [" + zp + "]");
			return null;
		}

	}

	@Override
	public boolean remove(ChunkData cm) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable is close");
		}

		try {
			db.remove(new ByteArrayWrapper(cm.getHash()));
		} catch (Exception e) {
			throw new IOException(e);
		}

		return true;
	}

	private ReentrantLock syncLock = new ReentrantLock();

	@Override
	public void sync() throws IOException {

	}

	@Override
	public void close() {
		try {
			this.closed = true;
			this.db.close();
		} finally {
			this.syncLock.unlock();
			SDFSLogger.getLog().info("Hashtable closed");
		}
	}

	@Override
	public void vanish() throws IOException {
		throw new IOException("not supported");

	}

	@Override
	public void initCompact() throws IOException {

	}

	@Override
	public void commitCompact(boolean force) throws IOException {

	}

	@Override
	public void rollbackCompact() throws IOException {

	}

	public final static class ProcessPriorityThreadFactory implements ThreadFactory {

		private final int threadPriority;

		public ProcessPriorityThreadFactory(int threadPriority) {
			this.threadPriority = threadPriority;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r);
			thread.setPriority(threadPriority);
			return thread;
		}

	}

	@Override
	public void cache(long pos) throws IOException, DataArchivedException {

		ChunkData.cacheChunk(pos);

	}

	@Override
	public boolean mightContainKey(byte[] key) {
		return this.db.containsKey(new ByteArrayWrapper(key));
	}
	
	

}