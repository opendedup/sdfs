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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

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
import org.opendedup.ignite.RocksDBPersistence;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.StringUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;


public class IgniteDBMap implements AbstractMap, AbstractHashesMap {
	IgniteCache<ByteArrayWrapper, ByteArrayWrapper> db = null;
	boolean closed = false;
	private long size = 0;
	IgniteTransactions transactions = null;
	RocksDB rmdb = null;
	private static final long GB = 1024*1024*1024;
	
	
	@Override
	public void init(long maxSize, String fileName, double fpp) throws IOException, HashtableFullException {

		try {
			this.size = maxSize;
			CacheConfiguration<ByteArrayWrapper, ByteArrayWrapper> cacheCfg = new CacheConfiguration<ByteArrayWrapper, ByteArrayWrapper>();
			cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(RocksDBPersistence.class));
			cacheCfg.setReadThrough(true);
			cacheCfg.setWriteThrough(true);
			cacheCfg.setName("sdfs");
			
			cacheCfg.setName("cacheName");
			cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

			IgniteConfiguration cfg = new IgniteConfiguration();
			MemoryConfiguration memCfg = new MemoryConfiguration();
			// Defining a policy for 20 GB memory region with RANDOM_LRU eviction.
			MemoryPolicyConfiguration memPlc = new MemoryPolicyConfiguration();

			memPlc.setName("Standar Eviction");
			memPlc.setMaxSize(20L * 1024 * 1024 * 1024);

			// Enabling RANDOM_LRU eviction.
			memPlc.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
			// Setting the size of the default memory region to 4GB to achieve this.
			memCfg.setMemoryPolicies(memPlc);
			cfg.setMemoryConfiguration(memCfg);
			cfg.setCacheConfiguration(cacheCfg);
			// Optional transaction configuration. Configure TM lookup here.
			TransactionConfiguration txCfg = new TransactionConfiguration();
			cfg.setTransactionConfiguration(txCfg);
			cacheCfg.setCacheMode(CacheMode.PARTITIONED);
			cacheCfg.setBackups(1);
			// Start Ignite node.
			Ignite ig  = Ignition.start(cfg);
			db = ig.getOrCreateCache(cacheCfg);
			transactions = ig.transactions();
			
			Options options = new Options();
			options.setCreateIfMissing(true);
			options.setCompactionStyle(CompactionStyle.LEVEL);
			options.setCompressionType(CompressionType.NO_COMPRESSION);
			BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
			blockConfig.setFilter(new BloomFilter(16, false));
			// blockConfig.setHashIndexAllowCollision(false);
			// blockConfig.setCacheIndexAndFilterBlocks(false);
			// blockConfig.setIndexType(IndexType.kBinarySearch);
			// blockConfig.setPinL0FilterAndIndexBlocksInCache(true);
			blockConfig.setBlockSize(4 * 1024);
			blockConfig.setFormatVersion(2);
			// options.useFixedLengthPrefixExtractor(3);

			Env env = Env.getDefault();
			env.setBackgroundThreads(8, Env.FLUSH_POOL);
			env.setBackgroundThreads(8, Env.COMPACTION_POOL);
			options.setMaxBackgroundCompactions(8);
			options.setMaxBackgroundFlushes(8);
			options.setEnv(env);
		
			// options.setNumLevels(8);
			// options.setLevelCompactionDynamicLevelBytes(true);
			//
			options.setAllowConcurrentMemtableWrite(true);
			//LRUCache c = new LRUCache(memperDB);
			//options.setRowCache(c);
			
			//blockConfig.setBlockCacheSize(memperDB);
			blockConfig.setNoBlockCache(true);
			options.setWriteBufferSize(GB);
			options.setMinWriteBufferNumberToMerge(2);
			options.setMaxWriteBufferNumber(6);
			options.setLevelZeroFileNumCompactionTrigger(2);

			// options.setCompactionReadaheadSize(1024*1024*25);
			// options.setUseDirectIoForFlushAndCompaction(true);
			// options.setUseDirectReads(true);
			options.setStatsDumpPeriodSec(30);
			// options.setAllowMmapWrites(true);
			// options.setAllowMmapReads(true);
			options.setMaxOpenFiles(-1);
			options.createStatistics();
			// options.setTargetFileSizeBase(512*1024*1024);
			options.setMaxBytesForLevelBase(GB);
			options.setTargetFileSizeBase(128 * 1024 * 1024);
			options.setTableFormatConfig(blockConfig);
			File f = new File(fileName + File.separator + "rmdb");
			f.mkdirs();
			rmdb = RocksDB.open(options, f.getPath());
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
				if(ct <=0) {
					
					rmdb.put(hash, v.getData());
					db.remove(new ByteArrayWrapper(hash));
				} else {
					bk.putLong(v.getData().length - 8, ct);
					db.put(hv, v);
				}
				tx.commit();
				return true;
			}
			SDFSLogger.getLog()
					.warn("When updating reference count. Key [" + StringUtils.getHexString(hash) + "] not found");
			return false;
		}catch(TransactionOptimisticException e) {
			SDFSLogger.getLog()
			.warn("Transaction Failed.",e);
			return this.claimKey(hash, val, ct);
		}
		catch (Exception e) {
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
		try {
				RocksIterator iter = rmdb.newIterator();
				SDFSLogger.getLog().info("Removing hashes " + rmdb.getLongProperty("rocksdb.estimate-num-keys"));
				ByteBuffer bk = ByteBuffer.allocateDirect(16);
				for (iter.seekToFirst(); iter.isValid(); iter.next()) {
					bk.position(0);
					bk.put(iter.value());
					bk.position(0);
					long pos = bk.getLong();
					ChunkData ck = new ChunkData(pos, iter.key());
					ck.setmDelete(true);
					rmdb.delete(iter.key());
				}

		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to finish Garbage Collection", e);
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
					}catch (org.opendedup.collections.HashExistsException e) {
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
					//SDFSLogger.getLog().info("Hash Found");
					v = bv.getData();
					ByteBuffer bk = ByteBuffer.wrap(v);
					long pos = bk.getLong();
					long ct = bk.getLong();
					if (cm.references <= 0)
						ct++;
					else
						ct += cm.references;
					bk.putLong(8, ct);
					db.put(hv,bv);
					tx.commit();
					return new InsertRecord(false, pos);
				}
			} catch(TransactionOptimisticException e) {
				SDFSLogger.getLog()
				.warn("Transaction Failed.",e);
				return this.put(cm, persist);
			} catch(IOException e) {
				throw e;
			}catch(Exception e) {
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
					db.put(hv,bv);
					tx.commit();
					return true;
				}
		} catch(TransactionOptimisticException e) {
			SDFSLogger.getLog()
			.warn("Transaction Failed.",e);
			return this.update(cm);
		} catch(Exception e) {
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