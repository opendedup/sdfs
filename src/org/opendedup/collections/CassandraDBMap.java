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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.cache.Cache.Entry;

import org.apache.commons.io.FileUtils;
import org.opendedup.cassandra.CassandraDedupeDB;
import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.io.events.ArchiveSync;
import org.opendedup.sdfs.io.events.HashBlobArchiveUploaded;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.CommandLineProgressBar;
import org.opendedup.util.LongConverter;
import org.opendedup.util.StringUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.BaseEncoding;

public class CassandraDBMap implements AbstractMap, AbstractHashesMap {
	WriteOptions wo = new WriteOptions();
	// RocksDB db = null;
	String fileName = null;
	ReentrantLock[] lockMap = new ReentrantLock[256];
	ReentrantLock[] arLockMap = new ReentrantLock[256];
	ReentrantReadWriteLock gclock = new ReentrantReadWriteLock();
	RocksDB[] dbs = new RocksDB[8];
	RocksDB rmdb = null;
	private final ArrayList<ColumnFamilyHandle> cf = new ArrayList<ColumnFamilyHandle>();
	private static final long GB = 1024 * 1024 * 1024;
	private static final long MB = 1024 * 1024;
	int multiplier = 0;
	boolean closed = false;
	private long size = 0;
	private static final long rmthreashold = 1 * 60 * 1000;
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(2);
	private transient ThreadPoolExecutor executor = null;
	final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
	static long bufferSize = GB;
	static long fsize = 128 * MB;
	CassandraDedupeDB cdb = null;
	private ConcurrentHashMap<ByteArrayWrapper, ByteBuffer> tempHt = new ConcurrentHashMap<ByteArrayWrapper, ByteBuffer>();

	static {
		RocksDB.loadLibrary();
	}

	@Override
	public void init(long maxSize, String fileName, double fpp) throws IOException, HashtableFullException {
		try {
			this.fileName = fileName;

			this.size = maxSize;
			multiplier = 256 / dbs.length;
			System.out.println("multiplier=" + this.multiplier + " size=" + dbs.length);
			bufferSize = GB;
			if (this.size < 10_000_000_000L) {
				SDFSLogger.getLog().info("Setting up Small Hash Table");
				fsize = 128 * MB;
				bufferSize = fsize * dbs.length;
			} else if (this.size < 50_000_000_000L) {
				SDFSLogger.getLog().info("Setting up Medium Hash Table");
				fsize = 256 * MB;
				bufferSize = GB * dbs.length;
			} else {
				SDFSLogger.getLog().info("Setting up Large Hash Table");
				// long mp = this.size / 10_000_000_000L;

				fsize = GB;
				bufferSize = 1 * GB * dbs.length;
			}
			// blockConfig.setChecksumType(ChecksumType.kNoChecksum);
			long totmem = maxSize;

			long memperDB = totmem / dbs.length;
			System.out.println("mem=" + totmem + " memperDB=" + memperDB + " bufferSize=" + bufferSize
					+ " bufferSizePerDB=" + (bufferSize / dbs.length));
			SDFSLogger.getLog().info("mem=" + totmem + " memperDB=" + memperDB + " bufferSize=" + bufferSize
					+ " bufferSizePerDB=" + (bufferSize / dbs.length));
			// blockConfig.setBlockCacheSize(memperDB);
			// blockConfig.setCacheIndexAndFilterBlocks(true);
			// a factory method that returns a RocksDB instance
			wo = new WriteOptions();
			wo.setDisableWAL(false);
			wo.setSync(false);
			// LRUCache c = new LRUCache(totmem);
			CommandLineProgressBar bar = new CommandLineProgressBar("Loading Existing Hash Tables", dbs.length,
					System.out);
			executor = new ThreadPoolExecutor(Main.writeThreads + 1, Main.writeThreads + 1, 10, TimeUnit.SECONDS,
					worksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
			AtomicInteger ct = new AtomicInteger();
			ArrayList<StartShard> shs = new ArrayList<StartShard>();
			for (int i = 0; i < dbs.length; i++) {
				BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
				// ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
				// DBOptions dbo = new DBOptions();
				// cfOptions.optimizeLevelStyleCompaction();
				// cfOptions.optimizeForPointLookup(8192);
				blockConfig.setFilter(new BloomFilter(16, false));
				// blockConfig.setHashIndexAllowCollision(false);
				// blockConfig.setCacheIndexAndFilterBlocks(false);
				// blockConfig.setIndexType(IndexType.kBinarySearch);
				// blockConfig.setPinL0FilterAndIndexBlocksInCache(true);
				blockConfig.setBlockSize(4 * 1024);
				blockConfig.setFormatVersion(2);
				blockConfig.setNoBlockCache(true);
				blockConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
				// options.useFixedLengthPrefixExtractor(3);

				Env env = Env.getDefault();
				env.setBackgroundThreads(8, Env.FLUSH_POOL);
				env.setBackgroundThreads(8, Env.COMPACTION_POOL);
				Options options = new Options();
				options.setCreateIfMissing(true);
				options.setCompactionStyle(CompactionStyle.LEVEL);
				options.setCompressionType(CompressionType.NO_COMPRESSION);
				options.setLevel0FileNumCompactionTrigger(8);
				options.setMaxBackgroundCompactions(2);
				options.setMaxBackgroundFlushes(8);
				options.setEnv(env);
				// options.setAccessHintOnCompactionStart(AccessHint.WILLNEED);
				options.setIncreaseParallelism(32);
				options.setAdviseRandomOnOpen(true);
				// options.setNumLevels(8);
				// options.setLevelCompactionDynamicLevelBytes(true);
				//
				options.setAllowConcurrentMemtableWrite(true);
				LRUCache c = new LRUCache(memperDB);
				options.setRowCache(c);
				// blockConfig.setBlockCacheSize(GB * 2);

				options.setWriteBufferSize(bufferSize / dbs.length);
				// options.setMinWriteBufferNumberToMerge(2);
				// options.setMaxWriteBufferNumber(6);
				// options.setLevelZeroFileNumCompactionTrigger(2);

				options.setCompactionReadaheadSize(1024 * 1024 * 25);
				// options.setUseDirectIoForFlushAndCompaction(true);
				// options.setUseDirectReads(true);
				//options.setStatsDumpPeriodSec(30);
				// options.setAllowMmapWrites(true);
				// options.setAllowMmapReads(true);
				options.setMaxOpenFiles(-1);
				// options.setTargetFileSizeBase(512*1024*1024);

				options.setMaxBytesForLevelBase(fsize * 5);
				options.setTargetFileSizeBase(fsize);
				options.setTableFormatConfig(blockConfig);
				File f = new File(fileName + File.separator + i);
				f.mkdirs();
				StartShard sh = new StartShard(i, this.dbs, options, f, bar, ct);
				executor.execute(sh);
				shs.add(sh);
			}
			executor.shutdown();
			try {
				while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
					SDFSLogger.getLog().debug("Awaiting loading of Hashtable.");
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			for (StartShard sh : shs) {
				if (sh.e != null)
					throw sh.e;
			}

			for (int i = 0; i < lockMap.length; i++) {
				lockMap[i] = new ReentrantLock();
				arLockMap[i] = new ReentrantLock();
			}
			ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

			DBOptions options = new DBOptions();
			options.setCreateIfMissing(true);
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
			// LRUCache c = new LRUCache(memperDB);
			// options.setRowCache(c);

			// blockConfig.setBlockCacheSize(memperDB);
			// blockConfig.setNoBlockCache(true);

			// options.setCompactionReadaheadSize(1024*1024*25);
			// options.setUseDirectIoForFlushAndCompaction(true);
			// options.setUseDirectReads(true);
			options.setStatsDumpPeriodSec(30);
			// options.setAllowMmapWrites(true);
			// options.setAllowMmapReads(true);
			options.setMaxOpenFiles(-1);
			options.setCreateMissingColumnFamilies(true);
			options.setRowCache(new LRUCache(memperDB));
			// options.setTargetFileSizeBase(512*1024*1024);

			File f = new File(fileName + File.separator + "rmdb");
			f.mkdirs();
			// options.useFixedLengthPrefixExtractor(3);
			ColumnFamilyOptions coptions0 = new ColumnFamilyOptions();
			ColumnFamilyOptions coptions1 = new ColumnFamilyOptions();
			ColumnFamilyOptions coptions2 = new ColumnFamilyOptions();
			coptions0.setCompactionStyle(CompactionStyle.LEVEL);
			coptions0.setCompressionType(CompressionType.NO_COMPRESSION);
			coptions0.setLevel0FileNumCompactionTrigger(8);
			coptions0.setWriteBufferSize(bufferSize / dbs.length);
			coptions0.setMaxBytesForLevelBase(fsize * 5);
			coptions0.setTargetFileSizeBase(fsize);
			coptions0.setTableFormatConfig(blockConfig);
			// coptions1.setMergeOperatorName("uint64add");
			coptions1.setCompactionStyle(CompactionStyle.LEVEL);
			coptions1.setCompressionType(CompressionType.NO_COMPRESSION);
			coptions1.setLevel0FileNumCompactionTrigger(8);
			coptions1.setWriteBufferSize(bufferSize / dbs.length);
			coptions1.setMaxBytesForLevelBase(fsize * 5);
			coptions1.setTargetFileSizeBase(fsize);
			coptions1.setTableFormatConfig(blockConfig);
			coptions2.setMergeOperatorName("stringappend");
			coptions2.setCompactionStyle(CompactionStyle.LEVEL);
			coptions2.setCompressionType(CompressionType.NO_COMPRESSION);
			coptions2.setLevel0FileNumCompactionTrigger(8);
			coptions2.setWriteBufferSize(bufferSize / dbs.length);
			coptions2.setMaxBytesForLevelBase(fsize * 5);
			coptions2.setTargetFileSizeBase(fsize);
			coptions2.setTableFormatConfig(blockConfig);

			columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, coptions0));
			columnFamilyDescriptors.add(new ColumnFamilyDescriptor("adb".getBytes(), coptions1));
			columnFamilyDescriptors.add(new ColumnFamilyDescriptor("refdb".getBytes(), coptions2));
			rmdb = RocksDB.open(options, f.getPath(), columnFamilyDescriptors, cf);
			System.out.println("Connecting Cassandra DB Column Tables = " + cf.size());
			for (InetSocketAddress addr : Main.volume.getCassandraNodes()) {
				System.out.println(addr);
			}
			if (Main.volume.isClustered()) {
				cdb = new CassandraDedupeDB(Main.volume.getCassandraNodes(), Main.volume.getDataCenter(),
						Main.volume.getUuid(), Main.volume.getClusterCopies().intValue(), true);

			}
			HashBlobArchive.registerEventBus(this);
			bar.finish();

			this.setUp();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void hashBlobArchiveSync(ArchiveSync evt) throws Exception {
		ArrayList<byte[]> al = evt.getHashes();
		for (byte[] b : al) {
			Lock l = this.getLock(b);
			l.lock();
			try {
				ByteBuffer bf = this.tempHt.get(new ByteArrayWrapper(b));
				if (bf != null) {
					RocksDB db = this.getDB(b);
					byte[] v = null;
					try {
						v = db.get(b);
						if (v != null) {
							throw new Exception(
									"Persistent Hashtable already has an entry that exists in the temp hashtable");
						} else {
							db.put(wo, b, bf.array());
						}
						this.tempHt.remove(new ByteArrayWrapper(b));
					} catch (Exception e) {
						SDFSLogger.getLog().warn(
								"unable to commit " + StringUtils.getHexString(b) + " id=" + evt.getID(),
								e);
						throw new Exception();
					}
				}
			} finally {
				l.unlock();
			}
		}

	}

	private ReentrantLock getLock(byte[] key) {
		int l = key[0];
		if (l < 0) {
			l = ((l * -1) + 127);
		}
		return lockMap[l];
	}

	private ReentrantLock getArchiveLock(long archive) {
		archive = Math.abs(archive);
		long val = archive / (Long.MAX_VALUE / this.arLockMap.length);
		int l = Math.toIntExact(val);
		if (l < 0) {
			l = ((l * -1) + 127);
		}
		return this.arLockMap[l];
	}

	private RocksDB getDB(byte[] key) {
		int l = key[key.length - 1];
		if (l < 0) {
			l = ((l * -1) + 127);
		}
		return dbs[l / multiplier];
	}

	@Subscribe
	@AllowConcurrentEvents
	public void hashBlobArchiveUploaded(HashBlobArchiveUploaded evt) {
		if (Main.volume.isClustered()) {
			long id = evt.getArchive().getID();
			byte[] bid = new byte[8];
			ByteBuffer _bid = ByteBuffer.wrap(bid);
			_bid.putLong(id);
			try {
				if (this.rmdb.get(this.cf.get(0), bid) == null) {
					byte[] v = rmdb.get(this.cf.get(2), bid);
					cdb.insertHashes(id, new String(v).split(","), Main.volume.getSerialNumber());
					String[] hsh = new String(v).split(",");
					cdb.setHashes(hsh, id);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to do update for " + id, e);
			}
		}
	}

	@Override
	public boolean claimKey(byte[] hash, long val, long ct) throws IOException {
		Lock gl = this.gclock.readLock();
		gl.lock();
		Lock l = this.getLock(hash);
		l.lock();
		Lock al = this.getArchiveLock(val);
		al.lock();
		try {
			byte[] v = null;
			byte[] id = new byte[8];
			ByteBuffer bid = ByteBuffer.wrap(id);
			bid.putLong(val);
			v = rmdb.get(this.cf.get(1), id);
			if (v == null && Main.volume.isClustered()) {
				long ha = this.cdb.getHash(hash);
				if (ha != -1 && ha != val) {
					SDFSLogger.getLog().warn("When updating reference count for key [" + StringUtils.getHexString(hash)
							+ "] hash locations didn't match cassandra stored val=" + ha + " request value=" + val);
					return false;
				} else if (ha != -1 && ct > 0) {
					v = new byte[8];
					ByteBuffer bf = ByteBuffer.wrap(v);
					bf.putLong(ha);
					this.getDB(hash).put(wo, hash, v);
					rmdb.put(this.cf.get(1), wo, v, LongConverter.toBytes(ct));
					rmdb.merge(this.cf.get(2), wo, v, BaseEncoding.base64Url().encode(hash).getBytes());
					SDFSLogger.getLog().debug("added " + ct + " from " + ha);
					cdb.claimArchive(ha, Main.volume.getSerialNumber());
					HashBlobArchive.claimBlock(ha);
					return true;
				}
			}
			if (v != null) {
				long cct = LongConverter.toLong(v);
				long nc = ct + cct;

				if (nc <= 0 && ct < 0) {
					byte[] rv = new byte[8];
					ByteBuffer rbk = ByteBuffer.wrap(rv);
					rbk.putLong(System.currentTimeMillis() + rmthreashold);
					rmdb.put(this.cf.get(0), wo, id, rv);
					SDFSLogger.getLog().debug("adding removal record for " + ByteBuffer.wrap(id).getLong());

				} else if (rmdb.get(id) != null) {
					rmdb.delete(this.cf.get(0), id);
				}
				SDFSLogger.getLog()
						.debug("incremented " + ct + " to " + ByteBuffer.wrap(id).getLong() + " current ct is " + nc);
				rmdb.put(this.cf.get(1), wo, id, LongConverter.toBytes(nc));
				return true;
			}

			SDFSLogger.getLog()
					.warn("When updating reference count. Key [" + StringUtils.getHexString(hash) + "] not found");
			return false;
		} catch (RocksDBException e) {
			throw new IOException(e);
		} finally {
			al.unlock();
			l.unlock();
			gl.unlock();
		}
	}

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	@Override
	public long getSize() {
		try {
			long sz = 0;
			for (RocksDB db : dbs) {
				sz += db.getLongProperty("rocksdb.estimate-num-keys");
			}
			return sz;
		} catch (RocksDBException e) {
			SDFSLogger.getLog().error("unable to get lenght for rocksdb", e);
			return 0;
		}
	}

	@Override
	public long getUsedSize() {

		try {
			long sz = 0;
			for (RocksDB db : dbs) {
				sz += db.getLongProperty("rocksdb.estimate-num-keys");
			}
			return sz * Main.CHUNK_LENGTH;
		} catch (RocksDBException e) {
			SDFSLogger.getLog().error("unable to get lenght for rocksdb", e);
			return 0;
		}
	}

	@Override
	public long getMaxSize() {
		return this.size;
	}

	@Override
	public synchronized long claimRecords(SDFSEvent evt, boolean compact) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		long rmk = 0;
		try {

			if (Main.volume.isClustered() && cdb.isMaster()) {
				Iterator<Entry<Long, Long>> riter = this.cdb.getAllRmrg();
				while (riter.hasNext()) {
					Entry<Long, Long> et = riter.next();
					if (et.getValue() < (System.currentTimeMillis() - rmthreashold)) {
						long ct = this.cdb.getRefCt(et.getKey());
						if (ct <= 0) {
							SDFSLogger.getLog().debug("Removing cassandra reference for " + et.getKey());
							this.cdb.deleteRef(et.getKey());
						} else {
							this.cdb.delRMRef(et.getKey());
						}
					}
				}
			}
			RocksIterator iter = rmdb.newIterator(cf.get(0));
			SDFSLogger.getLog().info("Removing hashes");
			ByteBuffer bk = ByteBuffer.allocateDirect(8);

			for (iter.seekToFirst(); iter.isValid(); iter.next()) {
				byte[] id = iter.key();

				try {

					SDFSLogger.getLog().debug("removing key " + ByteBuffer.wrap(id).getLong());
					byte[] v = null;
					bk.position(0);
					bk.put(iter.value());
					bk.position(0);
					long tm = bk.getLong();
					if (tm < System.currentTimeMillis()) {

						Lock al = this.getArchiveLock(ByteBuffer.wrap(id).getLong());
						al.lock();

						try {
							if (rmdb.get(this.cf.get(0), id) != null) {
								byte[] kb = this.rmdb.get(cf.get(2), id);
								if (kb != null) {
									String[] hsh = new String(kb).split(",");
									Lock l = this.gclock.writeLock();
									l.lock();
									try {
										v = rmdb.get(this.cf.get(1), id);
										long ct = 0;
										if (v != null)
											ct = LongConverter.toLong(v);

										if (ct <= 0) {
											for (String hs : hsh) {
												byte[] z = BaseEncoding.base64Url().decode(hs);
												// byte [] bid = this.getDB(z).get(z);
												this.getDB(z).delete(z);
												ChunkData ck = new ChunkData(ByteBuffer.wrap(id).getLong(), z);
												ck.setmDelete(true);
											}
											this.rmdb.delete(cf.get(0), id);
											this.rmdb.delete(cf.get(1), id);
											this.rmdb.delete(cf.get(2), id);
											if (Main.volume.isClustered()) {
												this.cdb.unClaimArchive(ByteBuffer.wrap(id).getLong(),
														Main.volume.getSerialNumber());
												if (this.cdb.getRefCt(ByteBuffer.wrap(id).getLong()) <= 0) {
													this.cdb.addRMRef(ByteBuffer.wrap(id).getLong());
												}
											}
											rmk++;
										} else {
											this.rmdb.delete(cf.get(0), id);
										}

									} finally {
										l.unlock();
									}

								} else {
									SDFSLogger.getLog().warn(
											"HashList not found during GC for id " + ByteBuffer.wrap(id).getLong());
								}
							}
						} finally {
							al.unlock();
						}

					}
				} finally {

				}
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
		this.size = maxSz;

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
		long sz = 0;
		for (RocksDB db : dbs) {
			// System.out.println("s="+i);
			sz += db.getLongProperty("rocksdb.estimate-num-keys");
		}
		long size = sz;
		this.closed = false;
		return size;
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
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		try {
			byte[] v = this.getDB(key).get(key);
			if (v == null)
				return false;
			else
				return true;
		} catch (RocksDBException e) {
			throw new IOException(e);
		}

	}

	@Override
	public InsertRecord put(ChunkData cm) throws IOException, HashtableFullException {
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName + " is close");
		// this.flushFullBuffer();
		return this.put(cm, true);
	}

	@Override
	public InsertRecord put(ChunkData cm, boolean persist) throws IOException, HashtableFullException {
		// persist = false;
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName + " is close");
		// if (persist)
		// this.flushFullBuffer();
		Lock gl = this.gclock.readLock();
		gl.lock();
		Lock l = this.getLock(cm.getHash());
		l.lock();
		try {

			try {
				byte[] v = null;
				if (this.tempHt.containsKey(new ByteArrayWrapper(cm.getHash()))) {
					ByteBuffer bk = this.tempHt.get(new ByteArrayWrapper(cm.getHash()));
					v = bk.array();
				}
				RocksDB db = this.getDB(cm.getHash());
				boolean clusterFound = false;
				if (v == null)
					v = db.get(cm.getHash());
				if (v == null) {
					long hl = -1;
					if (Main.volume.isClustered()) {
						hl = cdb.getHash(cm.getHash());
						if (hl != -1) {
							Lock al = this.getArchiveLock(hl);
							al.lock();
							try {
								v = new byte[8];
								ByteBuffer bf = ByteBuffer.wrap(v);
								bf.putLong(hl);
								if (rmdb.get(v) == null) {
									clusterFound = true;

								}
							} finally {
								al.unlock();
							}
						}
					}
					if (hl == -1) {
						try {
							cm.persistData(true);
						} catch (org.opendedup.collections.HashExistsException e) {
							cm.setcPos(e.getPos());
						}
						Lock al = this.getArchiveLock(cm.getcPos());
						al.lock();
						try {
							v = new byte[8];
							ByteBuffer bf = ByteBuffer.wrap(v);
							bf.putLong(cm.getcPos());

							/*
							 * if (cm.references <= 0) bf.putLong(1); else bf.putLong(cm.references);
							 */
							// db.put(wo, cm.getHash(), v);

							long ct = cm.references;
							if (cm.references <= 0)
								ct = 1;

							this.rmdb.delete(this.cf.get(0), v);
							rmdb.put(this.cf.get(1), v, LongConverter.toBytes(ct));
							rmdb.merge(this.cf.get(2), v, BaseEncoding.base64Url().encode(cm.getHash()).getBytes());
							this.tempHt.put(new ByteArrayWrapper(cm.getHash()), bf);
							return new InsertRecord(true, cm.getcPos());
						} finally {
							al.unlock();
						}
					} else {
						Lock al = this.getArchiveLock(hl);
						al.lock();
						try {
							v = new byte[8];
							ByteBuffer bf = ByteBuffer.wrap(v);
							bf.putLong(hl);
							long ct = cm.references;
							if (cm.references <= 0)
								ct = 1;
							byte[] oct = rmdb.get(this.cf.get(1), v);
							if (oct == null) {
								HashBlobArchive.claimBlock(hl);
								cdb.claimArchive(hl, Main.volume.getSerialNumber());
							}
							rmdb.put(this.cf.get(1), v, LongConverter.toBytes(ct));
							rmdb.merge(this.cf.get(2), v, BaseEncoding.base64Url().encode(cm.getHash()).getBytes());
							if (Main.volume.isClustered() && clusterFound) {

							}
							this.tempHt.put(new ByteArrayWrapper(cm.getHash()), bf);
							return new InsertRecord(false, hl);
						} finally {
							al.unlock();
						}
					}
				} else {
					// SDFSLogger.getLog().info("Hash Found");
					Lock al = this.getArchiveLock(cm.getcPos());
					al.lock();
					try {
						ByteBuffer bk = ByteBuffer.wrap(v);
						long pos = bk.getLong();
						// db.put(cm.getHash(), v);
						long ct = cm.references;
						if (cm.references <= 0)
							ct = 1;
						byte[] octb = rmdb.get(this.cf.get(1), v);
						if (octb == null) {
							SDFSLogger.getLog().warn("refernce count table could not find "
									+ ByteBuffer.wrap(v).getLong() + ". This could be due to garbage collection");
							return this.put(cm, persist);
						} else {
							long oct = LongConverter.toLong(octb);
							if (oct < 0) {
								this.rmdb.delete(this.cf.get(0), v);
							}
							rmdb.put(this.cf.get(1), v, LongConverter.toBytes(ct + oct));
							return new InsertRecord(false, pos);
						}
					} finally {
						al.unlock();
					}
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}

		} finally {

			l.unlock();
			gl.unlock();
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
		throw new IOException("not implemented");
	}

	public boolean isClaimed(ChunkData cm) throws KeyNotFoundException, IOException {
		throw new IOException("not implemented");
	}

	@Override
	public long get(byte[] key) throws IOException {
		Lock l = this.getLock(key);
		l.lock();
		try {

			try {
				if (this.tempHt.containsKey(new ByteArrayWrapper(key))) {
					ByteBuffer bf = this.tempHt.get(new ByteArrayWrapper(key));
					bf.position(0);
					return bf.getLong();
				}
				byte[] v = this.getDB(key).get(key);
				if (v == null) {
					return -1;
				} else {
					ByteBuffer bk = ByteBuffer.wrap(v);
					return bk.getLong();
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}

		} finally {
			l.unlock();
		}
	}

	@Override
	public byte[] getData(byte[] key) throws IOException, DataArchivedException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
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
			throw new IOException("Hashtable " + this.fileName + " is close");
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
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		Lock l = this.getLock(cm.getHash());
		l.lock();
		try {

			try {
				RocksDB db = this.getDB(cm.getHash());
				byte[] v = db.get(cm.getHash());
				if (v == null) {

					return false;
				} else {
					db.delete(cm.getHash());
					return true;
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}

		} finally {

			l.unlock();
		}
	}

	private ReentrantLock syncLock = new ReentrantLock();

	@Override
	public void sync() throws IOException {
		syncLock.lock();
		try {
			if (this.isClosed()) {
				throw new IOException("hashtable [" + this.fileName + "] is close");
			}
			try {
				for (RocksDB db : dbs) {
					db.flush(new FlushOptions().setWaitForFlush(true));
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} finally {
			syncLock.unlock();
		}
	}

	@Override
	public void close() {
		this.syncLock.lock();
		try {
			this.closed = true;
			CommandLineProgressBar bar = new CommandLineProgressBar("Closing Hash Tables", dbs.length, System.out);
			int i = 0;
			for (RocksDB db : dbs) {
				db.close();
				bar.update(i);
				i++;
			}
			bar.finish();

		} finally {
			this.syncLock.unlock();
			SDFSLogger.getLog().info("Hashtable [" + this.fileName + "] closed");
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
		try {
			for (RocksDB db : dbs)
				db.compactRange();
		} catch (RocksDBException e) {
			throw new IOException(e);
		}

	}

	@Override
	public void rollbackCompact() throws IOException {
		FileUtils.deleteDirectory(new File(this.fileName));

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
	public boolean mightContainKey(byte[] key, long aid) {
		Lock gl = this.gclock.readLock();
		gl.lock();
		Lock al = this.getArchiveLock(aid);
		al.lock();
		try {
			byte[] v = null;
			byte[] id = new byte[8];
			ByteBuffer bid = ByteBuffer.wrap(id);
			bid.putLong(aid);
			v = rmdb.get(this.cf.get(1), id);
			if (v == null) {
				return false;
			} else {
				long ct = LongConverter.toLong(v);
				if (ct <= 0)
					return false;
				else
					return true;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to check key " + BaseEncoding.base64Url().encode(key) + " with id " + aid);
			return true;
		} finally {
			al.unlock();
			gl.unlock();
		}
	}

	public static class StartShard implements Runnable {

		RocksDB[] dbs = null;
		int n = -1;
		Options options = null;
		String path = null;
		public Exception e = null;
		CommandLineProgressBar bar = null;
		AtomicInteger ct = null;

		public StartShard(int n, RocksDB[] dbs, Options options, File f, CommandLineProgressBar bar, AtomicInteger ct) {
			this.dbs = dbs;
			this.n = n;
			this.options = options;
			this.path = f.getPath();
			this.bar = bar;
			this.ct = ct;
		}

		@Override
		public void run() {
			try {
				dbs[n] = RocksDB.open(options, path);
				// System.out.println(dbs[n].toString());
			} catch (Exception e) {
				this.e = e;
			}
			bar.update(ct.incrementAndGet());

		}

	}

	public static void main(String[] args) throws RocksDBException {
		RocksDB db;
		List<ColumnFamilyDescriptor> colFamily = new ArrayList<ColumnFamilyDescriptor>();
		List<ColumnFamilyHandle> colFamilyHandles = new ArrayList<ColumnFamilyHandle>();
		RocksDB.loadLibrary();
		BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
		// ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
		// DBOptions dbo = new DBOptions();
		// cfOptions.optimizeLevelStyleCompaction();
		// cfOptions.optimizeForPointLookup(8192);
		blockConfig.setFilter(new BloomFilter(16, false));
		// blockConfig.setHashIndexAllowCollision(false);
		// blockConfig.setCacheIndexAndFilterBlocks(false);
		// blockConfig.setIndexType(IndexType.kBinarySearch);
		// blockConfig.setPinL0FilterAndIndexBlocksInCache(true);
		blockConfig.setBlockSize(4 * 1024);
		blockConfig.setFormatVersion(2);
		blockConfig.setNoBlockCache(true);
		blockConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
		@SuppressWarnings("resource")
		DBOptions options = new DBOptions().setCreateIfMissing(true);
		ColumnFamilyOptions coptions1 = new ColumnFamilyOptions();
		coptions1.setMergeOperatorName("uint64add");
		coptions1.setCompactionStyle(CompactionStyle.LEVEL);
		coptions1.setCompressionType(CompressionType.NO_COMPRESSION);
		coptions1.setLevel0FileNumCompactionTrigger(8);
		coptions1.setWriteBufferSize(4096L);
		// coptions1.setMaxBytesForLevelBase(fsize * 5);
		// coptions1.setTargetFileSizeBase(fsize);
		coptions1.setTableFormatConfig(blockConfig);
		options.setMaxBackgroundFlushes(1);
		colFamily.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, coptions1));

		options.setCreateMissingColumnFamilies(true);
		db = RocksDB.open(options, "c:/temp", colFamily, colFamilyHandles);
		byte[] k = "6442".getBytes();
		ByteBuffer bk = ByteBuffer.wrap(new byte[8]).order(ByteOrder.nativeOrder());
		bk.putLong(10);
		for (int i = 0; i < 10000; i++) {
			db.merge(k, bk.array());
		}
		bk.position(0);
		bk.put(db.get(k));
		bk.position(0);
		System.out.println(bk.getLong());
		db.close();

	}

}