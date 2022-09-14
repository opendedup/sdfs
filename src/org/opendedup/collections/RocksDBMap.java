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
import java.util.ArrayList;
import java.util.Arrays;
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

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.io.events.ArchiveSync;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.CommandLineProgressBar;
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
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;

public class RocksDBMap implements AbstractMap, AbstractHashesMap {
	WriteOptions wo = new WriteOptions();
	WriteOptions owo = new WriteOptions();
	// RocksDB db = null;
	String fileName = null;
	ReentrantLock[] lockMap = new ReentrantLock[256];
	RocksDB[] dbs = new RocksDB[8];
	RocksDB rmdb = null;
	ColumnFamilyHandle rmdbHsAr = null;
	RocksDB armdb = null;
	ColumnFamilyHandle armdbHsAr = null;
	private static final long GB = 1024 * 1024 * 1024;
	private static final long MB = 1024 * 1024;
	int multiplier = 0;
	boolean closed = false;
	private long size = 0;
	// Remove unreferenced data if older than 15 minutes
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(2);
	private transient ThreadPoolExecutor executor = null;
	private transient BlockingQueue<Runnable> aworksQueue = new ArrayBlockingQueue<Runnable>(2);
	private transient ThreadPoolExecutor arExecutor = new ThreadPoolExecutor(Main.dseIOThreads, Main.dseIOThreads + 1,
			10, TimeUnit.SECONDS, aworksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
	private List<String> colFamily = new ArrayList<String>();
	FlushOptions flo = null;
	private ConcurrentHashMap<ByteArrayWrapper, ByteBuffer> tempHt = new ConcurrentHashMap<ByteArrayWrapper, ByteBuffer>(
			1024, 0.75f, Main.writeThreads);
	private AtomicLong rmct = new AtomicLong();
	private AtomicLong nrmct = new AtomicLong();
	private AtomicLong trmct = new AtomicLong();
	private AtomicLong tnrmct = new AtomicLong();
	// private AtomicLong szct = new AtomicLong();

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
			long bufferSize = GB;
			long fsize = 128 * MB;
			if (this.size < 1_000_000_000L) {
				SDFSLogger.getLog().info("Setting up Small Hash Table");
				fsize = 128 * MB;
				bufferSize = fsize * dbs.length;
			} else if (this.size < 10_000_000_000L) {
				SDFSLogger.getLog().info("Setting up Medium Hash Table");
				fsize = 256 * MB;
				bufferSize = GB * dbs.length;
			} else if (this.size < 20_000_000_000L) {
				SDFSLogger.getLog().info("Setting up Large Hash Table");
				fsize = GB;
				bufferSize = GB * dbs.length;
			} else {
				SDFSLogger.getLog().info("Setting up XL Hash Table");
				// long mp = this.size / 10_000_000_000L;

				fsize = 256 * MB;
				bufferSize = 128 * MB * dbs.length;
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
			owo = new WriteOptions();
			owo.setDisableWAL(true);
			owo.setSync(false);
			flo = new FlushOptions();
			flo.setWaitForFlush(false);
			// LRUCache c = new LRUCache(totmem);
			CommandLineProgressBar bar = new CommandLineProgressBar("Loading Existing Hash Tables", dbs.length + 1,
					System.out);
			executor = new ThreadPoolExecutor(dbs.length, dbs.length + 1, 10, TimeUnit.SECONDS, worksQueue,
					new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
			AtomicInteger ct = new AtomicInteger();
			ArrayList<StartShard> shs = new ArrayList<StartShard>();
			colFamily.add("default");
			for (int i = 0; i < dbs.length; i++) {
				File f = new File(fileName + File.separator + i);
				int version = 2;
				if (!f.exists()) {
					version = 4;
				}
				BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
				// ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
				// DBOptions dbo = new DBOptions();
				// cfOptions.optimizeLevelStyleCompaction();
				// cfOptions.optimizeForPointLookup(8192);
				// blockConfig.setFilter(new BloomFilter(10, false));
				// blockConfig.setHashIndexAllowCollision(false);
				// blockConfig.setCacheIndexAndFilterBlocks(false);
				// blockConfig.setIndexType(IndexType.kBinarySearch);
				// blockConfig.setPinL0FilterAndIndexBlocksInCache(true);
				blockConfig.setBlockSize(4 * 1024);

				blockConfig.setNoBlockCache(true);

				// options.useFixedLengthPrefixExtractor(3);

				Options options = new Options();
				options.setCreateIfMissing(true);
				options.setCompactionStyle(CompactionStyle.LEVEL);
				options.setCompressionType(CompressionType.NO_COMPRESSION);

				// options.setMinWriteBufferNumberToMerge(2);
				// options.setMaxWriteBufferNumber(6);
				// options.setLevelZeroFileNumCompactionTrigger(2);
				Env env = Env.getDefault();
				options.setEnv(env);
				options.setCompactionReadaheadSize(1024 * 1024 * 25);
				// options.setUseDirectIoForFlushAndCompaction(true);
				// options.setUseDirectReads(true);
				options.setStatistics(new org.rocksdb.Statistics());
				options.setStatsDumpPeriodSec(300);
				options.setLevel0FileNumCompactionTrigger(2);

				// options.setAccessHintOnCompactionStart(AccessHint.WILLNEED);
				options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
				options.setAdviseRandomOnOpen(true);
				// options.setNumLevels(8);
				// options.setLevelCompactionDynamicLevelBytes(true);
				//
				options.setAllowConcurrentMemtableWrite(true);
				// LRUCache c = new LRUCache(memperDB);
				// options.setRowCache(c);
				// blockConfig.setBlockCacheSize(GB * 2);
				blockConfig.setFilterPolicy(new BloomFilter(16, false));
				options.setWriteBufferSize(bufferSize / dbs.length);
				options.setMaxWriteBufferNumber(4);
				options.setMinWriteBufferNumberToMerge(2);
				options.setMaxBytesForLevelBase(fsize * 5);
				options.setTargetFileSizeBase(fsize);

				// blockConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
				blockConfig.setFormatVersion(version);
				if (version == 4) {
					blockConfig.setIndexType(IndexType.kBinarySearch);
				} else {
					blockConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
				}
				if (Main.MAX_OPEN_SST_FILES > 0) {
					options.setMaxOpenFiles(Main.MAX_OPEN_SST_FILES / dbs.length);
					SDFSLogger.getLog()
							.info("Setting Maximum Open SST Files to " + (Main.MAX_OPEN_SST_FILES / dbs.length));
				} else {
					options.setMaxOpenFiles(-1);
				}
				options.setTableFormatConfig(blockConfig);
				// options.setAllowMmapWrites(true);
				// options.setAllowMmapReads(true);

				// options.setTargetFileSizeBase(512 * 1024 * 1024);
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
			}

			DBOptions options = new DBOptions();
			ColumnFamilyOptions familyOptions = new ColumnFamilyOptions();
			options.setCreateIfMissing(true);

			BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
			blockConfig.setFilterPolicy(new BloomFilter(16, false));
			// blockConfig.setHashIndexAllowCollision(false);
			// blockConfig.setCacheIndexAndFilterBlocks(false);
			// blockConfig.setIndexType(IndexType.kBinarySearch);
			// blockConfig.setPinL0FilterAndIndexBlocksInCache(true);
			blockConfig.setBlockSize(4 * 1024);
			// options.setNumLevels(8);
			// options.setLevelCompactionDynamicLevelBytes(true);
			//

			// LRUCache c = new LRUCache(memperDB);
			// options.setRowCache(c);

			// blockConfig.setBlockCacheSize(memperDB);
			blockConfig.setNoBlockCache(true);

			familyOptions.setCompactionStyle(CompactionStyle.LEVEL);
			familyOptions.setCompressionType(CompressionType.NO_COMPRESSION);
			familyOptions.setWriteBufferSize(GB);
			familyOptions.setMinWriteBufferNumberToMerge(2);
			familyOptions.setMaxWriteBufferNumber(6);
			familyOptions.setLevelZeroFileNumCompactionTrigger(2);

			Env env = Env.getDefault();
			options.setEnv(env);
			familyOptions.setMaxBytesForLevelBase(GB);
			familyOptions.setTargetFileSizeBase(128 * 1024 * 1024);
			// options.setAllowMmapWrites(true);
			// options.setAllowMmapReads(false);
			if (Main.MAX_OPEN_SST_FILES > 0) {
				options.setMaxOpenFiles(Main.MAX_OPEN_SST_FILES);
				SDFSLogger.getLog().info("Setting Maximum Open SST Files to " + Main.MAX_OPEN_SST_FILES);
			} else {
				options.setMaxOpenFiles(-1);
			}
			familyOptions.setTargetFileSizeBase(512 * 1024 * 1024);
			familyOptions.setTableFormatConfig(blockConfig);
			File f = new File(fileName + File.separator + "rmdb");

			blockConfig.setFormatVersion(4);
			f.mkdirs();
			ColumnFamilyDescriptor _hsArD = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, familyOptions);
			List<ColumnFamilyDescriptor> descriptors = new ArrayList<ColumnFamilyDescriptor>();
			descriptors.add(_hsArD);
			List<ColumnFamilyHandle> handles = new ArrayList<ColumnFamilyHandle>();
			rmdb = RocksDB.open(options, f.getPath(), descriptors, handles);
			for (ColumnFamilyHandle handle : handles) {
				if (Arrays.equals(handle.getName(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
					this.rmdbHsAr = handle;
				}
			}
			DBOptions dboptions = new DBOptions();
			dboptions.setCreateMissingColumnFamilies(true);
			dboptions.setCreateIfMissing(true).setAllowMmapReads(true).setAllowMmapWrites(true);
			// options.setAllowMmapWrites(true);
			// options.setAllowMmapReads(false);
			if (Main.MAX_OPEN_SST_FILES > 0) {
				dboptions.setMaxOpenFiles(Main.MAX_OPEN_SST_FILES);
				SDFSLogger.getLog().info("Setting Maximum Open SST Files to " + Main.MAX_OPEN_SST_FILES);
			} else {
				dboptions.setMaxOpenFiles(-1);
			}
			familyOptions = new ColumnFamilyOptions();
			familyOptions.setTargetFileSizeBase(512 * 1024 * 1024);
			familyOptions.setTableFormatConfig(blockConfig);
			familyOptions.setCompactionStyle(CompactionStyle.LEVEL);
			familyOptions.setCompressionType(CompressionType.NO_COMPRESSION);
			familyOptions.setWriteBufferSize(GB);
			File af = new File(fileName + File.separator + "armdb");
			af.mkdirs();
			_hsArD = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, familyOptions);
			descriptors = new ArrayList<ColumnFamilyDescriptor>();
			descriptors.add(_hsArD);
			handles = new ArrayList<ColumnFamilyHandle>();
			armdb = RocksDB.open(dboptions, af.getPath(), descriptors, handles);
			for (ColumnFamilyHandle handle : handles) {
				if (Arrays.equals(handle.getName(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
					this.armdbHsAr = handle;
				}
			}
			HashBlobArchive.registerEventBus(this);
			bar.finish();
			this.setUp();
			if (Main.runCompact) {
				ThreadPoolExecutor zexecutor = new ThreadPoolExecutor(dbs.length, dbs.length + 1, 10, TimeUnit.SECONDS,
						worksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);

				for (RocksDB db : dbs) {
					CompactShard cs = new CompactShard(db);
					SDFSLogger.getLog().info("compacting db");
					zexecutor.execute(cs);
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	private ReentrantLock getLock(byte[] key) {
		int l = key[0];
		if (l < 0) {
			l = ((l * -1) + 127);
		}
		return lockMap[l];
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
	public void hashBlobArchiveSync(ArchiveSync evt) throws Exception {
		arExecutor.execute(new CommitArchive(this, evt));
	}

	@Override
	public long claimKey(byte[] hash, long val, long ct) throws IOException {
		Lock l = this.getLock(hash);
		l.lock();
		try {
			if (this.tempHt.containsKey(new ByteArrayWrapper(hash))
					&& this.tempHt.get(new ByteArrayWrapper(hash)).position(0).getLong() == val) {
				ByteBuffer bk = this.tempHt.get(new ByteArrayWrapper(hash));
				bk.position(8);
				ct += bk.getLong();
				ByteBuffer keyb = ByteBuffer.wrap(new byte[hash.length + 8]);
				keyb.put(hash);
				keyb.putLong(val);
				byte[] key = keyb.array();
				if (ct <= 0) {
					bk.position(8);
					bk.putLong(System.currentTimeMillis() + Main.HT_RM_THRESH);
					rmdb.put(this.rmdbHsAr, wo, key, bk.array());
					this.tempHt.remove(new ByteArrayWrapper(hash), bk);
					trmct.incrementAndGet();
				} else {
					if (rmdb.get(this.rmdbHsAr, key) != null) {
						rmdb.delete(this.rmdbHsAr, key);
						rmct.decrementAndGet();
						trmct.decrementAndGet();
					}
					bk.putLong(8, ct);
					bk.position(0);
					this.tempHt.put(new ByteArrayWrapper(hash), bk);
				}
				return val;
			} else {
				byte[] v = null;
				v = this.getDB(hash).get(hash);
				if (v != null && ByteBuffer.wrap(v).getLong() == val) {
					ByteBuffer bk = ByteBuffer.wrap(v);
					long oval = bk.getLong();

					long oct = ct;
					ct += bk.getLong();
					ByteBuffer keyb = ByteBuffer.wrap(new byte[hash.length + 8]);
					keyb.put(hash);
					keyb.putLong(val);
					byte[] key = keyb.array();
					if (ct <= 0 && oct < 0) {
						ByteBuffer _rbk = ByteBuffer.wrap(new byte[16]);
						_rbk.putLong(val);
						_rbk.putLong(System.currentTimeMillis() + Main.HT_RM_THRESH);
						rmdb.put(this.rmdbHsAr, key, _rbk.array());
						getDB(hash).delete(hash);
						rmct.incrementAndGet();
						// this.szct.decrementAndGet();
					} else {
						if (rmdb.get(this.rmdbHsAr, key) != null) {
							rmdb.delete(this.rmdbHsAr, key);
							rmct.decrementAndGet();
						}
						if (v.length >= 24) {
							bk.putLong(v.length - 16, ct);
						} else {
							bk.putLong(v.length - 8, ct);
						}
						nrmct.incrementAndGet();
						getDB(hash).put(wo, hash, v);
						// this.szct.incrementAndGet();
					}
					return oval;
				} else {
					ByteBuffer keyb = ByteBuffer.wrap(new byte[hash.length + 8]);
					keyb.put(hash);
					keyb.putLong(val);
					byte[] key = keyb.array();
					v = this.armdb.get(this.armdbHsAr, hash);

					if (v != null) {
						ByteBuffer bk = ByteBuffer.wrap(v);
						byte[] nb = this.getArVal(bk, val);
						if (nb.length > 0) {
							ByteBuffer _nbf = ByteBuffer.wrap(nb);
							_nbf.position(8);
							long refs = _nbf.getLong();
							long oct = ct;
							ct += refs;
							if (ct <= 0 && oct < 0) {
								rmdb.put(this.rmdbHsAr, key, nb);
								ByteBuffer _bf = this.removeArRef(bk, val);
								byte[] _val = _bf.array();
								if (_val.length == 0) {
									this.armdb.delete(this.armdbHsAr, hash);

								} else {
									this.armdb.put(this.armdbHsAr, hash, _val);
								}
								// this.abdt.decrementAndGet();

							} else {
								if (rmdb.get(this.rmdbHsAr, key) != null) {
									rmdb.delete(this.rmdbHsAr, key);
									rmct.decrementAndGet();
								}
								this.setArRefs(bk, val, ct);
								this.armdb.put(this.armdbHsAr, hash, bk.array());
							}
							return val;
						}
					}
				}
			}
			if (ct > 0) {
				try {
					throw new Exception();
				} catch (Exception e) {
					SDFSLogger.getLog().warn("When updating reference count. Key [" + StringUtils.getHexString(hash)
							+ "] not found ct requested=" + ct, e);
				}
			}
			return -1;
		} catch (RocksDBException e) {
			throw new IOException(e);
		} finally {
			l.unlock();
		}
	}

	private long getArRefs(ByteBuffer entries, long archive) {
		entries.position(0);
		while (entries.hasRemaining()) {
			long _ar = entries.getLong();
			if (_ar == archive) {
				entries.position(0);
				return entries.getLong();
			} else {
				entries.position(entries.position() + 16);
			}
		}
		entries.position(0);
		return -1;
	}

	private byte[] getArVal(ByteBuffer entries, long archive) {
		entries.position(0);
		while (entries.hasRemaining()) {
			long _ar = entries.getLong();
			if (_ar == archive) {
				byte[] b = new byte[24];
				entries.position(entries.position() - 8);
				entries.get(b);
				return b;
			} else {
				entries.position(entries.position() + 16);
			}
		}
		entries.position(0);
		return new byte[0];
	}

	private void setArRefs(ByteBuffer entries, long archive, long ct) {
		entries.position(0);
		while (entries.hasRemaining()) {
			long _ar = entries.getLong();
			if (_ar == archive) {
				entries.putLong(ct);
			} else if (entries.hasRemaining()) {
				entries.position(entries.position() + 16);
			}
		}
		entries.position(0);
	}

	private ByteBuffer removeArRef(ByteBuffer entries, long archive) {
		try {
			ByteBuffer bf = ByteBuffer.wrap(new byte[entries.capacity() - 24]);
			entries.position(0);
			byte[] lb = new byte[16];
			while (entries.hasRemaining()) {
				long _ar = entries.getLong();
				entries.get(lb);
				if (_ar != archive) {
					bf.putLong(_ar);
					bf.put(lb);
				}
			}
			entries.position(0);
			bf.position(0);
			return bf;
		} catch (IndexOutOfBoundsException e) {
			SDFSLogger.getLog().warn("archive " + archive + " not found", e);
			entries.position(0);
			return entries;
		}
	}

	private ByteBuffer addArRef(ByteBuffer entries, byte[] v) {
		ByteBuffer bf = ByteBuffer.wrap(new byte[entries.capacity() + v.length]);
		bf.put(entries.array());
		bf.put(v);
		return bf;
	}

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	@Override
	public long getSize() {
		this.syncLock.lock();
		try {
			if (!this.closed) {

				try {
					long sz = 0;
					for (RocksDB db : dbs) {
						sz += db.getLongProperty("rocksdb.estimate-num-keys");
					}
					sz += this.tempHt.size();
					return sz;
				} catch (RocksDBException e) {
					SDFSLogger.getLog().error("unable to get lenght for rocksdb", e);
					return 0;
				}
			} else {
				return 0;
			}
		} finally {
			this.syncLock.unlock();
		}
	}

	@Override
	public long getUsedSize() {
		// return this.szct.get();

		this.syncLock.lock();
		try {
			if (!this.closed) {
				long sz = 0;
				for (RocksDB db : dbs) {
					sz += db.getLongProperty("rocksdb.estimate-num-keys");
				}
				return sz + tempHt.size();
			} else {
				return 0;
			}

		} catch (RocksDBException e) {
			SDFSLogger.getLog().error("unable to get lenght for rocksdb", e);
			return 0;
		} finally {
			this.syncLock.unlock();
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
		long hct = 0;
		long dct = 0;
		long ndct = 0;
		try {
			RocksIterator iter = rmdb.newIterator(this.rmdbHsAr);
			SDFSLogger.getLog().info("Removing hashes rmct=" + rmct.get() +
					" nrmct=" + nrmct.get() + " trmct=" + trmct.get() + " tnrmct=" + tnrmct);
			rmct.set(0);
			nrmct.set(0);
			trmct.set(0);
			tnrmct.set(0);
			ByteBuffer bk = ByteBuffer.allocateDirect(24);

			for (iter.seekToFirst(); iter.isValid(); iter.next()) {
				hct++;
				byte[] key = iter.key();
				ByteBuffer keyb = ByteBuffer.wrap(key);
				byte[] hash = new byte[key.length - 8];
				keyb.get(hash);
				Lock l = this.getLock(hash);
				l.lock();
				try {
					if (this.rmdb.get(this.rmdbHsAr, key) != null) {

						bk.position(0);
						bk.put(iter.value());
						bk.position(0);
						long pos = bk.getLong();
						long tm = bk.getLong();

						if (System.currentTimeMillis() > tm) {
							boolean recovered = false;
							byte[] arVal = this.armdb.get(this.armdbHsAr, hash);

							if (arVal != null && this.getArRefs(ByteBuffer.wrap(arVal), pos) > 0) {
								ndct++;
								recovered = true;
								rmdb.delete(this.rmdbHsAr, key);
							}
							byte[] hrVal = this.getDB(hash).get(hash);
							if (hrVal != null && ByteBuffer.wrap(hrVal).getLong() == pos) {

								if (!recovered) {
									ndct++;
									recovered = true;
									rmdb.delete(this.rmdbHsAr, key);
								}
							}
							if (!recovered) {
								ChunkData ck = new ChunkData(pos, hash);
								ck.setmDelete(true);
								rmdb.delete(this.rmdbHsAr, key);
								dct++;
							}
						}
					}

				} finally {
					l.unlock();
				}

			}
			SDFSLogger.getLog()
					.info("Checked [" + hct + "] removed [" + dct + "] reclaimed [" + ndct + "]");
			if (compact) {
				SDFSLogger.getLog().info("compacting archives");
				int i = 0;
				for (RocksDB db : dbs) {
					SDFSLogger.getLog().info("compacting rocksdb " + i);
					db.compactRange();
					i++;
				}
				this.rmdb.compactRange();
				this.armdb.compactRange(this.armdbHsAr);
				SDFSLogger.getLog().info("done compacting rocksdb");
			}

		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to finish Garbage Collection", e);
		}
		return dct;
	}

	@Override
	public synchronized void clearRefMap() throws IOException {
		throw new IOException("not supported");
	}

	AtomicLong csz = new AtomicLong(0);

	public void setMaxSize(long maxSz) throws IOException {
		this.size = maxSz;

	}

	/**
	 * initializes the Object set of this hash table.
	 *
	 * @param initialCapacity an <code>int</code> value
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
	 * @param obj an <code>Object</code> value
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
		Lock l = this.getLock(cm.getHash());

		try {
			// If the key is present in temporary hash table then update whatever exists
			l.lock();
			try {
				if (this.tempHt.containsKey(new ByteArrayWrapper(cm.getHash()))) {
					ByteBuffer bk = this.tempHt.get(new ByteArrayWrapper(cm.getHash()));
					bk.position(0);
					long pos = bk.getLong();
					long ct = bk.getLong();
					if (cm.references <= 0)
						ct++;
					else
						ct += cm.references;
					bk.putLong(8, ct);
					this.tempHt.put(new ByteArrayWrapper(cm.getHash()), bk);
					cm.setcPos(pos);
					return new InsertRecord(false, pos, 0);
				}
			} finally {
				l.unlock();
			}
			// Key not found in temporary hash table
			// Query RocksDB
			// If key does not exist and sync_on_write is false
			// and it is not a case of import,
			// then create an entry in the temporary hash table
			// . The temporary hash table will be flushed later
			// via an ArchiveSync event. Otherwise update the
			// RocksDB directly.
			RocksDB db = this.getDB(cm.getHash());
			byte[] v = null;

			v = db.get(cm.getHash());
			if (v == null) {
				int writtenLen = 0;
				try {
					writtenLen = cm.persistData(true).getCompressedLength();
				} catch (org.opendedup.collections.HashExistsException e) {
					cm.setcPos(e.getPos());
				}
				l.lock();
				try {
					v = db.get(cm.getHash());
					if (v == null) {
						if (this.tempHt.containsKey(new ByteArrayWrapper(cm.getHash()))) {
							ByteBuffer bk = this.tempHt.get(new ByteArrayWrapper(cm.getHash()));
							bk.position(0);
							long pos = bk.getLong();
							long ct = bk.getLong();
							if (cm.references <= 0)
								ct++;
							else
								ct += cm.references;
							bk.putLong(8, ct);
							cm.setcPos(pos);
							this.tempHt.put(new ByteArrayWrapper(cm.getHash()), bk);
							return new InsertRecord(false, pos, 0);
						} else if (db.get(cm.getHash()) != null) {
							return this.put(cm, true);
						} else {
							v = new byte[16];
							ByteBuffer bf = ByteBuffer.wrap(v);
							bf.putLong(cm.getcPos());
							if (cm.references <= 0)
								bf.putLong(1);
							else
								bf.putLong(cm.references);
							this.tempHt.put(new ByteArrayWrapper(cm.getHash()), bf);
							// this.rmdb.delete(cm.getHash());
							return new InsertRecord(true, cm.getcPos(), writtenLen);
						}
					}
				} finally {
					l.unlock();
				}
			}
			l.lock();
			try {
				// SDFSLogger.getLog().info("Hash Found");
				ByteBuffer bk = ByteBuffer.wrap(v);
				long pos = bk.getLong();
				long ct = bk.getLong();
				if (v.length >= 24 && Main.maxAge > -1) {
					long age = bk.getLong(16);
					if (age + Main.maxAge < System.currentTimeMillis()) {
						byte[] arVal = armdb.get(this.armdbHsAr, cm.getHash());
						if (arVal == null) {
							armdb.put(this.armdbHsAr, cm.getHash(), v);

						} else {
							ByteBuffer _nbf = this.addArRef(ByteBuffer.wrap(arVal), v);
							armdb.put(this.armdbHsAr, cm.getHash(), _nbf.array());
						}
						// abdt.incrementAndGet();

						this.getDB(cm.getHash()).delete(cm.getHash());
						// this.szct.decrementAndGet();
						return this.put(cm, persist);
					}
				}

				if (cm.references <= 0)
					ct++;
				else
					ct += cm.references;
				cm.setcPos(pos);
				bk.putLong(8, ct);
				db.put(wo, cm.getHash(), v);
				return new InsertRecord(false, pos, 0);
			} finally {
				l.unlock();
			}
		} catch (RocksDBException e) {
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
		Lock l = this.getLock(cm.getHash());
		l.lock();
		try {

			try {
				RocksDB db = this.getDB(cm.getHash());
				byte[] v = db.get(cm.getHash());
				if (v == null) {

					return false;
				} else {
					ByteBuffer bk = ByteBuffer.wrap(v);
					bk.putLong(0, cm.getcPos());
					db.put(wo, cm.getHash(), v);
					return true;
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}

		} finally {

			l.unlock();
		}
	}

	public boolean isClaimed(ChunkData cm) throws KeyNotFoundException, IOException {
		throw new IOException("not implemented");
	}

	// AtomicLong abdt = new AtomicLong();

	@Override
	public long get(byte[] key) throws IOException {
		// SDFSLogger.getLog().info("key length is " + key.length);
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
					long pos = bk.getLong();
					if (v.length >= 24 && Main.maxAge > -1) {
						long age = bk.getLong(16);
						bk.getLong();
						if (age + Main.maxAge < System.currentTimeMillis()) {
							byte[] arVal = armdb.get(this.armdbHsAr, key);
							if (arVal == null) {
								armdb.put(this.armdbHsAr, key, v);

							} else {
								ByteBuffer _nbf = this.addArRef(ByteBuffer.wrap(arVal), v);
								armdb.put(this.armdbHsAr, key, _nbf.array());
							}
							// abdt.incrementAndGet();
							this.getDB(key).delete(key);
							// this.szct.decrementAndGet();

							return this.get(key);
						}
					}
					return pos;
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
				if (data != null && data.length == 0) {
					SDFSLogger.getLog().warn("Data Lenght is 0");
				}
				SDFSLogger.getLog().warn(" miss for [" + StringUtils.getHexString(key) + "] [" + pos + "] ");
				long opos = pos;
				pos = this.get(key);
				if (pos != -1) {
					SDFSLogger.getLog().warn(
							" miss for [" + StringUtils.getHexString(key) + "] [" + opos + "] found at [" + pos + "]");
					return data = ChunkData.getChunk(key, pos);
				} else {
					SDFSLogger.getLog()
							.warn(" do data found for [" + StringUtils.getHexString(key) + "] [" + pos + "]");
					return null;
				}
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
		throw new IOException("not implemented");
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
					db.flush(flo);
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
			// CommandLineProgressBar bar = new CommandLineProgressBar("Closing Hash
			// Tables", dbs.length + 2, System.out);
			// int i = 0;
			for (Lock l : lockMap) {
				l.lock();
			}

			for (RocksDB db : dbs) {

				try {
					FlushOptions op = new FlushOptions();
					op.setWaitForFlush(true);
					db.flush(op);
					db.close();
					db = null;
				} catch (Exception e) {
					SDFSLogger.getLog().warn("While closing hashtable ", e);
				}
				// bar.update(i);
				// i++;
			}
			dbs = null;
			try {
				this.rmdb.flush(new FlushOptions());
				this.rmdb.close();
				this.rmdb = null;
			} catch (Exception e) {
				SDFSLogger.getLog().warn("While closing hashtable ", e);
			}
			// bar.update(i);
			// i++;
			try {
				this.armdb.flush(new FlushOptions(), this.armdbHsAr);
				this.armdb = null;
			} catch (Exception e) {
				SDFSLogger.getLog().warn("While closing hashtable ", e);
			}
			for (Lock l : lockMap) {
				l.unlock();
			}
			// bar.update(i);
			// i++;
			// bar.finish();

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
		ThreadPoolExecutor zexecutor = new ThreadPoolExecutor(dbs.length, dbs.length + 1, 10, TimeUnit.SECONDS,
				worksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);

		for (RocksDB db : dbs) {

			CompactShard cs = new CompactShard(db);
			zexecutor.execute(cs);
		}
		zexecutor.shutdown();
		try {
			while (!zexecutor.awaitTermination(10, TimeUnit.MINUTES)) {
				SDFSLogger.getLog().debug("Awaiting Compaction of Hashtables to complete.");
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}

	}

	@Override
	public void rollbackCompact() throws IOException {
		// FileUtils.deleteDirectory(new File(this.fileName));

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
	public boolean mightContainKey(byte[] key, long archive) {
		Lock l = this.getLock(key);
		l.lock();
		try {
			if (this.tempHt.containsKey(new ByteArrayWrapper(key))
					&& this.tempHt.get(new ByteArrayWrapper(key)).position(0).getLong() == archive) {
				return true;

			}
			byte[] v = null;
			v = this.getDB(key).get(key);
			if (v != null && ByteBuffer.wrap(v).getLong() == archive) {
				return true;
			}
			v = this.armdb.get(this.armdbHsAr, key);
			if (v != null && this.getArRefs(ByteBuffer.wrap(v), archive) > 0) {
				return true;
			}
			return false;
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to check", e);
			return false;
		} finally {
			l.unlock();
		}
	}

	public void compactArchive() {

	}

	private static class CommitArchive implements Runnable {
		RocksDBMap m = null;
		ArchiveSync evt = null;

		public CommitArchive(RocksDBMap m, ArchiveSync evt) {
			this.m = m;
			this.evt = evt;
		}

		@Override
		public void run() {
			ArrayList<byte[]> al = evt.getHashes();
			for (byte[] b : al) {
				Lock l = m.getLock(b);
				l.lock();
				try {
					ByteBuffer bf = m.tempHt.get(new ByteArrayWrapper(b));
					if (bf != null) {
						bf.position(0);
						long oval = bf.getLong();
						if (oval != evt.getID()) {
							SDFSLogger.getLog().debug(
									"archive id [" + evt.getID() + "] does not equal hashtable id[" + oval + "]");
						} else {
							if (m.closed) {
								SDFSLogger.getLog().info("Unable to sync " + evt.getID() + " during shutdown ");
							} else {
								RocksDB db = m.getDB(b);
								byte[] v = null;
								try {
									v = db.get(b);
									if (v != null) {
										m.tempHt.remove(new ByteArrayWrapper(b));
										throw new Exception(
												"Persistent Hashtable already has an entry that exists in the temp hashtable");
									} else {
										ByteBuffer valb = ByteBuffer.wrap(new byte[bf.array().length + 8]);
										valb.position(0);
										valb.put(bf.array());
										valb.putLong(System.currentTimeMillis());
										db.put(m.owo, b, valb.array());
										// m.szct.incrementAndGet();
									}
									m.tempHt.remove(new ByteArrayWrapper(b));
								} catch (Exception e) {
									SDFSLogger.getLog().warn(
											"unable to commit " + StringUtils.getHexString(b) + " id=" + evt.getID(),
											e);

								}
							}
						}
					} else {
						SDFSLogger.getLog()
								.debug("could not find " + StringUtils.getHexString(b) + " id=" + evt.getID());
					}
				} finally {
					l.unlock();
				}
			}
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

	private static class CompactShard implements Runnable {

		private RocksDB dbs = null;

		private CompactShard(RocksDB dbs) {
			this.dbs = dbs;
		}

		@Override
		public void run() {
			try {
				this.dbs.compactRange();
				SDFSLogger.getLog().info("compaction done");
			} catch (RocksDBException e) {
				SDFSLogger.getLog().warn("unable to compact range", e);
			}

		}

	}

}