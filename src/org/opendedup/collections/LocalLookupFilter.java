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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.StringUtils;
import org.rocksdb.AccessHint;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class LocalLookupFilter {
	WriteOptions wo = new WriteOptions();
	// RocksDB db = null;
	String fileName = null;
	ReentrantLock[] lockMap = new ReentrantLock[256];
	RocksDB dbs = null;
	private static final long GB = 1024 * 1024 * 1024;
	private static final long MB = 1024 * 1024;
	boolean closed = false;
	private static final LoadingCache<String, LocalLookupFilter> lfs = CacheBuilder.newBuilder()
			.maximumSize(Main.maxOpenFiles).concurrencyLevel(72).expireAfterAccess(120, TimeUnit.MINUTES)
			.removalListener(new RemovalListener<String, LocalLookupFilter>() {

				@Override
				public void onRemoval(RemovalNotification<String, LocalLookupFilter> ev) {
					try {
						ev.getValue().close();
					} catch (Exception e) {
						SDFSLogger.getLog().warn("unable to close " + ev.getKey(), e);
					}

				}

			}).build(new CacheLoader<String, LocalLookupFilter>() {

				@Override
				public LocalLookupFilter load(String name) throws Exception {
					LocalLookupFilter lf = new LocalLookupFilter();
					lf.init(name);
					return lf;
				}
			});
	
	public static LocalLookupFilter getLocalLookupFilter(String filter) throws IOException {
		try {
		return lfs.get(filter);
		}catch(Exception e) {
			throw new IOException(e);
		}
	}
	
	public static void closeAll() {
		lfs.invalidateAll();
	}
	static {
		RocksDB.loadLibrary();
		if (Main.lookupfilterStore == null) {
			File lookupP = new File(new File(Main.dedupDBStore).getParent() + File.separator + "lookupfilters");
			lookupP.mkdirs();
			Main.lookupfilterStore = lookupP.getPath();
		}
	}

	private void init(String lookupfilter) throws IOException, HashtableFullException {

		try {
			this.fileName = Main.lookupfilterStore + File.separator + lookupfilter;
			long bufferSize = 80 * MB;
			long fsize = 80 * MB;
			// blockConfig.setBlockCacheSize(memperDB);
			// blockConfig.setCacheIndexAndFilterBlocks(true);

			// a factory method that returns a RocksDB instance
			wo = new WriteOptions();
			wo.setDisableWAL(false);
			wo.setSync(false);
			// LRUCache c = new LRUCache(totmem);

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
			Options options = new Options();
			options.setCreateIfMissing(true);
			options.setCompactionStyle(CompactionStyle.LEVEL);
			options.setCompressionType(CompressionType.NO_COMPRESSION);
			options.setLevel0FileNumCompactionTrigger(8);
			options.setMaxBackgroundCompactions(2);
			options.setMaxBackgroundFlushes(8);
			options.setEnv(env);
			options.setAccessHintOnCompactionStart(AccessHint.WILLNEED);
			options.setIncreaseParallelism(32);
			options.setAdviseRandomOnOpen(true);
			// options.setNumLevels(8);
			// options.setLevelCompactionDynamicLevelBytes(true);
			//
			options.setAllowConcurrentMemtableWrite(true);
			// LRUCache c = new LRUCache(memperDB);
			// options.setRowCache(c);
			blockConfig.setBlockCacheSize(GB * 2);

			options.setWriteBufferSize(bufferSize);
			// options.setMinWriteBufferNumberToMerge(2);
			// options.setMaxWriteBufferNumber(6);
			// options.setLevelZeroFileNumCompactionTrigger(2);

			options.setCompactionReadaheadSize(1024 * 1024 * 25);
			// options.setUseDirectIoForFlushAndCompaction(true);
			// options.setUseDirectReads(true);
			options.setStatsDumpPeriodSec(30);
			// options.setAllowMmapWrites(true);
			// options.setAllowMmapReads(true);
			options.setMaxOpenFiles(-1);
			// options.setTargetFileSizeBase(512*1024*1024);

			options.setMaxBytesForLevelBase(fsize * 5);
			options.setTargetFileSizeBase(fsize);
			options.setTableFormatConfig(blockConfig);
			File f = new File(fileName + File.separator);
			f.mkdirs();
			dbs = RocksDB.open(options, f.getPath());

			for (int i = 0; i < lockMap.length; i++) {
				lockMap[i] = new ReentrantLock();
			}
			this.setUp();
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
		return this.dbs;
	}

	public long claimKey(byte[] hash, long val, long ct) throws IOException {
		Lock l = this.getLock(hash);
		l.lock();
		try {
			byte[] v = null;

			v = this.getDB(hash).get(hash);
			if (v != null) {
				ByteBuffer bk = ByteBuffer.wrap(v);
				long oval = bk.getLong();
				if (oval != val) {
					SDFSLogger.getLog().warn("When updating reference count for key [" + StringUtils.getHexString(hash)
							+ "] hash locations didn't match stored val=" + oval + " request value=" + val);
					return HCServiceProxy.hcService.claimKey(hash, val, ct);
				}
				ct += bk.getLong();
				if (ct <= 0) {
					if (ct == 0)
						ct = -1;
					this.getDB(hash).delete(wo,hash);
					return HCServiceProxy.hcService.claimKey(hash, val, ct);
				} else {
					bk.putLong(v.length - 8, ct);
					this.getDB(hash).put(wo, hash, v);
					return val;
				}
			} else {
				return val;
			}
		} catch (RocksDBException e) {
			throw new IOException(e);
		} finally {
			l.unlock();
		}
	}

	public boolean isClosed() {
		return this.closed;
	}

	public long getSize() {
		try {
			long sz = 0;
			sz = dbs.getLongProperty("rocksdb.estimate-num-keys");
			return sz;
		} catch (RocksDBException e) {
			SDFSLogger.getLog().error("unable to get lenght for rocksdb", e);
			return 0;
		}
	}

	public long setUp() throws Exception {
		long sz = 0;
		// System.out.println("s="+i);
		sz += dbs.getLongProperty("rocksdb.estimate-num-keys");
		long size = sz;
		this.closed = false;
		return size;
	}

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

	public InsertRecord put(byte [] key,byte [] contents,long ct,String uuid) throws IOException, HashtableFullException {
		// persist = false;
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName + " is close");
		// if (persist)
		// this.flushFullBuffer();
		Lock l = this.getLock(key);
		l.lock();
		try {
			try {
				RocksDB db = this.getDB(key);
				byte[] v = null;

				v = db.get(key);
				if (v == null) {
					InsertRecord ir = HCServiceProxy.hcService.writeChunk(key, contents, false, 1,uuid);
					v = new byte[16];
					ByteBuffer bf = ByteBuffer.wrap(v);
					bf.put(ir.getHashLocs());
					if (ct <= 0)
						bf.putLong(1);
					else
						bf.putLong(ct);
					db.put(key, v);
					return ir;
				} else {
					// SDFSLogger.getLog().info("Hash Found");
					ByteBuffer bk = ByteBuffer.wrap(v);
					long pos = bk.getLong();
					
					if (ct <= 0)
						ct =  bk.getLong() +1;
					else
						ct += bk.getLong();
					bk.putLong(8, ct);
					db.put(key, v);
					return new InsertRecord(false, pos);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}

		} finally {

			l.unlock();
		}
	}
	
	public long put(byte [] key,long ct) throws IOException, HashtableFullException {
		// persist = false;
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName + " is close");
		// if (persist)
		// this.flushFullBuffer();
		Lock l = this.getLock(key);
		l.lock();
		try {
			try {
				RocksDB db = this.getDB(key);
				byte[] v = null;

				v = db.get(key);
				if (v == null) {
					return -1;
				} else {
					// SDFSLogger.getLog().info("Hash Found");
					ByteBuffer bk = ByteBuffer.wrap(v);
					long pos = bk.getLong();
					
					if (ct <= 0)
						ct =  bk.getLong() +1;
					else
						ct += bk.getLong();;
					bk.putLong(8, ct);
					db.put(key, v);
					return pos;
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}

		} finally {

			l.unlock();
		}
	}
	
	public void put(byte [] key,long pos, long ct) throws IOException, HashtableFullException {
		// persist = false;
				if (this.isClosed())
					throw new HashtableFullException("Hashtable " + this.fileName + " is close");
				// if (persist)
				// this.flushFullBuffer();
				Lock l = this.getLock(key);
				l.lock();
				try {
					try {
						RocksDB db = this.getDB(key);
						byte[] v = null;

						v = db.get(key);
						if (v == null) {
							
							v = new byte[16];
							ByteBuffer bf = ByteBuffer.wrap(v);
							bf.putLong(pos);
							if (ct <= 0)
								bf.putLong(1);
							else
								bf.putLong(ct);
							db.put(key, v);
						} else {
							// SDFSLogger.getLog().info("Hash Found");
							ByteBuffer bk = ByteBuffer.wrap(v);
							long npos = bk.getLong();
							if(npos != pos)
								throw new IOException("npos " + npos + " != pos" + pos);
							
							ct += bk.getLong();
							if (ct <= 0)
								ct=bk.getLong() +1;
							else
								ct += bk.getLong();
							bk.putLong(8, ct);
							db.put(key, v);
						}
					} catch (RocksDBException e) {
						throw new IOException(e);
					}

				} finally {

					l.unlock();
				}
	}
	
	

	public long get(byte[] key) throws IOException {
		Lock l = this.getLock(key);
		l.lock();
		try {

			try {

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

	public void close() {
		try {
			this.closed = true;

			dbs.close();

		} finally {
			SDFSLogger.getLog().info("Hashtable [" + this.fileName + "] closed");
		}
	}

}