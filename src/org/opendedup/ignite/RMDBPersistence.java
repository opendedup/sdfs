package org.opendedup.ignite;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;

import org.apache.ignite.lang.IgniteBiInClosure;
import org.opendedup.collections.ByteArrayWrapper;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

public class RMDBPersistence implements org.apache.ignite.cache.store.CacheStore<ByteArrayWrapper, ByteArrayWrapper> {

	WriteOptions wo = new WriteOptions();
	// RocksDB db = null;
	String fileName = null;
	ReentrantLock[] lockMap = new ReentrantLock[256];
	RocksDB rmdb = null;
	private static final long MB = 1024 * 1024;
	int multiplier = 0;
	boolean closed = false;
	static {
		RocksDB.loadLibrary();
	}

	public RMDBPersistence() {
		super();
		File directory = new File(Main.hashDBStore + File.separator + "rmdb" + File.separator);
		if (!directory.exists())
			directory.mkdirs();
		this.fileName = directory.getPath();
		try {
			
			// blockConfig.setBlockCacheSize(memperDB);
			// blockConfig.setCacheIndexAndFilterBlocks(true);

			// a factory method that returns a RocksDB instance
			wo = new WriteOptions();
			wo.setDisableWAL(false);
			wo.setSync(false);
			// LRUCache c = new LRUCache(totmem);
			
			
			

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
			// LRUCache c = new LRUCache(memperDB);
			// options.setRowCache(c);

			// blockConfig.setBlockCacheSize(memperDB);
			blockConfig.setNoBlockCache(true);
			options.setWriteBufferSize(512*MB);
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
			// options.setTargetFileSizeBase(512*1024*1024);
			options.setMaxBytesForLevelBase(512*MB);
			options.setTargetFileSizeBase(128 * 1024 * 1024);
			options.setTableFormatConfig(blockConfig);
			File f = new File(fileName + File.separator + "rmdb");
			f.mkdirs();
			rmdb = RocksDB.open(options, f.getPath());
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to initiate datastore", e);
			System.exit(1);
		}
	}
	@Override public void loadCache(IgniteBiInClosure<ByteArrayWrapper, ByteArrayWrapper> clo, Object... args) {
		SDFSLogger.getLog().info("loading cache");
	}

	private RocksDB getDB(byte[] key) {
		return rmdb;
	}

	@Override
	public ByteArrayWrapper load(ByteArrayWrapper key) throws CacheLoaderException {
		try {
			byte[] b = this.getDB(key.getData()).get(key.getData());
			if (b != null)
				return new ByteArrayWrapper(b);
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to load hash", e);
			throw new CacheLoaderException(e);
		}

		return null;
	}

	@Override
	public void delete(Object key) throws CacheWriterException {
		try {
			ByteArrayWrapper bw = (ByteArrayWrapper) key;
			this.getDB(bw.getData()).delete(bw.getData());
		} catch (RocksDBException e) {
			throw new CacheWriterException(e);
		}

	}

	@Override
	public void write(Entry<? extends ByteArrayWrapper, ? extends ByteArrayWrapper> entry) throws CacheWriterException {
		try {
			ByteArrayWrapper key = entry.getKey();
			ByteArrayWrapper value = entry.getValue();
			this.getDB(key.getData()).put(key.getData(), value.getData());
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to persist hash", e);
			throw new CacheWriterException(e);
		}
	}
	@Override
	public Map<ByteArrayWrapper, ByteArrayWrapper> loadAll(Iterable<? extends ByteArrayWrapper> arg0)
			throws CacheLoaderException {
		return null;
	}
	@Override
	public void deleteAll(Collection<?> arg0) throws CacheWriterException {
		
	}
	@Override
	public void writeAll(Collection<Entry<? extends ByteArrayWrapper, ? extends ByteArrayWrapper>> arg0)
			throws CacheWriterException {
		
	}
	@Override
	public void sessionEnd(boolean arg0) throws CacheWriterException {
		
	}

}
