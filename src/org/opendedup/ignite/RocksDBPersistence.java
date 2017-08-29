package org.opendedup.ignite;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;

import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.opendedup.collections.ByteArrayWrapper;
import org.opendedup.collections.RocksDBMap.ProcessPriorityThreadFactory;
import org.opendedup.collections.RocksDBMap.StartShard;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.util.CommandLineProgressBar;
import org.rocksdb.AccessHint;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

public class RocksDBPersistence extends CacheStoreAdapter<ByteArrayWrapper, ByteArrayWrapper> {
	
	WriteOptions wo = new WriteOptions();
	// RocksDB db = null;
	String fileName = null;
	ReentrantLock[] lockMap = new ReentrantLock[256];
	RocksDB[] dbs = new RocksDB[8];
	RocksDB rmdb = null;
	private static final long GB = 1024*1024*1024;
	private static final long MB = 1024*1024;
	int multiplier = 0;
	boolean closed = false;
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(2);
	private transient ThreadPoolExecutor executor = null;
	static {
		RocksDB.loadLibrary();
	}
	
	public RocksDBPersistence() {
		super();
		File directory = new File(Main.hashDBStore + File.separator);
		if (!directory.exists())
			directory.mkdirs();
		this.fileName = directory.getPath();
		
		long size = ((Main.chunkStoreAllocationSize / Main.chunkStorePageSize)) + 8000;
		if (HashFunctionPool.max_hash_cluster > 1) {
			size = (Main.chunkStoreAllocationSize / HashFunctionPool.avg_page_size) + 8000;
		}
		try {
			multiplier = 256 / dbs.length;
			System.out.println("multiplier=" + this.multiplier + " size=" + dbs.length);
			long bufferSize =GB;
			long fsize = 128*MB;
			if (size < 1_000_000_000L) {
				bufferSize = 512*MB*dbs.length;
			}
			else if (size < 10_000_000_000L) {
				fsize = 128*MB;
				bufferSize = fsize*10*dbs.length;
			} else if (size < 50_000_000_000L) {
				fsize = 256*MB;
				bufferSize = GB*dbs.length;
			} else {
				//long mp = this.size / 10_000_000_000L;
				
				fsize = GB;
				bufferSize = 1*GB*dbs.length;
			} 

			// blockConfig.setChecksumType(ChecksumType.kNoChecksum);
			long totmem = size;

			long memperDB = totmem / dbs.length;
			System.out.println("mem=" + totmem + " memperDB=" + memperDB + " bufferSize=" + bufferSize
					+ " bufferSizePerDB=" + (bufferSize / dbs.length));
			// blockConfig.setBlockCacheSize(memperDB);
			// blockConfig.setCacheIndexAndFilterBlocks(true);

			// a factory method that returns a RocksDB instance
			wo = new WriteOptions();
			wo.setDisableWAL(false);
			wo.setSync(false);
			// LRUCache c = new LRUCache(totmem);
			CommandLineProgressBar bar = new CommandLineProgressBar("Loading Existing Hash Tables", dbs.length + 1,
					System.out);
			executor = new ThreadPoolExecutor(Main.writeThreads + 1, Main.writeThreads + 1, 10, TimeUnit.SECONDS,
					worksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
			AtomicInteger ct = new AtomicInteger();
			ArrayList<StartShard> shs = new ArrayList<StartShard>();
			for (int i = 0; i < dbs.length; i++) {
				
				BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
				//ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
				//DBOptions dbo = new DBOptions();
				//cfOptions.optimizeLevelStyleCompaction();
				//cfOptions.optimizeForPointLookup(8192);
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
				options.setAccessHintOnCompactionStart(AccessHint.WILLNEED);
				options.setIncreaseParallelism(32);
				options.setAdviseRandomOnOpen(true);
				// options.setNumLevels(8);
				// options.setLevelCompactionDynamicLevelBytes(true);
				//
				options.setAllowConcurrentMemtableWrite(true);
				//LRUCache c = new LRUCache(memperDB);
				//options.setRowCache(c);
				blockConfig.setBlockCacheSize(GB*2);		
				
				options.setWriteBufferSize(bufferSize/dbs.length);
				//options.setMinWriteBufferNumberToMerge(2);
				//options.setMaxWriteBufferNumber(6);
				//options.setLevelZeroFileNumCompactionTrigger(2);

				options.setCompactionReadaheadSize(1024*1024*25);
				// options.setUseDirectIoForFlushAndCompaction(true);
				// options.setUseDirectReads(true);
				options.setStatsDumpPeriodSec(30);
				// options.setAllowMmapWrites(true);
				//options.setAllowMmapReads(true);
				options.setMaxOpenFiles(-1);
				options.createStatistics();
				//options.setTargetFileSizeBase(512*1024*1024);
				
				options.setMaxBytesForLevelBase(fsize*5);
				options.setTargetFileSizeBase(fsize);
				options.setTableFormatConfig(blockConfig);
				File f = new File(fileName + File.separator + i);
				f.mkdirs();
				StartShard sh = new StartShard(i,this.dbs, options,f,bar,ct);
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
			for(StartShard sh : shs) {
				if(sh.e != null)
					throw sh.e;
			}

			for (int i = 0; i < lockMap.length; i++) {
				lockMap[i] = new ReentrantLock();
			}
			

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
			bar.finish();
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to initiate datastore",e);
			System.exit(1);
		}
	}

	@Override
	public ByteArrayWrapper load(ByteArrayWrapper key) throws CacheLoaderException {
		byte [] b  =
		return null;
	}

	@Override
	public void delete(Object key) throws CacheWriterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(Entry<? extends ByteArrayWrapper, ? extends ByteArrayWrapper> arg0) throws CacheWriterException {
		// TODO Auto-generated method stub
		
	}

}
