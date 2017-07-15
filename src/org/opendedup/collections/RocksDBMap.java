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
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.hashing.Tiger16HashEngine;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.StringUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;


public class RocksDBMap implements AbstractMap, AbstractHashesMap {
	WriteOptions wo = new WriteOptions();
	RocksDB db = null;
	String fileName = null;
	ReentrantLock [] lockMap = new ReentrantLock[256];
	boolean closed = false;
	private long size = 0;
	static  {
		RocksDB.loadLibrary();
	}

	@Override
	public void init(long maxSize, String fileName, double fpp) throws IOException, HashtableFullException {
		
		try {
			this.fileName = fileName;
			Options options = new Options();
			options.setCreateIfMissing(true);
			this.size = maxSize;
		      options.setCompactionStyle(CompactionStyle.UNIVERSAL);
		      options.setCompressionType(CompressionType.NO_COMPRESSION);
		      
		      BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
		      blockConfig.setFilter(new BloomFilter(16,false));
		      blockConfig.setBlockSize(4*1024);
		      blockConfig.setFormatVersion(2);
		      blockConfig.setBlockCacheSize(512*1024*1024);
		      //blockConfig.setCacheIndexAndFilterBlocks(true);
		      options.setTableFormatConfig(blockConfig);
		      options.setWriteBufferSize(512*1024*1024);
		      options.setAllowMmapWrites(true);
		      options.setAllowMmapReads(true);
		      options.setMaxOpenFiles(-1);
		      options.setTargetFileSizeBase(1024*1024*1024*4);
			    // a factory method that returns a RocksDB instance
		      wo = new WriteOptions();
		      wo.setDisableWAL(true);
		      wo.setSync(false);
			  db = RocksDB.open(options, fileName);
			  for(int i = 0; i < lockMap.length;i++) {
				  lockMap[i] = new ReentrantLock();
			  }
			  this.setUp();
		}catch(Exception e) {
			throw new IOException(e);
		}
			    
	}
	
	private ReentrantLock getLock(byte [] key) {
		int l = key[0];
		if (l < 0) {
			l = ((l * -1) + 127);
		}
		return lockMap[l];
	}

	

	

	@Override
	public boolean claimKey(byte[] hash, long val,long ct) throws IOException {
		Lock l = this.getLock(hash);
		l.lock();
		try {
			byte [] v = db.get(hash);
			if(v != null) {
				ByteBuffer bk = ByteBuffer.wrap(v);
				long oval = bk.getLong();
				if(oval != val) {
					SDFSLogger.getLog().warn("When updating reference count for key [" + StringUtils.getHexString(hash) + "] hash locations didn't match stored val=" + oval + " request value=" +val);
					return false;
				}
				ct += bk.getLong();
				bk.putLong(v.length - 8, ct);
				db.put(wo,hash, v);
				return true;
			}
				
			
			SDFSLogger.getLog().warn("When updating reference count. Key [" + StringUtils.getHexString(hash) + "] not found");	
			return false;
		} catch (RocksDBException e) {
			throw new IOException(e);
		}finally {
			l.unlock();
		}
	}

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	@Override
	public long getSize() {
		try {
			return db.getLongProperty("rocksdb.estimate-num-keys");
		} catch (RocksDBException e) {
			SDFSLogger.getLog().error("unable to get lenght for rocksdb",e);
			return 0;
		}
	}

	@Override
	public long getUsedSize() {

		try {
			return db.getLongProperty("rocksdb.estimate-num-keys")*Main.CHUNK_LENGTH;
		} catch (RocksDBException e) {
			SDFSLogger.getLog().error("unable to get lenght for rocksdb",e);
			return 0;
		}
	}

	@Override
	public long getMaxSize() {
		return this.size;
	}

	@Override
	public synchronized long claimRecords(SDFSEvent evt) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		long rmk = 0;
		try {
			RocksIterator iter = db.newIterator();
			ByteBuffer bk = ByteBuffer.allocateDirect(16);
			for(iter.seekToFirst();iter.isValid();iter.next()) {
				bk.position(0);
				bk.put(iter.value());
				bk.position(0);
				bk.getLong();
				long ct = bk.getLong();
				if(ct <= 0) {
					Lock l = this.getLock(iter.key());
					l.lock();
					try {
						bk.position(0);
						byte [] k = iter.key();
						byte [] v = db.get(k);
						if(v != null) {
							bk.put(v);
							bk.position(0);
							bk.position(0);
							long pos = bk.getLong();
							ct = bk.getLong();
							if(ct <= 0) {
								ChunkData ck = new ChunkData(pos, k);
								ck.setmDelete(true);
								db.delete(k);
							}
						}
					}catch(Exception e){
						
					}finally {
						l.unlock();
					}
				}
			}
			
		}catch(Exception e) {
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
		long size =db.getLongProperty("rocksdb.estimate-num-keys");
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
			byte [] v = db.get(key);
			if(v == null)
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

	// AtomicLong misses = new AtomicLong(0);
	// AtomicLong trs = new AtomicLong(0);
	// AtomicLong msTr = new AtomicLong(0);

	@Override
	public InsertRecord put(ChunkData cm, boolean persist) throws IOException, HashtableFullException {
		// persist = false;
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName + " is close");
		// if (persist)
		// this.flushFullBuffer();
		Lock l = this.getLock(cm.getHash());
		l.lock();
		try {
						
							try {
								StringBuilder sb = new StringBuilder();
								byte [] v = null;
								//if(db.keyMayExist(cm.getHash(), sb)) {
									//System.out.println("miss="+sb.toString());
								v = db.get(cm.getHash());
								//}
								if(v == null) {
									cm.persistData(true);
									v = new byte[16];
									ByteBuffer bf = ByteBuffer.wrap(v);
									bf.putLong(cm.getcPos());
									bf.putLong(1);
									db.put(wo,cm.getHash(), v);
									return new InsertRecord(true, cm.getcPos());
								} else {
									ByteBuffer bk = ByteBuffer.wrap(v);
									bk.getLong();
									long ct = bk.getLong();
									ct++;
									bk.putLong(8, ct);
									db.put(wo,cm.getHash(), v);
									return new InsertRecord(false, cm.getcPos());
								}
							} catch (org.opendedup.collections.HashExistsException e) {
								return put(cm, persist);
							} catch (RocksDBException e) {
								throw new IOException(e);
							} 
			

		} finally {

			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.opendedup.collections.AbstractHashesMap#update(org.opendedup.sdfs
	 * .filestore.ChunkData)
	 */
	@Override
	public boolean update(ChunkData cm) throws IOException {
		Lock l = this.getLock(cm.getHash());
		l.lock();
		try {
						
							try {
								
								byte [] v = db.get(cm.getHash());
								if(v == null) {
									
									return false;
								} else {
									ByteBuffer bk = ByteBuffer.wrap(v);
									bk.putLong(0, cm.getcPos());
									db.put(wo,cm.getHash(), v);
									return true;
								}
							}  catch (RocksDBException e) {
								throw new IOException(e);
							} 
			

		} finally {

			l.unlock();
		}
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
								
								byte [] v = db.get(key);
								if(v == null) {
									
									return -1;
								} else {
									ByteBuffer bk = ByteBuffer.wrap(v);
									return bk.getLong();
								}
							}  catch (RocksDBException e) {
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
								
								byte [] v = db.get(cm.getHash());
								if(v == null) {
									
									return false;
								} else {
									db.delete(cm.getHash());
									return true;
								}
							}  catch (RocksDBException e) {
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
				this.db.flush(new FlushOptions().setWaitForFlush(true));
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
			this.db.close();

			
		} finally {
			this.syncLock.unlock();
			SDFSLogger.getLog().info("Hashtable [" + this.fileName + "] closed");
		}
	}

	@Override
	public void vanish() throws IOException {
		throw new IOException("not supported");

	}

	public static void main(String[] args) throws Exception {
		RocksDBMap b = new RocksDBMap();
		b.init(1000000, "/opt/sdfs/hash", .001);
		long start = System.currentTimeMillis();
		Random rnd = new Random();
		byte[] hash = null;
		long val = -33;
		byte[] hash1 = null;
		long val1 = -33;
		Tiger16HashEngine eng = new Tiger16HashEngine();
		for (int i = 0; i < 60000; i++) {
			byte[] z = new byte[64];
			rnd.nextBytes(z);
			hash = eng.getHash(z);
			val = rnd.nextLong();
			if (i == 1) {
				val1 = val;
				hash1 = hash;
			}
			if (val < 0)
				val = val * -1;
			ChunkData cm = new ChunkData(hash, val);
			InsertRecord k = b.put(cm);
			if (k.getInserted())
				System.out.println("Unable to add this " + k);

		}
		long end = System.currentTimeMillis();
		System.out.println("Took " + (end - start) / 1000 + " s " + val1);
		System.out.println("Took " + (System.currentTimeMillis() - end) / 1000 + " ms at pos " + b.get(hash1));
		b.claimRecords(SDFSEvent.gcInfoEvent("testing 123"));
		b.close();

	}

	@Override
	public void initCompact() throws IOException {
		

	}

	@Override
	public void commitCompact(boolean force) throws IOException {
		try {
			this.db.compactRange();
		} catch (RocksDBException e) {
			throw new IOException(e);
		}

	}

	@Override
	public void rollbackCompact() throws IOException {
		FileUtils.deleteDirectory(new File(this.fileName));

	}

	protected final static class ProcessPriorityThreadFactory implements ThreadFactory {

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
		Lock l = this.getLock(key);
		l.lock();
		try {
			StringBuilder retValue = new StringBuilder();
				return this.db.keyMayExist(key, retValue);
				
		} finally {
			l.unlock();
		}
	}

}