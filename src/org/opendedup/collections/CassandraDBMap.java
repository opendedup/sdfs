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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
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
import org.opendedup.ignite.GCRunner;
import org.opendedup.ignite.RMDBPersistence;
import org.opendedup.ignite.RocksDBPersistence;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.StringUtils;

public class CassandraDBMap implements AbstractMap, AbstractHashesMap,Serializable  {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5135883328710571241L;
	public static transient Ignite ig = null;

	private transient boolean closed = false;
	private transient long size = 0;
	
	
	

	@Override
	public void init(long maxSize, String fileName, double fpp) throws IOException, HashtableFullException {

		try {
			this.size = maxSize;
			IgniteConfiguration cfg = new IgniteConfiguration();
			cfg.getAtomicConfiguration().setCacheMode(CacheMode.PARTITIONED);
			cfg.getAtomicConfiguration().setBackups(Main.volume.getClusterCopies());
			ig = Ignition.start(cfg);
			
		} catch (Exception e) {
			throw new IOException(e);
		}
	}




	@Override
	public long endStartingPosition() {
		// TODO Auto-generated method stub
		return 0;
	}




	@Override
	public long getSize() {
		// TODO Auto-generated method stub
		return 0;
	}




	@Override
	public long getUsedSize() {
		// TODO Auto-generated method stub
		return 0;
	}




	@Override
	public long getMaxSize() {
		// TODO Auto-generated method stub
		return 0;
	}




	@Override
	public boolean mightContainKey(byte[] key) {
		// TODO Auto-generated method stub
		return false;
	}




	@Override
	public long claimRecords(SDFSEvent evt) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}




	@Override
	public long claimRecords(SDFSEvent evt, LargeBloomFilter bf) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}




	@Override
	public boolean containsKey(byte[] key) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}




	@Override
	public InsertRecord put(ChunkData cm) throws IOException, HashtableFullException {
		// TODO Auto-generated method stub
		return null;
	}




	@Override
	public InsertRecord put(ChunkData cm, boolean persist) throws IOException, HashtableFullException {
		// TODO Auto-generated method stub
		return null;
	}




	@Override
	public boolean update(ChunkData cm) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}




	@Override
	public void cache(long pos) throws IOException, DataArchivedException {
		// TODO Auto-generated method stub
		
	}




	@Override
	public long get(byte[] key) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}




	@Override
	public byte[] getData(byte[] key) throws IOException, DataArchivedException {
		// TODO Auto-generated method stub
		return null;
	}




	@Override
	public byte[] getData(byte[] key, long pos) throws IOException, DataArchivedException {
		// TODO Auto-generated method stub
		return null;
	}




	@Override
	public boolean remove(ChunkData cm) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}




	@Override
	public boolean isClaimed(ChunkData cm) throws KeyNotFoundException, IOException {
		// TODO Auto-generated method stub
		return false;
	}




	@Override
	public void initCompact() throws IOException {
		// TODO Auto-generated method stub
		
	}




	@Override
	public void commitCompact(boolean force) throws IOException {
		// TODO Auto-generated method stub
		
	}




	@Override
	public void rollbackCompact() throws IOException {
		// TODO Auto-generated method stub
		
	}




	@Override
	public void clearRefMap() throws IOException {
		// TODO Auto-generated method stub
		
	}




	@Override
	public boolean claimKey(byte[] hash, long val, long ct) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}




	@Override
	public boolean isClosed() {
		// TODO Auto-generated method stub
		return false;
	}




	@Override
	public void sync() throws IOException {
		// TODO Auto-generated method stub
		
	}




	@Override
	public void vanish() throws IOException {
		// TODO Auto-generated method stub
		
	}




	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	
	
	
}