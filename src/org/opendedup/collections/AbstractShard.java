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

import java.io.IOException;

import java.io.SyncFailedException;

import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.sdfs.filestore.ChunkData;

public interface AbstractShard {

	public abstract void iterInit();

	public abstract byte[] nextKey() throws IOException;


	/**
	 * initializes the Object set of this hash table.
	 * 
	 * @param initialCapacity
	 *            an <code>int</code> value
	 * @return an <code>int</code> value
	 * @throws IOException
	 */
	public abstract long setUp() throws IOException;

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 * @throws MapClosedException 
	 */
	public abstract boolean containsKey(byte[] key) throws MapClosedException ;

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 * @throws KeyNotFoundException
	 */
	public abstract boolean isClaimed(byte[] key) throws KeyNotFoundException,
			IOException;

	public abstract boolean update(byte[] key, long value) throws IOException;

	public abstract boolean remove(byte[] key) throws IOException;

	public abstract InsertRecord put(ChunkData cm)
			throws HashtableFullException, IOException, MapClosedException;

	public abstract InsertRecord put(byte[] key, long val)
			throws HashtableFullException, IOException, MapClosedException;

	public abstract int getEntries();

	public abstract long get(byte[] key) throws MapClosedException;

	public abstract long get(byte[] key, boolean claim) throws MapClosedException ;

	public abstract int size();

	public abstract void close();

	public abstract long claimRecords() throws IOException;

	public abstract long claimRecords(LargeBloomFilter bf) throws IOException;

	public abstract void sync() throws SyncFailedException, IOException;

	void clearRefMap() throws IOException;

	KVPair nextKeyValue() throws IOException;

	long getLastModified();

	void vanish();

	long claimRecords(LargeBloomFilter nbf, LargeBloomFilter lbf) throws IOException;

	boolean isClosed();

	int maxSize();

	int avail();

	InsertRecord put(byte[] key, long value, long claims) throws HashtableFullException, IOException;

	boolean isMaxed();

	boolean isFull();

	void cache();

	void activate();

	boolean isActive();

	void inActive();

	boolean isCompactig();

	void compactRunning(boolean running);

	long getLastAccess();

	boolean claim(byte[] key, long val, long ct) throws MapClosedException;

}