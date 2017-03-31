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
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.collections.SimpleByteArrayLongMap.KeyValuePair;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.util.NextPrime;

public class SimpleMemoryByteArrayLongMap implements SimpleMapInterface{
	// MappedByteBuffer keys = null;
	private static int MAGIC_NUMBER = 6442;
	private int version = -1;
	private int size = 0;
	private int offset = 16;
	private String path = null;
	//private FileChannel kFC = null;
	//RandomAccessFile rf = null;
	private ReentrantReadWriteLock hashlock = new ReentrantReadWriteLock();
	ByteBuffer buf =null;
	public static final byte[] FREE = new byte[HashFunctionPool.hashLength];
	transient protected int EL = HashFunctionPool.hashLength + 8;
	transient private static final int VP = HashFunctionPool.hashLength;
	BitSet mapped = null;
	private int iterPos = 0;
	private int currentSz = 0;

	static {
		Arrays.fill(FREE, (byte) 0);
	}

	public SimpleMemoryByteArrayLongMap(String path, int sz,int ver)
			throws IOException {
		this.size = NextPrime.getNextPrimeI(sz);
		this.path = path;
		this.version = ver;
		this.setUp();
	}

	public int getVersion() {
		return this.version;
	}

	public String getPath() {
		return this.path;
	}

	private ReentrantLock iterlock = new ReentrantLock();

	public void iterInit() {
		this.iterlock.lock();
		this.iterPos = 0;
		this.iterlock.unlock();
	}

	public int getCurrentSize() {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			return this.currentSz;
		} finally {
			l.unlock();
		}
	}

	public KeyValuePair next() throws IOException, MapClosedException {
		while ((iterPos+offset) < this.buf.capacity()) {
			Lock l = this.hashlock.writeLock();
			l.lock();
			try {
				if (this.closed)
					throw new MapClosedException();
				if ((iterPos+offset) < this.buf.capacity()) {
					byte[] key = new byte[FREE.length];
					buf.position(iterPos + offset);
					buf.get(key);
					iterPos = iterPos + EL;
					if (!Arrays.equals(key, FREE)) {
						if (this.version == 0)
							return new KeyValuePair(key, buf.getInt());
						else
							return new KeyValuePair(key, buf.getLong());
					}
				} else {
					iterPos = iterPos + EL;
				}
			} finally {
				l.unlock();
			}

		}
		return null;
	}

	public int getMaxSz() {
		return this.size;
	}

	/**
	 * initializes the Object set of this hash table.
	 * 
	 * @param initialCapacity
	 *            an <code>int</code> value
	 * @return an <code>int</code> value
	 * @throws IOException
	 */
	public void setUp() throws IOException {
		mapped = new BitSet(size);
		if (version != 0) {
		
		EL = HashFunctionPool.hashLength + 8;
		this.offset = 16;
		
		buf = ByteBuffer.allocateDirect((EL * size)+offset);
		buf.putInt(MAGIC_NUMBER);
		buf.putInt(this.version);
		}else {
			EL = HashFunctionPool.hashLength + 4;
			this.version = 0;
			this.offset = 0;
			buf = ByteBuffer.allocateDirect((EL * size)+offset);
		}
		
		this.closed = false;
	}

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 */
	public boolean containsKey(byte[] key) throws MapClosedException {
		Lock l = this.hashlock.readLock();
		l.lock();
		try {

			if (this.closed)
				throw new MapClosedException();
			int index = index(key);
			if (index >= 0) {
				return true;
			}
			return false;
		} catch (MapClosedException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		} finally {
			l.unlock();
		}
	}

	private int hashFunc1(int hash) {
		return hash % size;
	}

	public int hashFunc3(int hash) {
		int result = hash + 1;
		return result;
	}

	/**
	 * Locates the index of <tt>obj</tt>.
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return the index of <tt>obj</tt> or -1 if it isn't in the set.
	 * @throws IOException
	 */
	protected int index(byte[] key) throws IOException {

		// From here on we know obj to be non-null
		ByteBuffer _buf = ByteBuffer.wrap(key);
		_buf.position(8);
		int hash = _buf.getInt() & 0x7fffffff;
		int hi = this.hashFunc1(hash);
		int index = hi * EL;
		byte[] cur = new byte[FREE.length];
		if (this.mapped == null || this.mapped.get(hi)) {
			buf.position(index + offset);
			buf.get(cur);
			if (Arrays.equals(cur, key)) {
				return index;
			}

			if (Arrays.equals(cur, FREE)) {
				return -1;
			}
		}
		return indexRehashed(key, index, hash, cur);
	}

	/**
	 * Locates the index of non-null <tt>obj</tt>.
	 * 
	 * @param obj
	 *            target key, know to be non-null
	 * @param index
	 *            we start from
	 * @param hash
	 * @param cur
	 * @return
	 * @throws IOException
	 */
	private int indexRehashed(byte[] key, int index, int hash, byte[] cur)
			throws IOException {

		// NOTE: here it has to be REMOVED or FULL (some user-given value)
		// see Knuth, p. 529
		int length = size * EL;
		int probe = (1 + (hash % (size - 2))) * EL;

		final int loopIndex = index;

		do {
			index -= probe;
			if (index < 0) {
				index += length;
			}
			if (mapped == null || mapped.get(index / EL)) {
				buf.position(index + offset);
				buf.get(cur);
				if (Arrays.equals(cur, key))
					return index;
				//
				if (Arrays.equals(cur, FREE)) {
					return -1;
				}
			}
			//

		} while (index != loopIndex);

		return -1;
	}

	boolean closed = false;

	public void vanish() {
		//SDFSLogger.getLog().info("removed" + this.path);
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			this.close();
		} catch (Exception e) {

		}
		try {
			buf = null;
		} catch (Exception e) {
		}
		l.unlock();
	}
	
	public void toFile(String np) throws IOException {
		FileChannel fc = FileChannel.open(new File(np).toPath(), StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.READ);
		buf.position(0);
		fc.write(buf, 0);
		fc.force(false);
		fc.close();
		
		//SDFSLogger.getLog().info("wrote " +  k + " to " + np);
	}

	protected int insertionIndex(byte[] key) throws IOException {
		ByteBuffer _buf = ByteBuffer.wrap(key);
		_buf.position(8);
		int hash = _buf.getInt() & 0x7fffffff;
		int hi = this.hashFunc1(hash);
		int index = hi * EL;
		byte[] cur = new byte[FREE.length];
		if (this.mapped == null || this.mapped.get(hi)) {
			buf.position(index + offset);
			buf.get(cur);

			if (Arrays.equals(cur, FREE)) {
				return index; // empty, all done
			} else if (Arrays.equals(cur, key)) {
				return -index - 1; // already stored
			}
		} else if (this.mapped != null && !this.mapped.get(hi))
			return index;
		return insertKeyRehash(key, index, hash, cur);
	}

	/**
	 * Looks for a slot using double hashing for a non-null key values and
	 * inserts the value in the slot
	 * 
	 * @param key
	 *            non-null key value
	 * @param index
	 *            natural index
	 * @param hash
	 * @param cur
	 *            value of first matched slot
	 * @return
	 * @throws IOException
	 */
	private int insertKeyRehash(byte[] key, int index, int hash, byte[] cur)
			throws IOException {
		final int length = size * (EL);
		final int probe = (1 + (hash % (size - 2))) * EL;

		final int loopIndex = index;

		/**
		 * Look until FREE slot or we start to loop
		 */
		do {
			// Identify first removed slot

			index -= probe;
			if (index < 0) {
				index += length;
			}
			if (mapped == null || mapped.get(index / EL)) {
				buf.position(index + offset);
				buf.get(cur);

				// A FREE slot stops the search
				if (Arrays.equals(cur, FREE)) {

					return index;
				}

				if (Arrays.equals(cur, key)) {
					return -index - 1;
				}
			} else if (this.mapped != null && !this.mapped.get(index / EL))
				return index;

			// Detect loop
		} while (index != loopIndex);

		// We inspected all reachable slots and did not find a FREE one
		// If we found a REMOVED slot we return the first one found

		// Can a resizing strategy be found that resizes the set?
		throw new IllegalStateException(
				"No free or removed slots available. Key set full?!!");
	}

	public boolean put(byte[] key, long value) throws MapClosedException {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			if (this.closed)
				throw new MapClosedException();
			int pos = this.insertionIndex(key);
			if (pos < 0) {
				int npos = -pos - 1;
				npos = (npos / EL);
				return false;
			}
			buf.position(pos + offset);
			buf.put(key);
			if(this.version ==0)
				buf.putInt((int) value);
			else
				buf.putLong(value);
			
			pos = (pos / EL);
			this.mapped.set(pos);
			this.currentSz++;
			return pos > -1 ? true : false;
		} catch (MapClosedException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error inserting record", e);
			e.printStackTrace();
			return false;
		} finally {
			l.unlock();
		}
	}

	public long get(byte[] key) throws MapClosedException {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {

			if (this.closed)
				throw new MapClosedException();
			if (key == null)
				return -1;
			int pos = this.index(key);
			if (pos == -1) {
				return -1;
			} else {
				this.buf.position(pos + offset + VP);
				long val = -1;
				if (this.version == 0)
					val = this.buf.getInt();
				else
					val = this.buf.getLong();
				//SDFSLogger.getLog().info("read at " + pos + " fp " + (pos + offset) + " val " +val);
				return val;

			}
		} catch (MapClosedException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return -1;
		} finally {
			l.unlock();
		}

	}

	public void close() {
		Lock l = this.hashlock.writeLock();
		l.lock();
		this.closed = true;
		this.buf = null;
		this.mapped = null;
		l.unlock();
		SDFSLogger.getLog().debug("closed " + this.path);
	}

	public void sync() throws SyncFailedException, IOException {
		//this.kFC.force(false);

	}
}
