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
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.util.NextPrime;
import org.opendedup.util.StringUtils;

public class SimpleByteArrayLongMap {
	// MappedByteBuffer keys = null;
	private static int MAGIC_NUMBER = 6442;
	private int version = -1;
	private int size = 0;
	private int offset = 16;
	private String path = null;
	private FileChannel kFC = null;
	RandomAccessFile rf = null;
	private ReentrantReadWriteLock hashlock = new ReentrantReadWriteLock();
	public static final byte[] FREE = new byte[HashFunctionPool.hashLength];
	transient protected int EL = HashFunctionPool.hashLength + 4;
	transient private static final int VP = HashFunctionPool.hashLength;
	BitSet mapped = null;
	private int iterPos = 0;
	private int currentSz = 0;

	static {
		Arrays.fill(FREE, (byte) 0);
	}

	public SimpleByteArrayLongMap(String path, int sz,int ver)
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
		while (iterPos < this.kFC.size()) {
			Lock l = this.hashlock.writeLock();
			l.lock();
			try {
				if (this.closed)
					throw new MapClosedException();
				if (iterPos < this.kFC.size()) {
					byte[] key = new byte[FREE.length];
					this.vb.position(0);
					kFC.read(vb, iterPos + offset);
					vb.position(0);
					iterPos = iterPos + EL;
					vb.get(key);
					if (!Arrays.equals(key, FREE)) {
						if (this.version == 0)
							return new KeyValuePair(key, vb.getInt());
						else
							return new KeyValuePair(key, vb.getLong());
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
		boolean nf = false;
		if (!new File(path).exists()) {
			nf = true;
			mapped = new BitSet(size);
		}
		rf = new RandomAccessFile(path, "rw");
		this.kFC = rf.getChannel();
		if (version != 0) {
			if (nf) {
				ByteBuffer nbf = ByteBuffer.allocate(16);
				nbf.putInt(MAGIC_NUMBER);
				nbf.putInt(this.version);
				nbf.position(0);
				this.kFC.write(nbf);
				EL = HashFunctionPool.hashLength + 8;
				this.offset = 16;
				rf.setLength((EL * size)+offset);
			} else {
				ByteBuffer nbf = ByteBuffer.allocate(16);
				this.kFC.read(nbf);
				nbf.position(0);
				int mn = nbf.getInt();
				int vr = nbf.getInt();
				if (mn == MAGIC_NUMBER) {
					EL = HashFunctionPool.hashLength + 8;
					this.version = vr;
					this.offset = 16;

				} else {
					EL = HashFunctionPool.hashLength + 4;
					this.version = 0;
					this.offset = 0;
				}
				size = (int) (new File(path).length() - offset) / EL;
			}
		} else {
			EL = HashFunctionPool.hashLength + 4;
			this.version = 0;
			this.offset = 0;
			if (!nf) {
				size = (int) (new File(path).length() - offset) / EL;
			} else {
				rf.setLength((EL * size)+offset);
			}
		}
		vb = ByteBuffer.allocateDirect(EL);
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
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int hi = this.hashFunc1(hash);
		int index = hi * EL;
		byte[] cur = new byte[FREE.length];
		if (this.mapped == null || this.mapped.get(hi)) {
			kFC.read(ByteBuffer.wrap(cur), index + offset);
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
				kFC.read(ByteBuffer.wrap(cur), index + offset);
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
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			this.close();
		} catch (Exception e) {

		}
		try {
			File f = new File(this.path);
			f.delete();
		} catch (Exception e) {
		}
		l.unlock();
	}

	protected int insertionIndex(byte[] key) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int hi = this.hashFunc1(hash);
		int index = hi * EL;
		byte[] cur = new byte[FREE.length];
		if (this.mapped == null || this.mapped.get(hi)) {

			kFC.read(ByteBuffer.wrap(cur), index + offset);

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
				kFC.read(ByteBuffer.wrap(cur), index + offset);

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

	ByteBuffer vb = null;

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
			//SDFSLogger.getLog().info("wrote at " + pos + " fp " + (pos + offset) + " val " + value);
			vb.position(0);
			vb.put(key);
			if (version == 0) {
				vb.putInt((int) value);
			} else {
				vb.putLong(value);
			}
			vb.position(0);
			this.kFC.write(vb, pos + offset);
			vb.position(0);
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
		Lock l = this.hashlock.readLock();
		l.lock();
		ByteBuffer kb = ByteBuffer.allocate(EL);
		try {

			if (this.closed)
				throw new MapClosedException();
			if (key == null)
				return -1;
			int pos = this.index(key);
			if (pos == -1) {
				return -1;
			} else {
				kb.position(0);
				
				this.kFC.read(kb, pos + offset);
				kb.position(VP);
				long val = -1;
				if (this.version == 0)
					val = kb.getInt();
				else
					val = kb.getLong();
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
		try {
			this.kFC.close();
		} catch (Exception e) {

		}
		try {
			this.rf.close();
		} catch (Exception e) {

		}
		this.mapped = null;
		l.unlock();
		SDFSLogger.getLog().debug("closed " + this.path);
	}

	public static void main(String[] args) throws Exception {
		SimpleByteArrayLongMap b = new SimpleByteArrayLongMap(
				"c:\\temp\\-5355749298482906702.map", 10000000,0);
		b.iterInit();
		KeyValuePair p = b.next();
		int i = 0;
		while (p != null) {
			i++;
			System.out.println("key=" + StringUtils.getHexString(p.key)
					+ " value=" + p.value);
			p = b.next();

		}
		System.out.println("sz=" + i);

		/*
		 * Random rnd = new Random(); byte[] hash = null; int val = -33; byte[]
		 * hash1 = null; int val1 = -33; for (int i = 0; i < 60000; i++) { hash
		 * = new byte[16]; rnd.nextBytes(hash); val = rnd.nextInt(); if (i ==
		 * 5000) { val1 = val; hash1 = hash; } if (val < 0) val = val * -1;
		 * boolean k = b.put(hash, val); if (k == false) System.out.println(
		 * "Unable to add this " + k); } long end = System.currentTimeMillis();
		 * System.out.println("Took " + (end - start) / 1000 + " s " + val1);
		 * System.out.println("Took " + (System.currentTimeMillis() - end) /
		 * 1000 + " ms at pos " + b.get(hash1)); b.iterInit(); int vals = 0;
		 * byte[] key = new byte[16]; start = System.currentTimeMillis(); while
		 * (key != null) { KeyValuePair p = b.next(); if(p == null) key = null;
		 * else { key = p.key; if (Arrays.equals(key, hash1))
		 * System.out.println("found it! at " + vals); vals++; } }
		 * System.out.println("Took " + (System.currentTimeMillis() - start) +
		 * " ms " + vals); b.iterInit(); key = new byte[16]; start =
		 * System.currentTimeMillis(); vals = 0; while (key != null) {
		 * KeyValuePair p = b.next(); if(p == null) key = null; else { key =
		 * p.key; int _val = p.value; if (Arrays.equals(key, hash1))
		 * System.out.println("found it! at " + vals); int cval = b.get(key);
		 * if(cval !=_val) System.out.println("poop " + cval + " " +_val);
		 * vals++; } } b.vanish(); System.out.println("Took " +
		 * (System.currentTimeMillis() - start) + " ms " + vals);
		 */
	}

	public void sync() throws SyncFailedException, IOException {
		this.kFC.force(false);

	}

	public static class KeyValuePair {
		long value;
		byte[] key;

		protected KeyValuePair(byte[] key, long value) {
			this.key = key;
			this.value = value;
		}

		public byte[] getKey() {
			return this.key;
		}

		public long getValue() {
			return this.value;
		}
	}
}
