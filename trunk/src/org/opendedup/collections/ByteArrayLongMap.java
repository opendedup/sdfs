package org.opendedup.collections;

import java.io.IOException;


import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

public class ByteArrayLongMap {
	ByteBuffer values = null;
	private BitSet claims = null;
	ByteBuffer keys = null;
	byte[] compValues = null;
	byte[] compClaims = null;
	byte[] compKeys = null;
	private int size = 0;
	private int entries = 0;

	private ReentrantLock hashlock = new ReentrantLock();
	public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	public static byte[] REMOVED = new byte[HashFunctionPool.hashLength];
	
	private int iterPos = 0;

	static {
		FREE = new byte[HashFunctionPool.hashLength];
		REMOVED = new byte[HashFunctionPool.hashLength];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
	}

	public ByteArrayLongMap(int size, short arraySize) throws IOException {
		this.size = size;
		this.setUp();
	}

	private ReentrantLock iterlock = new ReentrantLock();

	public void iterInit() {
		this.iterlock.lock();
		this.iterPos = 0;
		this.iterlock.unlock();
	}

	public byte[] nextKey() {
		while (iterPos < size) {
			this.hashlock.lock();
			try {
			byte[] key = new byte[FREE.length];
			keys.position(iterPos * FREE.length);
			keys.get(key);
			iterPos++;
			if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
				return key;
			}
			}finally {
				this.hashlock.unlock();
			}
		}
		return null;
	}

	public byte[] nextClaimedKey(boolean clearClaim) {
		while (iterPos < size) {
			this.hashlock.lock();
			byte[] key = new byte[FREE.length];
			keys.position(iterPos * FREE.length);
			try {
				keys.get(key);
				iterPos++;
				if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
					boolean claimed = claims.get(iterPos - 1);
					if (clearClaim) {
						claims.clear(iterPos - 1);
					}
					if (claimed)
						return key;
				}
			} catch (Exception e) {

			} finally {
				this.hashlock.unlock();
			}
		}
		return null;
	}

	public long nextClaimedValue(boolean clearClaim) {
		while (iterPos < size) {
			long val = -1;
			this.hashlock.lock();
			values.position(iterPos * 8);
			try {
				val = values.getLong();
				if (val >= 0) {
					boolean claimed = claims.get(iterPos);
					if (clearClaim) {
						claims.clear(iterPos);
					}
					if (claimed)
						return val;
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("error getting next claimed value at [" + iterPos + "]", e);
			} finally {
				iterPos++;
				this.hashlock.unlock();
			}
		}
		return -1;
	}

	/**
	 * initializes the Object set of this hash table.
	 * 
	 * @param initialCapacity
	 *            an <code>int</code> value
	 * @return an <code>int</code> value
	 * @throws IOException
	 */
	public int setUp() throws IOException {
		if (!Main.compressedIndex) {
			keys = ByteBuffer.allocateDirect(size * FREE.length);
			values = ByteBuffer.allocateDirect(size * 8);
			claims = new BitSet(size);
		} else {
			byte[] keyB = new byte[size * FREE.length];
			byte[] valueB = new byte[size * 8];
			byte[] claimsB = new byte[size];
			this.compKeys = LZFEncoder.encode(keyB);
			this.compValues = LZFEncoder.encode(valueB);
			this.compClaims = LZFEncoder.encode(claimsB);
		}
		this.decompress();
		for (int i = 0; i < size; i++) {
			keys.put(FREE);
			values.putLong(-1);
			claims.clear();
			// store.put((byte) 0);
		}
		this.compress();
		this.derefByteArray();
		// store = ByteBuffer.allocateDirect(size);

		// values = new long[this.size][this.size];
		// Arrays.fill( keys, FREE );
		// Arrays.fill(values, blank);
		return size;
	}

	private void decompress() throws IOException {
		if (Main.compressedIndex) {
			keys = ByteBuffer.wrap(LZFDecoder.decode(compKeys));
			values = ByteBuffer.wrap(LZFDecoder.decode(compValues));
			
		}
	}

	private void derefByteArray() {
		if (Main.compressedIndex) {
			keys = null;
			values = null;
			claims = null;
		}
	}

	private void compress() throws IOException {
		if (Main.compressedIndex) {
			compKeys = LZFEncoder.encode(keys.array());
			compValues = LZFEncoder.encode(values.array());
		}
	}

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 */
	public boolean containsKey(byte[] key) {
		try {
			this.hashlock.lock();
			this.decompress();
			int index = index(key);
			if (index >= 0) {
				int pos = (index / FREE.length);
				this.claims.set(pos);
				return true;
			}
			return false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		} finally {
			this.derefByteArray();
			this.hashlock.unlock();
		}
	}

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 */
	public boolean isClaimed(byte[] key) {
		try {
			this.hashlock.lock();
			this.decompress();
			int index = index(key);
			if (index >= 0) {
				int pos = (index / FREE.length);
				boolean cl = this.claims.get(pos);
				if (cl)
					return true;
			}
			return false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		} finally {
			this.derefByteArray();
			this.hashlock.unlock();
		}
	}

	public boolean update(byte[] key, long value)
			throws KeyNotFoundException {
		try {
			this.hashlock.lock();
			int pos = this.index(key);
			if (pos == -1) {
				throw new KeyNotFoundException();
			} else {
					keys.position(pos);
					pos = (pos / FREE.length) * 8;
					this.values.position(pos);
					this.values.putLong(value);
					pos = (pos / 8);
					this.claims.set(pos);
					return true;
			}
		}  finally {
			this.hashlock.unlock();
		}
	}

	public boolean remove(byte[] key) throws IOException {
		try {
			this.hashlock.lock();
			this.decompress();
			int pos = this.index(key);
			boolean claimed = this.claims.get(pos / FREE.length);
			if (pos == -1) {
				return false;
			} else if (claimed) {
				return false;
			} else {
				try {
					keys.position(pos);
					keys.put(REMOVED);
					pos = (pos / FREE.length) * 8;
					this.values.position(pos);
					this.values.putLong(-1);
					pos = (pos / 8);
					this.claims.clear(pos);
					// this.store.position(pos);
					// this.store.put((byte)0);
					this.entries = entries - 1;
					return true;
				} catch (Exception e) {
					throw e;
				} finally {
					this.compress();
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		} finally {
			this.derefByteArray();
			this.hashlock.unlock();
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
	 */
	protected int index(byte[] key) {
		// From here on we know obj to be non-null
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int index = this.hashFunc1(hash) * FREE.length;
		byte[] cur = new byte[FREE.length];
		keys.position(index);
		keys.get(cur);

		if (Arrays.equals(cur, key)) {
			return index;
		}

		if (Arrays.equals(cur, FREE)) {
			return -1;
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
	 */
	private int indexRehashed(byte[] key, int index, int hash, byte[] cur) {

		// NOTE: here it has to be REMOVED or FULL (some user-given value)
		// see Knuth, p. 529
		int length = size * FREE.length;
		int probe = (1 + (hash % (size - 2))) * FREE.length;

		final int loopIndex = index;

		do {
			index -= probe;
			if (index < 0) {
				index += length;
			}
			keys.position(index);
			keys.get(cur);
			//
			if (Arrays.equals(cur, FREE)) {
				return -1;
			}
			//
			if (Arrays.equals(cur, key))
				return index;
		} while (index != loopIndex);

		return -1;
	}

	protected int insertionIndex(byte[] key) {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int index = this.hashFunc1(hash) * FREE.length;
		byte[] cur = new byte[FREE.length];
		keys.position(index);
		keys.get(cur);

		if (Arrays.equals(cur, FREE)) {
			return index; // empty, all done
		} else if (Arrays.equals(cur, key)) {
			return -index - 1; // already stored
		}
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
	 */
	private int insertKeyRehash(byte[] key, int index, int hash, byte[] cur) {
		final int length = size * FREE.length;
		final int probe = (1 + (hash % (size - 2))) * FREE.length;

		final int loopIndex = index;
		int firstRemoved = -1;

		/**
		 * Look until FREE slot or we start to loop
		 */
		do {
			// Identify first removed slot
			if (Arrays.equals(cur, REMOVED) && firstRemoved == -1)
				firstRemoved = index;

			index -= probe;
			if (index < 0) {
				index += length;
			}
			keys.position(index);
			keys.get(cur);

			// A FREE slot stops the search
			if (Arrays.equals(cur, FREE)) {
				if (firstRemoved != -1) {
					return firstRemoved;
				} else {
					return index;
				}
			}

			if (Arrays.equals(cur, key)) {
				return -index - 1;
			}

			// Detect loop
		} while (index != loopIndex);

		// We inspected all reachable slots and did not find a FREE one
		// If we found a REMOVED slot we return the first one found
		if (firstRemoved != -1) {
			return firstRemoved;
		}

		// Can a resizing strategy be found that resizes the set?
		throw new IllegalStateException(
				"No free or removed slots available. Key set full?!!");
	}

	public boolean put(byte[] key, long value, byte storeID) {
		try {
			this.hashlock.lock();
			this.decompress();
			if (entries >= size)
				throw new IOException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			int pos = this.insertionIndex(key);
			if (pos < 0)
				return false;
			try {
				this.keys.position(pos);
				this.keys.put(key);
				pos = (pos / FREE.length) * 8;
				this.values.position(pos);
				this.values.putLong(value);
				pos = (pos / 8);
				this.claims.set(pos);
				// this.store.position(pos);
				// this.store.put(storeID);
				this.entries = entries + 1;
				return pos > -1 ? true : false;
			} catch (Exception e) {
				throw e;
			} finally {
				this.compress();
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error inserting record", e);
			return false;
		} finally {
			this.derefByteArray();
			this.hashlock.unlock();
		}
	}

	public int getEntries() {
		return this.entries;
	}

	public long get(byte[] key) {
		return this.get(key, true);
	}
	
	

	public long get(byte[] key, boolean claim) {
		try {
			this.hashlock.lock();
			this.decompress();
			if (key == null)
				return -1;
			int pos = this.index(key);
			if (pos == -1) {
				return -1;
			} else {
				pos = (pos / FREE.length) * 8;
				this.values.position(pos);
				long val = this.values.getLong();
				if (claim) {
					pos = (pos / 8);
					this.claims.set(pos);
				}
				return val;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return -1;
		} finally {
			this.derefByteArray();
			this.hashlock.unlock();
		}

	}

	public int size() {
		return this.size;
	}

	
}
