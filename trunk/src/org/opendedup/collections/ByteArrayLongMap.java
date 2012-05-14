package org.opendedup.collections;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.sdfs.Main;
import org.opendedup.util.HashFunctions;
import org.opendedup.util.SDFSLogger;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

public class ByteArrayLongMap {
	ByteBuffer values = null;
	ByteBuffer claims = null;
	ByteBuffer keys = null;
	byte[] compValues = null;
	byte[] compClaims = null;
	byte[] compKeys = null;
	private int size = 0;
	private int entries = 0;

	private ReentrantLock hashlock = new ReentrantLock();
	public static byte[] FREE = new byte[Main.hashLength];
	public static byte[] REMOVED = new byte[Main.hashLength];
	private int iterPos = 0;

	static {
		FREE = new byte[Main.hashLength];
		REMOVED = new byte[Main.hashLength];
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
					claims.position(iterPos - 1);
					byte claimed = claims.get();
					if (clearClaim) {
						claims.position(iterPos - 1);
						claims.put((byte) 0);
					}
					if (claimed == 1)
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
					claims.position(iterPos);
					byte claimed = claims.get();
					if (clearClaim) {
						claims.position(iterPos);
						claims.put((byte) 0);
					}
					if (claimed == 1)
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
			claims = ByteBuffer.allocateDirect(size);
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
			claims.put((byte) 0);
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
			claims = ByteBuffer.wrap(LZFDecoder.decode(compClaims));
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
			compClaims = LZFEncoder.encode(claims.array());
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
				this.claims.position(pos);
				this.claims.put((byte) 1);
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
				this.claims.position(pos);
				byte cl = this.claims.get();
				if (cl == 1)
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

	public boolean update(byte[] key, long value, byte storeID)
			throws IOException {
		try {
			this.hashlock.lock();
			this.decompress();
			int pos = this.index(key);
			if (pos == -1) {
				return false;
			} else {
				try {
					keys.position(pos);
					pos = (pos / FREE.length) * 8;
					this.values.position(pos);
					this.values.putLong(value);
					pos = (pos / 8);
					this.claims.position(pos);
					this.claims.put((byte) 1);
					// this.store.position(pos);
					// this.store.put(storeID);
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

	public boolean remove(byte[] key) throws IOException {
		try {
			this.hashlock.lock();
			this.decompress();
			int pos = this.index(key);
			this.claims.position(pos / FREE.length);
			byte claimed = this.claims.get();
			if (pos == -1) {
				return false;
			} else if (claimed == 1) {
				return false;
			} else {
				try {
					keys.position(pos);
					keys.put(REMOVED);
					pos = (pos / FREE.length) * 8;
					this.values.position(pos);
					this.values.putLong(-1);
					pos = (pos / 8);
					this.claims.position(pos);
					this.claims.put((byte) 0);
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
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int index = this.hashFunc1(hash) * FREE.length;
		// int stepSize = hashFunc2(hash);
		byte[] cur = new byte[FREE.length];
		keys.position(index);
		keys.get(cur);

		if (Arrays.equals(cur, key)) {
			return index;
		}

		if (Arrays.equals(cur, FREE)) {
			return -1;
		}

		// NOTE: here it has to be REMOVED or FULL (some user-given value)
		if (Arrays.equals(cur, REMOVED) || !Arrays.equals(cur, key)) {
			// see Knuth, p. 529
			final int probe = (1 + (hash % (size - 2))) * FREE.length;
			int z = 0;
			do {
				z++;
				index += (probe); // add the step
				index %= (size * FREE.length); // for wraparound
				cur = new byte[FREE.length];
				keys.position(index);
				keys.get(cur);
				if (z > size) {
					SDFSLogger.getLog().info(
							"entries exhaused size=" + this.size + " entries="
									+ this.entries);
					return -1;
				}
			} while (!Arrays.equals(cur, FREE)
					&& (Arrays.equals(cur, REMOVED) || !Arrays.equals(cur, key)));
		}

		return Arrays.equals(cur, FREE) ? -1 : index;
	}

	/**
	 * Locates the index at which <tt>obj</tt> can be inserted. if there is
	 * already a value equal()ing <tt>obj</tt> in the set, returns that value's
	 * index as <tt>-index - 1</tt>.
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return the index of a FREE slot at which obj can be inserted or, if obj
	 *         is already stored in the hash, the negative value of that index,
	 *         minus 1: -index -1.
	 */
	protected int insertionIndex(byte[] key) {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int index = this.hashFunc1(hash) * FREE.length;
		// int stepSize = hashFunc2(hash);
		byte[] cur = new byte[FREE.length];
		keys.position(index);
		keys.get(cur);

		if (Arrays.equals(cur, FREE)) {
			return index; // empty, all done
		} else if (Arrays.equals(cur, key)) {
			return -index - 1; // already stored
		} else { // already FULL or REMOVED, must probe
			// compute the double hash
			final int probe = (1 + (hash % (size - 2))) * FREE.length;

			// if the slot we landed on is FULL (but not removed), probe
			// until we find an empty slot, a REMOVED slot, or an element
			// equal to the one we are trying to insert.
			// finding an empty slot means that the value is not present
			// and that we should use that slot as the insertion point;
			// finding a REMOVED slot means that we need to keep searching,
			// however we want to remember the offset of that REMOVED slot
			// so we can reuse it in case a "new" insertion (i.e. not an update)
			// is possible.
			// finding a matching value means that we've found that our desired
			// key is already in the table
			if (!Arrays.equals(cur, REMOVED)) {
				// starting at the natural offset, probe until we find an
				// offset that isn't full.
				do {
					index += (probe); // add the step
					index %= (size * FREE.length); // for wraparound
					cur = new byte[FREE.length];
					keys.position(index);
					keys.get(cur);
				} while (!Arrays.equals(cur, FREE)
						&& !Arrays.equals(cur, REMOVED)
						&& !Arrays.equals(cur, key));
			}

			// if the index we found was removed: continue probing until we
			// locate a free location or an element which equal()s the
			// one we have.
			if (Arrays.equals(cur, REMOVED)) {
				int firstRemoved = index;
				while (!Arrays.equals(cur, FREE)
						&& (Arrays.equals(cur, REMOVED) || !Arrays.equals(cur,
								key))) {
					index += (probe); // add the step
					index %= (size * FREE.length); // for wraparound
					cur = new byte[FREE.length];
					keys.position(index);
					keys.get(cur);
				}
				// NOTE: cur cannot == REMOVED in this block
				return (!Arrays.equals(cur, FREE)) ? -index - 1 : firstRemoved;
			}
			// if it's full, the key is already stored
			// NOTE: cur cannot equal REMOVE here (would have retuned already
			// (see above)
			return (!Arrays.equals(cur, FREE)) ? -index - 1 : index;
		}
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
				this.claims.position(pos);
				this.claims.put((byte) 1);
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
					this.claims.position(pos);
					this.claims.put((byte) 1);
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

	public static void main(String[] args) throws Exception {
		ByteArrayLongMap b = new ByteArrayLongMap(1000000, (short) 16);
		long start = System.currentTimeMillis();
		Random rnd = new Random();
		byte[] hash = null;
		long val = -33;
		byte[] hash1 = null;
		long val1 = -33;
		for (int i = 0; i < 60000; i++) {
			byte[] z = new byte[64];
			rnd.nextBytes(z);
			hash = HashFunctions.getMD5ByteHash(z);
			val = rnd.nextLong();
			if (i == 55379) {
				val1 = val;
				hash1 = hash;
			}
			if (val < 0)
				val = val * -1;
			boolean k = b.put(hash, val, (byte) 1);
			if (k == false)
				System.out.println("Unable to add this " + k);

		}
		long end = System.currentTimeMillis();
		System.out.println("Took " + (end - start) / 1000 + " s " + val1);
		System.out.println("Took " + (System.currentTimeMillis() - end) / 1000
				+ " ms at pos " + b.get(hash, true));
		b.iterInit();
		int vals = 0;
		byte[] key = new byte[16];
		start = System.currentTimeMillis();
		while (key != null) {
			key = b.nextKey();
			if (Arrays.equals(key, hash1))
				System.out.println("found it! at " + vals);
			vals++;
		}

		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms " + vals);
		b.iterInit();
		key = new byte[16];
		start = System.currentTimeMillis();
		vals = 0;
		while (key != null) {
			key = b.nextClaimedKey(false);
			if (Arrays.equals(key, hash1))
				System.out.println("found it! at " + vals);
			vals++;
		}
		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms " + vals);
		b.iterInit();
		long v = 0;
		start = System.currentTimeMillis();
		vals = 0;
		while (v >= 0) {
			v = b.nextClaimedValue(true);
			vals++;
		}
		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms " + vals);
	}
}
