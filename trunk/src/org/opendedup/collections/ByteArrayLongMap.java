package org.opendedup.collections;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.util.HashFunctions;
import org.opendedup.util.StringUtils;

public class ByteArrayLongMap {
	ByteBuffer values = null;
	ByteBuffer claims = null;
	ByteBuffer store = null;
	ByteBuffer keys = null;
	private int size = 0;
	private final static long blank = -1;
	private ReentrantLock hashlock = new ReentrantLock();
	private static Logger log = Logger.getLogger("sdfs");
	public byte[] FREE = new byte[16];
	public byte[] REMOVED = new byte[16];
	private int iterPos = 0;

	public ByteArrayLongMap(int size, short arraySize) {
		this.size = size;
		FREE = new byte[arraySize];
		REMOVED = new byte[arraySize];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
		this.setUp();
	}

	public void iterInit() {
		this.iterPos = 0;
	}

	public byte[] nextKey() {
		while (iterPos < size) {
			byte[] key = new byte[FREE.length];
			keys.position(iterPos * FREE.length);
			keys.get(key);
			iterPos++;
			if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
				return key;
			}
		}
		return null;
	}

	public byte[] nextClaimedKey(boolean clearClaim) {
		while (iterPos < size) {
			byte[] key = new byte[FREE.length];
			keys.position(iterPos * FREE.length);
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
		}
		return null;
	}

	public long nextClaimedValue(boolean clearClaim) {
		while (iterPos < size) {
			long val = -1;
			values.position(iterPos * 8);
			val = values.getLong();
			iterPos++;
			if (val >= 0) {
				claims.position(iterPos - 1);
				byte claimed = claims.get();
				if (clearClaim) {
					claims.position(iterPos - 1);
					claims.put((byte) 0);
				}
				if (claimed == 1)
					return val;
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
	 */
	public int setUp() {
		int kSz = 0;
		keys = ByteBuffer.allocateDirect(size * FREE.length);
		values = ByteBuffer.allocateDirect(size * 8);
		claims = ByteBuffer.allocateDirect(size);
		store = ByteBuffer.allocateDirect(size);
		for (int i = 0; i < size; i++) {
			keys.put(FREE);
			values.putLong(-1);
			claims.put((byte) 0);
			store.put((byte) 0);
			kSz++;
		}
		// values = new long[this.size][this.size];
		// Arrays.fill( keys, FREE );
		// Arrays.fill(values, blank);
		return size;
	}

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 */
	@SuppressWarnings( { "unchecked" })
	public boolean containsKey(byte[] key) {
		try {
			this.hashlock.lock();
			int index = index(key);
			if (index >= 0) {
				int pos = (index / FREE.length);
				this.claims.position(pos);
				this.claims.put((byte) 1);
				return true;
			}
			return false;
		} catch (Exception e) {
			log.log(Level.SEVERE, "error getting record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}
	
	public boolean update(byte[] key,long value,byte storeID) throws IOException {
		try {
			this.hashlock.lock();
			int pos = this.index(key);
			if (pos == -1) {
				return false;
			} else {
				keys.position(pos);
				pos = (pos / FREE.length) * 8;
				this.values.position(pos);
				this.values.putLong(value);
				pos = (pos / 8);
				this.claims.position(pos);
				this.claims.put((byte) 1);
				this.store.position(pos);
				this.store.put(storeID);
				return true;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "error getting record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	public boolean remove(byte[] key) throws IOException {
		try {
			this.hashlock.lock();
			int pos = this.index(key);
			this.claims.position(pos / FREE.length);
			byte claimed = this.claims.get();
			if (pos == -1) {
				return false;
			} else if (claimed == 1) {
				return false;
			} else {
				keys.position(pos);
				keys.put(this.REMOVED);
				pos = (pos / FREE.length) * 8;
				this.values.position(pos);
				this.values.position(pos);
				this.values.putLong(-1);
				pos = (pos / 8);
				this.claims.position(pos);
				this.claims.put((byte) 0);
				this.store.position(pos);
				this.store.put((byte)0);
				return true;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "error getting record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	public int hashFunc1(int hash) {
		return hash % size;
	}

	public int hashFunc2(int hash) {
		return 6 - hash % 6;
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
		int stepSize = hashFunc2(hash);
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
			// final int probe = (1 + (hash % (length - 2))) * FREE.length;

			do {
				index += (stepSize * FREE.length); // add the step
				index %= (size * FREE.length); // for wraparound
				cur = new byte[FREE.length];
				keys.position(index);
				keys.get(cur);
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
		int stepSize = hashFunc2(hash);
		byte[] cur = new byte[FREE.length];
		keys.position(index);
		keys.get(cur);

		if (Arrays.equals(cur, FREE)) {
			return index; // empty, all done
		} else if (Arrays.equals(cur, key)) {
			return -index - 1; // already stored
		} else { // already FULL or REMOVED, must probe
			// compute the double hash
			// final int probe = (1 + (hash % (length - 2))) * FREE.length;

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
				int w = 0;
				do {
					w++;

					index += (stepSize * FREE.length); // add the step
					index %= (size * FREE.length); // for wraparound
					if (w > 1000) {
						log.finest("been searching at index " + index
								+ " for " + w);
					}
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
					index += (stepSize * FREE.length); // add the step
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

	public boolean put(byte[] key, long value,byte storeID) {
		try {
			this.hashlock.lock();
			int pos = this.insertionIndex(key);
			if (pos < 0)
				return false;
			this.keys.position(pos);
			this.keys.put(key);
			pos = (pos / FREE.length) * 8;
			this.values.position(pos);
			this.values.putLong(value);
			pos = (pos / 8);
			this.claims.position(pos);
			this.claims.put((byte) 1);
			this.store.position(pos);
			this.store.put(storeID);
			return pos > -1 ? true : false;
		} catch (Exception e) {
			log.log(Level.SEVERE, "error inserting record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	public long get(byte[] key) {
		return this.get(key, true);
	}

	public long get(byte[] key, boolean claim) {
		try {
			this.hashlock.lock();
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
			log.log(Level.SEVERE, "error getting record", e);
			return -1;
		} finally {
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
			boolean k = b.put(hash, val,(byte)1);
			if (k == false)
				System.out.println("Unable to add this " + k);

		}
		long end = System.currentTimeMillis();
		System.out.println("Took " + (end - start) / 1000 + " s " + val1);
		System.out.println("Took " + (System.currentTimeMillis() - end) / 1000
				+ " ms at pos " + b.get(hash1));
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
