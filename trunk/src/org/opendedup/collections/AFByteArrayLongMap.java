package org.opendedup.collections;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.collections.threads.SyncThread;
import org.opendedup.util.HashFunctions;

public class AFByteArrayLongMap implements AbstractMap {
	ByteBuffer values = null;
	ByteBuffer keys = null;
	ByteBuffer claims = null;
	RandomAccessFile kRaf = null;
	private int size = 0;
	private ReentrantLock hashlock = new ReentrantLock();
	private static Logger log = Logger.getLogger("sdfs");
	private byte[] FREE = new byte[16];
	private byte[] REMOVED = new byte[16];
	private long resValue = -1;
	private long freeValue = -1;
	private String fileName;
	private ByteBuffer kBuf = null;
	private boolean closed = true;
	int kSz = 0;
	private int iterPos = 0;

	public AFByteArrayLongMap(int maxSize, short arraySize, String fileName)
			throws IOException {
		this.size = maxSize;
		this.fileName = fileName;
		FREE = new byte[arraySize];
		REMOVED = new byte[arraySize];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
		this.setUp();
		this.closed = false;
		new SyncThread(this);
	}

	public boolean isClosed() {
		return this.closed;
	}

	public int getSize() {
		return this.kSz;

	}

	public int getMaxSize() {
		return this.size;
	}

	public void iterInit() {
		this.hashlock.lock();
		try {
			this.iterPos = 0;
		} catch (Exception e) {
		} finally {
			this.hashlock.unlock();
		}
	}

	public byte[] nextKey() {
		this.hashlock.lock();
		try {
			byte[] key = new byte[FREE.length];

			while (iterPos < this.keys.capacity()) {
				this.keys.position(iterPos);
				this.keys.get(key);
				this.iterPos = this.keys.position();
				if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED))
					return key;
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "error while iterating through keys", e);
		} finally {
			this.hashlock.unlock();
		}
		return null;
	}

	public byte[] nextClaimedKey(boolean clearClaim) {
		this.hashlock.lock();
		try {
			byte[] key = new byte[FREE.length];

			while (iterPos < this.keys.capacity()) {
				this.keys.position(iterPos);
				this.keys.get(key);
				int cp = this.iterPos;
				this.iterPos = this.keys.position();
				if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
					claims.position(cp / FREE.length);
					if (claims.get() == 1) {
						if (clearClaim) {
							claims.position(cp / FREE.length);
							claims.put((byte) 0);
						}
						return key;
					}
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "error while iterating through keys", e);
		} finally {
			this.hashlock.unlock();
		}
		return null;
	}

	/**
	 * initializes the Object set of this hash table.
	 * 
	 * @param initialCapacity
	 *            an <code>int</code> value
	 * @return an <code>int</code> value
	 * @throws FileNotFoundException
	 */
	public int setUp() throws IOException {

		boolean exists = new File(fileName).exists();
		long fileSize = this.size * (FREE.length + 8);
		if (fileSize > Integer.MAX_VALUE)
			throw new IOException("File size is limited to "
					+ Integer.MAX_VALUE + " proposed fileSize would be "
					+ fileSize + " try "
					+ "reducing the number of entries or the key size");
		kRaf = new RandomAccessFile(fileName, "rw");
		kBuf = ByteBuffer.allocateDirect(300 * (FREE.length + 8));
		keys = ByteBuffer.allocateDirect(size * FREE.length);
		values = ByteBuffer.allocateDirect(size * 8);
		claims = ByteBuffer.allocateDirect(size);
		if (!exists) {
			log.info("This looks like a new hashtable will prepopulate with ["
					+ size + "] entries.");

			for (int i = 0; i < size; i++) {
				this.flushFullBuffer();
				keys.put(FREE);
				kBuf.put(FREE);
				values.putLong(-1);
				this.claims.put((byte) 0);
				kBuf.putLong(-1);
			}
			this.flushBuffer(true);
			kRaf.getChannel().position(0);
		} else {
			log.info("This looks an existing hashtable will repopulate with ["
					+ size + "] entries.");
			for (int i = 0; i < size; i++) {
				byte[] k = new byte[FREE.length];
				kRaf.read(k);
				boolean foundFree = Arrays.equals(k, FREE);
				boolean foundReserved = Arrays.equals(k, REMOVED);
				long value = kRaf.readLong();

				if (foundFree && value > -1) {
					this.freeValue = value;
					log.info("found free  key  with value " + value);
				} else if (foundReserved && value > -1) {
					this.resValue = value;
					log.info("found reserve  key  with value " + value);
				} else if (foundFree && value < 0) {
					kRaf.seek(kRaf.getChannel().position() - k.length);
					kRaf.seek(kRaf.getChannel().position() - 8);
					break;
				} else if (value >= 0) {
					this.put(k, value, false);
					kSz++;
				}
			}
		}
		log.info("loaded [" + kSz + "] into the hashtable " + this.fileName);

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
			int idx = index(key);
			if (idx >= 0) {
				this.claims.position(idx / FREE.length);
				this.claims.put((byte) 0);
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

	private int hashFunc1(int hash) {
		return hash % size;
	}

	private int hashFunc2(int hash) {
		return 6 - hash % 6;
	}

	/**
	 * Locates the index of <tt>obj</tt>.
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return the index of <tt>obj</tt> or -1 if it isn't in the set.
	 */
	private int index(byte[] key) {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.getInt();
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
	private int insertionIndex(byte[] key) {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.getInt();
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

	public boolean put(byte[] key, long value) throws IOException {
		if (key.length != this.FREE.length)
			throw new IOException("key length mismatch");
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		this.flushFullBuffer();
		boolean added = false;
		boolean foundFree = Arrays.equals(key, FREE);
		boolean foundReserved = Arrays.equals(key, REMOVED);
		if (foundFree && value > 0) {
			this.freeValue = value;
			try {
				this.hashlock.lock();
				this.kBuf.put(key);
				this.kBuf.putLong(value);
			} catch (Exception e) {
				throw new IOException(e.toString());
			} finally {
				this.hashlock.unlock();
			}
			added = true;
		} else if (foundReserved && value > 0) {
			this.resValue = value;
			try {
				this.hashlock.lock();
				this.kBuf.put(key);
				this.kBuf.putLong(value);
			} catch (Exception e) {
				throw new IOException(e.toString());
			} finally {
				this.hashlock.unlock();
			}
			added = true;
		} else {
			added = this.put(key, value, true);
		}
		if (added)
			kSz++;
		return added;
	}

	private boolean put(byte[] key, long value, boolean persist)
			throws IOException {
		if (kSz >= this.size)
			throw new IOException("maximum sized reached");
		if (persist)
			this.flushFullBuffer();
		try {
			this.hashlock.lock();
			if (value < 0)
				throw new IOException("unable to add value [" + value
						+ "] less than 0 ");
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
			if (persist) {
				try {
					this.kBuf.put(key);
					this.kBuf.putLong(value);
				} catch (BufferOverflowException e) {
					this.flushBuffer(false);
					this.kBuf.put(key);
					this.kBuf.putLong(value);
				}
			}
			return pos > -1 ? true : false;
		} catch (Exception e) {
			log.log(Level.SEVERE, "error inserting record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	public boolean update(byte[] key, long value) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		try {
			this.hashlock.lock();
			int pos = this.index(key);
			if (pos == -1) {
				return false;
			} else {
				pos = (pos / FREE.length) * 8;
				this.values.position(pos);
				this.values.putLong(value);
				pos = (pos / 8);
				this.claims.position(pos);
				this.claims.put((byte) 1);
				try {
					this.kBuf.put(key);
					this.kBuf.putLong(value);
				} catch (BufferOverflowException e) {
					this.flushBuffer(false);
					this.kBuf.put(key);
					this.kBuf.putLong(value);
				}
				return true;
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "error record record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	public long get(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		try {
			this.hashlock.lock();
			int pos = this.index(key);
			if (pos == -1) {
				return -1;
			} else {
				pos = (pos / FREE.length) * 8;
				this.values.position(pos);
				long val = this.values.getLong();
				pos = (pos / 8);
				this.claims.position(pos);
				this.claims.put((byte) 1);
				return val;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "error getting record", e);
			return -1;
		} finally {
			this.hashlock.unlock();
		}

	}

	private synchronized void flushBuffer(boolean lock) throws IOException {
		ByteBuffer oldkBuf = null;
		try {
			if (lock)
				this.hashlock.lock();
			oldkBuf = kBuf;
			kBuf = ByteBuffer.allocateDirect(500 * (FREE.length + 8));
		} catch (Exception e) {
		} finally {
			if (lock)
				this.hashlock.unlock();
		}
		int oldkBufPos = oldkBuf.position();
		oldkBuf.flip();
		byte[] b = new byte[oldkBufPos];
		oldkBuf.get(b);
		kRaf.write(b);
		oldkBuf.clear();
		oldkBuf = null;
		b = null;

	}

	public boolean remove(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		try {
			this.hashlock.lock();
			int pos = this.index(key);
			if (pos == -1) {
				return false;
			} else {
				keys.position(pos);
				keys.put(this.REMOVED);
				pos = (pos / FREE.length) * 8;
				this.values.position(pos);
				this.values.putLong(-1);
				pos = (pos / 8);
				this.claims.position(pos);
				this.claims.put((byte) 0);
				try {
					this.kBuf.put(key);
					this.kBuf.putLong(-1);
				} catch (BufferOverflowException e) {
					this.flushBuffer(false);
					this.kBuf.put(key);
					this.kBuf.putLong(-1);
				}
				return true;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "error getting record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	private synchronized void flushFullBuffer() throws IOException {
		boolean flush = false;
		try {
			this.hashlock.lock();
			if (kBuf.position() == kBuf.capacity()) {
				flush = true;
			}
		} catch (Exception e) {
		} finally {
			this.hashlock.unlock();
		}
		if (flush) {
			this.flushBuffer(true);
		}
	}

	public synchronized void sync() throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		this.flushBuffer(true);
		this.kRaf.getFD().sync();
	}

	public void close() {
		this.closed = true;
		try {
			this.sync();
		} catch (Exception e) {
		}
		this.keys = null;
		this.values = null;
	}

	public static void main(String[] args) throws Exception {
		for (int l = 0; l < 1; l++) {
			AFByteArrayLongMap b = new AFByteArrayLongMap(16777216 * 6,
					(short) 16, "/opt/ddb/test-" + l);
			long start = System.currentTimeMillis();
			Random rnd = new Random();
			byte[] hash = null;
			long val = -33;
			byte[] hash1 = null;
			long val1 = -33;
			for (int i = 0; i < 11000000; i++) {
				byte[] z = new byte[64];
				rnd.nextBytes(z);
				hash = HashFunctions.getMD5ByteHash(z);
				val = rnd.nextLong();
				if (val < 0)
					val *= -1;
				if (i == 55379) {
					val1 = val;
					hash1 = hash;
				}
				boolean k = b.put(hash, val);
				if (k == false)
					System.out.println("Unable to add this " + k);

			}
			long end = System.currentTimeMillis();
			System.out.println("Took " + (end - start) / 1000 + " s " + val1);
			System.out.println("Took " + (System.currentTimeMillis() - end)
					/ 1000 + " ms at pos " + b.get(hash1));
			end = System.currentTimeMillis();
			b.sync();
			System.out.println("Sync Took "
					+ (System.currentTimeMillis() - end));
		}
	}

	@Override
	public byte[] get(long pos) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void put(long pos, byte[] data) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void remove(long pos) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void vanish() throws IOException {
		// TODO Auto-generated method stub

	}
}
