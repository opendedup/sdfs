package org.opendedup.collections;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.SyncFailedException;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.Tiger16HashEngine;
import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

public class FileByteArrayLongMap {
	MappedByteBuffer keys = null;
	private int size = 0;
	private int entries = 0;
	private String path = null;
	private FileChannel kFC = null;
	private RandomAccessFile vRaf = null;
	private RandomAccessFile tRaf = null;
	private ReentrantLock hashlock = new ReentrantLock();
	public static byte[] FREE = new byte[Main.hashLength];
	public static byte[] REMOVED = new byte[Main.hashLength];
	private int iterPos = 0;
	private boolean closed = false;
	private BitSet claims = null;
	private BitSet mapped = null;
	long bgst = 0;

	static {
		FREE = new byte[Main.hashLength];
		REMOVED = new byte[Main.hashLength];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
	}

	public FileByteArrayLongMap(String path, int size, short arraySize)
			throws IOException {
		this.size = size;
		this.path = path;
	}

	private ReentrantLock iterlock = new ReentrantLock();

	public void iterInit() {
		this.iterlock.lock();
		this.iterPos = 0;
		this.iterlock.unlock();
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
			this.hashlock.lock();
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

	public long nextClaimedValue(boolean clearClaim) throws IOException {
		while (iterPos < size) {
			long val = -1;
			this.hashlock.lock();
			try {
				vRaf.seek(iterPos * 8);
				val = vRaf.readLong();
				if (val >= 0) {
					boolean claimed = claims.get(iterPos);
					if (clearClaim) {
						claims.clear(iterPos);
						this.tRaf.seek(iterPos*8);
						this.tRaf.writeLong(System.currentTimeMillis());
					}
					if (claimed)
						return val;
				}

			} finally {
				iterPos++;
				this.hashlock.unlock();
			}
		}
		return -1;
	}

	public long getBigestKey() throws IOException {
		this.iterInit();
		long _bgst = 0;
		try {
			this.hashlock.lock();
			while (iterPos < size) {
				long val = -1;
				vRaf.seek(iterPos * 8);
				val = vRaf.readLong();
				iterPos++;
				if (val > _bgst)
					_bgst = val;
			}
		} finally {
			this.hashlock.unlock();
		}
		return _bgst;
	}

	/**
	 * initializes the Object set of this hash table.
	 * 
	 * @param initialCapacity
	 *            an <code>int</code> value
	 * @return an <code>int</code> value
	 * @throws IOException
	 */
	public long setUp() throws IOException {
		File posFile = new File(path + ".pos");
		boolean newInstance = !posFile.exists();
		vRaf = new RandomAccessFile(path + ".pos", "rw");
		vRaf.setLength(size * 8);
		tRaf = new RandomAccessFile(path + ".ctimes", "rw");
		tRaf.setLength(size * 8);
		this.kFC = FileChannel.open(Paths.get(path + ".keys"),
				StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
				StandardOpenOption.WRITE, StandardOpenOption.READ);
		RandomAccessFile _bpos = new RandomAccessFile(path + ".bpos", "rw");
		_bpos.setLength(8);
		bgst = _bpos.readLong();
		if (bgst < 0) {
			SDFSLogger.getLog()
					.info("Hashtable " + path
							+ " did not close correctly. scanning ");
			bgst = this.getBigestKey();
		}

		_bpos.seek(0);
		_bpos.writeLong(-1);
		_bpos.close();
		keys = kFC.map(MapMode.READ_WRITE, 0, size * FREE.length);
		keys.load();
		claims = new BitSet(size);
		if (newInstance) {
			mapped = new BitSet(size);
		} else {
			File f = new File(path + ".vmp");
			FileInputStream fin = new FileInputStream(f);
			ObjectInputStream oon = new ObjectInputStream(fin);
			try {
				mapped = (BitSet) oon.readObject();
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			f.delete();
		}
		return bgst;
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
			this.hashlock.unlock();
		}
	}

	public boolean update(byte[] key, long value) throws IOException {
		try {
			this.hashlock.lock();
			int pos = this.index(key);
			if (pos == -1) {
				return false;
			} else {
				keys.position(pos);
				if (value > bgst)
					bgst = value;
				pos = (pos / FREE.length) * 8;
				this.vRaf.seek(pos);
				this.vRaf.writeLong(value);
				pos = (pos / 8);
				this.claims.set(pos);
				// this.store.position(pos);
				// this.store.put(storeID);
				return true;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	public boolean remove(byte[] key) throws IOException {
		try {
			this.hashlock.lock();
			int pos = this.index(key);

			if (pos == -1) {
				return false;
			}
			boolean claimed = this.claims.get(pos);
			if (claimed) {
				return false;
			} else {
				keys.position(pos);
				keys.put(REMOVED);

				pos = (pos / FREE.length) * 8;
				this.vRaf.seek(pos);
				long fp = vRaf.readLong();
				fp = fp * -1;
				this.vRaf.seek(pos);
				this.vRaf.writeLong(fp);
				this.tRaf.seek(pos);
				this.tRaf.writeLong(0);
				pos = (pos / 8);
				this.claims.clear(pos);
				this.mapped.clear(pos);
				// this.store.position(pos);
				// this.store.put((byte)0);
				this.entries = entries - 1;
				return true;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		} finally {
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

	public boolean put(byte[] key, long value) {
		try {
			this.hashlock.lock();
			if (entries >= size)
				throw new IOException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			int pos = this.insertionIndex(key);
			if (pos < 0)
				return false;
			this.keys.position(pos);
			this.keys.put(key);

			if (value > bgst)
				bgst = value;
			pos = (pos / FREE.length) * 8;
			this.vRaf.seek(pos);
			this.vRaf.writeLong(value);
			pos = (pos / 8);
			this.claims.set(pos);
			this.mapped.set(pos);
			// this.store.position(pos);
			// this.store.put(storeID);
			this.entries = entries + 1;
			return pos > -1 ? true : false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error inserting record", e);
			return false;
		} finally {
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
			if (key == null)
				return -1;
			int pos = this.index(key);
			if (pos == -1) {
				return -1;
			} else {
				pos = (pos / FREE.length) * 8;
				this.vRaf.seek(pos);
				long val = this.vRaf.readLong();
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
			this.hashlock.unlock();
		}

	}

	public int size() {
		return this.mapped.cardinality();
	}

	public void close() {
		this.hashlock.lock();
		this.closed = true;
		try {
			this.vRaf.getFD().sync();
			this.vRaf.close();
		} catch (Exception e) {

		}
		try {
			this.kFC.force(true);
			this.kFC.close();
		} catch (Exception e) {

		}
		try {
			this.tRaf.getFD().sync();
			this.tRaf.close();
		} catch (Exception e) {

		}
		try {
			File f = new File(path + ".vmp");
			FileOutputStream fout = new FileOutputStream(f);
			ObjectOutputStream oon = new ObjectOutputStream(fout);
			oon.writeObject(mapped);
			oon.flush();
			oon.close();
			fout.flush();
			fout.close();
		} catch (Exception e) {

		}
		try {
			RandomAccessFile _bpos = new RandomAccessFile(path + ".bpos", "rw");
			_bpos.seek(0);
			_bpos.writeLong(bgst);
			_bpos.close();
		} catch (Exception e) {

		}

		this.hashlock.unlock();
		SDFSLogger.getLog().debug("closed " + this.path);
	}

	public static void main(String[] args) throws Exception {
		FileByteArrayLongMap b = new FileByteArrayLongMap(
				"/opt/sdfs/hashesaaa", 10000000, (short) 16);
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
			if (i == 5000) {
				val1 = val;
				hash1 = hash;
			}
			if (val < 0)
				val = val * -1;
			boolean k = b.put(hash, val);
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

	public synchronized int claimRecords() throws IOException {
		if (this.closed)
			throw new IOException("Hashtable " + this.path + " is close");
		int k = 0;
		try {
			this.iterInit();
			while (iterPos < size) {
				this.hashlock.lock();
				try {
					boolean claimed = claims.get(iterPos);
					if (claimed) {
						claims.clear(iterPos);
						this.tRaf.seek(iterPos * 8);
						this.tRaf.writeLong(System.currentTimeMillis());
						k++;
					}
				} finally {
					iterPos++;
					this.hashlock.unlock();
				}
			}
		} catch (NullPointerException e) {

		}
		return k;
	}
	
	public void sync() throws SyncFailedException, IOException {
		keys.force();
		vRaf.getFD().sync();
		tRaf.getFD().sync();
		File f = new File(path + ".vmp");
		FileOutputStream fout = new FileOutputStream(f);
		ObjectOutputStream oon = new ObjectOutputStream(fout);
		oon.writeObject(mapped);
		oon.flush();
		oon.close();
		fout.flush();
		fout.close();
	}

	public synchronized long removeNextOldRecord(long time) throws IOException {
		while (iterPos < size) {
			long val = -1;
			this.hashlock.lock();
			try {
				if (mapped.get(iterPos)) {
					this.tRaf.seek(iterPos * 8);
					long tm = this.tRaf.readLong();
					if (tm < time) {
						boolean claimed = this.claims.get(iterPos);
						if (!claimed) {
							keys.position(iterPos * FREE.length);
							keys.put(REMOVED);
							this.vRaf.seek(iterPos * 8);
							val = this.vRaf.readLong();
							long fp = val * -1;
							this.vRaf.seek(iterPos * 8);
							this.vRaf.writeLong(fp);
							this.tRaf.seek(iterPos * 8);
							this.tRaf.writeLong(-1);
							this.claims.clear(iterPos);
							this.mapped.clear(iterPos);
							this.entries = entries - 1;
							return val;
						}
					}
				}
			} finally {
				iterPos++;
				this.hashlock.unlock();
			}
		}
		return -1;
	}
}
