package org.opendedup.collections;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
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

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.Tiger16HashEngine;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.ChunkData;

public class FileByteArrayLongMap implements AbstractShard {
	MappedByteBuffer keys = null;
	MappedByteBuffer values = null;
	MappedByteBuffer times = null;
	private int size = 0;
	private String path = null;
	private FileChannel kFC = null;
	private FileChannel vRaf = null;
	private FileChannel tRaf = null;
	private ReentrantLock hashlock = new ReentrantLock();
	public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	public static byte[] REMOVED = new byte[HashFunctionPool.hashLength];
	private int iterPos = 0;
	private boolean closed = false;
	private BitSet claims = null;
	private BitSet mapped = null;
	long bgst = 0;

	static {
		FREE = new byte[HashFunctionPool.hashLength];
		REMOVED = new byte[HashFunctionPool.hashLength];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
	}

	public FileByteArrayLongMap(String path, int size, short arraySize)
			throws IOException {
		this.size = size;
		this.path = path;
	}

	private ReentrantLock iterlock = new ReentrantLock();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#iterInit()
	 */
	@Override
	public void iterInit() {
		this.iterlock.lock();
		this.iterPos = 0;
		this.iterlock.unlock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#nextKey()
	 */
	@Override
	public byte[] nextKey() {
		while (iterPos < size) {
			byte[] key = new byte[FREE.length];
			keys.position(iterPos * FREE.length);
			keys.get(key);
			iterPos++;
			if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
				this.mapped.set(iterPos-1);
				return key;
			} else {
				this.mapped.clear(iterPos-1);
			}
			
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#nextClaimedKey(boolean)
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#nextClaimedValue(boolean)
	 */
	@Override
	public long nextClaimedValue(boolean clearClaim) throws IOException {
		while (iterPos < size) {
			long val = -1;
			this.hashlock.lock();
			try {
				values.position(iterPos * 8);
				val = values.getLong();
				if (val >= 0) {
					boolean claimed = claims.get(iterPos);
					if (clearClaim) {
						this.mapped.set(iterPos);
						claims.clear(iterPos);
						this.times.position(iterPos * 8);
						this.times.putLong(System.currentTimeMillis());
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

	private void recreateMap() {
		mapped = new BitSet(size);
		mapped.clear();
		this.iterInit();
		byte[] key = this.nextKey();
		while (key != null)
			key = this.nextKey();
		SDFSLogger.getLog().warn(
				"Recovered Hashmap " + this.path + " entries = "
						+ mapped.cardinality());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#getBigestKey()
	 */
	@Override
	public long getBigestKey() throws IOException {
		this.iterInit();
		long _bgst = 0;
		try {
			this.hashlock.lock();
			while (iterPos < size) {
				long val = -1;
				values.position(iterPos * 8);
				val = values.getLong();
				iterPos++;
				if (val > _bgst)
					_bgst = val;
			}
		} finally {
			this.hashlock.unlock();
		}
		return _bgst;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#setUp()
	 */
	@Override
	public long setUp() throws IOException {
		File posFile = new File(path + ".pos");
		boolean newInstance = !posFile.exists();
		vRaf = FileChannel.open(Paths.get(path + ".pos"),
				StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
				StandardOpenOption.WRITE, StandardOpenOption.READ);

		tRaf = FileChannel.open(Paths.get(path + ".ctimes"),
				StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
				StandardOpenOption.WRITE, StandardOpenOption.READ);

		this.kFC = FileChannel.open(Paths.get(path + ".keys"),
				StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
				StandardOpenOption.WRITE, StandardOpenOption.READ);
		RandomAccessFile _bpos = new RandomAccessFile(path + ".bpos", "rw");
		_bpos.setLength(8);
		bgst = _bpos.readLong();
		boolean closedCorrectly = true;
		if (newInstance) {
			mapped = new BitSet(size);
		} else {
			File f = new File(path + ".vmp");
			if (!f.exists())
				closedCorrectly = false;
			else {
				FileInputStream fin = new FileInputStream(f);
				ObjectInputStream oon = new ObjectInputStream(fin);
				try {
					mapped = (BitSet) oon.readObject();
				} catch (Exception e) {
					closedCorrectly = false;
				}
				f.delete();
			}
		}
		keys = kFC.map(MapMode.READ_WRITE, 0, size * FREE.length);
		keys.load();
		this.values = vRaf.map(MapMode.READ_WRITE, 0, size * 8);
		values.load();
		this.times = tRaf.map(MapMode.READ_WRITE, 0, size * 8);
		times.load();
		if (bgst < 0) {
			SDFSLogger.getLog()
					.info("Hashtable " + path
							+ " did not close correctly. scanning ");
			bgst = this.getBigestKey();

		}
		if (!closedCorrectly)
			this.recreateMap();
		_bpos.seek(0);
		_bpos.writeLong(-1);
		_bpos.close();

		claims = new BitSet(size);
		claims.clear();

		return bgst;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#containsKey(byte[])
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#isClaimed(byte[])
	 */
	@Override
	public boolean isClaimed(byte[] key) throws KeyNotFoundException {
		try {
			this.hashlock.lock();
			int index = index(key);
			if (index >= 0) {
				int pos = (index / FREE.length);
				boolean cl = this.claims.get(pos);
				if (cl)
					return true;
			} else {
				throw new KeyNotFoundException(key);
			}
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#update(byte[], long)
	 */
	@Override
	public boolean update(byte[] key, long value) throws IOException {
		try {
			this.hashlock.lock();
			int pos = this.index(key);
			if (pos == -1) {
				return false;
			} else {
				// keys.position(pos);
				if (value > bgst)
					bgst = value;
				pos = (pos / FREE.length) * 8;
				this.values.position(pos);
				this.values.putLong(value);
				pos = (pos / 8);
				this.claims.set(pos);
				this.mapped.set(pos);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#remove(byte[])
	 */
	@Override
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
				this.values.position(pos);
				long fp = values.getLong();
				ChunkData ck = new ChunkData(fp, key);
				if (ck.setmDelete(true)) {
					fp = fp * -1;
					this.values.position(pos);
					this.values.putLong(fp);
					this.times.position(pos);
					this.times.putLong(0);
					pos = (pos / 8);
					this.claims.clear(pos);
					this.mapped.clear(pos);
					// this.store.position(pos);
					// this.store.put((byte)0);
					return true;
				} else
					return false;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#hashFunc3(int)
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#put(byte[], long)
	 */
	@Override
	public boolean put(byte[] key, long value) throws HashtableFullException {
		try {
			this.hashlock.lock();
			if (this.mapped.cardinality() >= size)
				throw new HashtableFullException(
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
			this.values.position(pos);
			this.values.putLong(value);
			pos = (pos / 8);
			this.claims.set(pos);
			this.mapped.set(pos);
			// this.store.position(pos);
			// this.store.put(storeID);
			return pos > -1 ? true : false;
		} finally {
			this.hashlock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#getEntries()
	 */
	@Override
	public int getEntries() {
		return this.mapped.cardinality();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#get(byte[])
	 */
	@Override
	public long get(byte[] key) {
		return this.get(key, true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#get(byte[], boolean)
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#size()
	 */
	@Override
	public int size() {
		return this.mapped.cardinality();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#close()
	 */
	@Override
	public void close() {
		this.hashlock.lock();
		this.closed = true;
		try {
			this.vRaf.force(true);
			this.vRaf.close();
		} catch (Exception e) {

		}
		try {
			this.kFC.force(true);
			this.kFC.close();
		} catch (Exception e) {

		}
		try {
			this.tRaf.force(true);
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
		AbstractShard b = new FileByteArrayLongMap("/opt/sdfs/hashesaaa",
				10000000, (short) 16);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#claimRecords()
	 */
	@Override
	public synchronized long claimRecords() throws IOException {
		if (this.closed)
			throw new IOException("Hashtable " + this.path + " is close");
		long k = 0;
		try {
			this.iterInit();
			while (iterPos < size) {
				this.hashlock.lock();
				try {
					boolean claimed = claims.get(iterPos);
					claims.clear(iterPos);
					if (claimed) {
						this.mapped.set(iterPos);
						this.times.position(iterPos * 8);
						this.times.putLong(System.currentTimeMillis());
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#sync()
	 */
	@Override
	public void sync() throws SyncFailedException, IOException {
		keys.force();
		vRaf.force(true);
		tRaf.force(true);
		File f = new File(path + ".vmp");
		FileOutputStream fout = new FileOutputStream(f);
		ObjectOutputStream oon = new ObjectOutputStream(fout);
		oon.writeObject(mapped);
		oon.flush();
		oon.close();
		fout.flush();
		fout.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#removeNextOldRecord(long)
	 */

	@Override
	public synchronized long removeNextOldRecord(long time) throws IOException {
		while (iterPos < size) {
			long val = -1;
			this.hashlock.lock();
			try {
				if (this.mapped.get(iterPos)) {
					this.times.position(iterPos * 8);
					long tm = this.times.getLong();
					if (tm < time) {
						boolean claimed = claims.get(iterPos);
						if (!claimed) {
							byte[] key = new byte[FREE.length];
							keys.position(iterPos * FREE.length);
							keys.get(key);
							keys.position(iterPos * FREE.length);
							keys.put(REMOVED);
							this.values.position(iterPos * 8);
							val = this.values.getLong();
							ChunkData ck = new ChunkData(val, key);
							ck.setmDelete(true);
							this.values.position(iterPos * 8);
							this.values.putLong(0);
							this.times.position(iterPos * 8);
							this.times.putLong(0);
							this.claims.clear(iterPos);
							this.mapped.clear(iterPos);
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
