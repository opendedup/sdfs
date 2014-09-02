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
import java.nio.channels.FileChannel;
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
import org.opendedup.util.LargeBloomFilter;


public class FCByteArrayLongMap implements AbstractShard {
	// MappedByteBuffer keys = null;
	private int size = 0;
	private String path = null;
	private FileChannel kFC = null;
	private RandomAccessFile vRaf = null;
	private RandomAccessFile tRaf = null;
	private ReentrantLock hashlock = new ReentrantLock();
	public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	public static byte[] REMOVED = new byte[HashFunctionPool.hashLength];
	private int iterPos = 0;
	private boolean closed = false;
	private BitSet claims = null;
	private BitSet mapped = null;
	private long bgst = 0;

	static {
		FREE = new byte[HashFunctionPool.hashLength];
		REMOVED = new byte[HashFunctionPool.hashLength];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
	}

	public FCByteArrayLongMap(String path, int size, short arraySize)
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

	public byte[] nextKey() throws IOException {
		while (iterPos < size) {
			this.hashlock.lock();
			try {
				if (this.mapped.get(iterPos)) {
					byte[] key = new byte[FREE.length];
					kFC.read(ByteBuffer.wrap(key), iterPos * FREE.length);
					iterPos++;
					if (!Arrays.equals(key, FREE)
							&& !Arrays.equals(key, REMOVED)) {
						this.mapped.set(iterPos);
						return key;
					}
				} else {
					iterPos++;
				}
			} finally {
				this.hashlock.unlock();
			}

		}
		return null;
	}

	public byte[] nextClaimedKey(boolean clearClaim) {
		while (iterPos < size) {
			byte[] key = new byte[FREE.length];
			this.hashlock.lock();
			try {
				kFC.read(ByteBuffer.wrap(key), iterPos * FREE.length);
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
						this.mapped.set(iterPos);
						claims.clear(iterPos);
						this.tRaf.seek(iterPos * 8);
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

	private void recreateMap() throws IOException {
		mapped = new BitSet(size);
		this.iterInit();
		byte[] key = this.nextKey();
		while (key != null)
			key = this.nextKey();
		SDFSLogger.getLog().warn("Recovered Hashmap " + this.path);
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
				oon.close();
				f.delete();
			}
		}

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
	 * @throws KeyNotFoundException
	 * @throws IOException
	 */
	public boolean isClaimed(byte[] key) throws KeyNotFoundException,
			IOException {
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
				this.vRaf.seek(pos);
				this.vRaf.writeLong(value);
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
				kFC.write(ByteBuffer.wrap(REMOVED), pos);
				pos = (pos / FREE.length) * 8;
				this.vRaf.seek(pos);
				long fp = vRaf.readLong();
				ChunkData ck = new ChunkData(fp, key);
				if (ck.setmDelete(true)) {
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
		int index = this.hashFunc1(hash) * FREE.length;
		byte[] cur = new byte[FREE.length];
		kFC.read(ByteBuffer.wrap(cur), index);
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
	 * @throws IOException
	 */
	private int indexRehashed(byte[] key, int index, int hash, byte[] cur)
			throws IOException {

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
			kFC.read(ByteBuffer.wrap(cur), index);
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

	protected int insertionIndex(byte[] key) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int index = this.hashFunc1(hash) * FREE.length;
		byte[] cur = new byte[FREE.length];
		kFC.read(ByteBuffer.wrap(cur), index);

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
	 * @throws IOException
	 */
	private int insertKeyRehash(byte[] key, int index, int hash, byte[] cur)
			throws IOException {
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
			kFC.read(ByteBuffer.wrap(cur), index);

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
	
	public boolean put(ChunkData cm) {
		try {
			byte [] key = cm.getHash();
			this.hashlock.lock();
			if (this.mapped.cardinality() >= size)
				throw new IOException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			int pos = this.insertionIndex(key);
			if (pos < 0) {
				int npos = -pos -1;
				npos = (npos / FREE.length);
				this.claims.set(npos);
				return false;
			}
			this.kFC.write(ByteBuffer.wrap(key), pos);
			if (!cm.recoverd) {
				cm.persistData(true);
			}
			if (cm.getcPos() > bgst)
				bgst = cm.getcPos();
			pos = (pos / FREE.length) * 8;
			this.vRaf.seek(pos);
			this.vRaf.writeLong(cm.getcPos());
			pos = (pos / 8);
			this.claims.set(pos);
			this.mapped.set(pos);
			// this.store.position(pos);
			// this.store.put(storeID);
			return pos > -1 ? true : false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error inserting record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	public boolean put(byte[] key, long value) {
		try {
			this.hashlock.lock();
			if (this.mapped.cardinality() >= size)
				throw new IOException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			int pos = this.insertionIndex(key);
			if (pos < 0) {
				int npos = -pos -1;
				npos = (npos / FREE.length);
				this.claims.set(npos);
				return false;
			}
			this.kFC.write(ByteBuffer.wrap(key), pos);

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
			return pos > -1 ? true : false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error inserting record", e);
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}

	public int getEntries() {
		return this.mapped.cardinality();
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
		FCByteArrayLongMap b = new FCByteArrayLongMap("/opt/sdfs/hashesaaa",
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

	@Override
	public long claimRecords(LargeBloomFilter bf) throws IOException {
		this.iterInit();
		byte[] key = this.nextKey();
		while (key != null) {
			if (bf.mightContain(key)) {
				this.hashlock.lock();
				this.claims.set(this.iterPos - 1);
				this.hashlock.unlock();
			}
			key = this.nextKey();
		}
		this.iterInit();
		return this.claimRecords();
	}

	public void sync() throws SyncFailedException, IOException {
		this.kFC.force(false);
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
				if (this.mapped.get(iterPos)) {
					this.tRaf.seek(iterPos * 8);
					long tm = this.tRaf.readLong();
					if (tm < time) {
						boolean claimed = claims.get(iterPos);
						if (!claimed) {
							byte[] key = new byte[FREE.length];
							kFC.read(ByteBuffer.wrap(key), iterPos
									* FREE.length);
							this.kFC.write(ByteBuffer.wrap(REMOVED), iterPos
									* FREE.length);
							this.vRaf.seek(iterPos * 8);
							val = this.vRaf.readLong();
							ChunkData ck = new ChunkData(val, key);
							ck.setmDelete(true);
							this.vRaf.seek(iterPos * 8);
							this.vRaf.writeLong(0);
							this.tRaf.seek(iterPos * 8);
							this.tRaf.writeLong(0);
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
