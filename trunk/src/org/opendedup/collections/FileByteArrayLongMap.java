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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.util.LargeBloomFilter;


public class FileByteArrayLongMap implements AbstractShard {
	MappedByteBuffer keys = null;
	MappedByteBuffer values = null;
	String fn = null;
	private int size = 0;
	private String path = null;
	private FileChannel kFC = null;
	private FileChannel vRaf = null;
	private ReentrantLock hashlock = new ReentrantLock();
	public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	public static byte[] REMOVED = new byte[HashFunctionPool.hashLength];
	private int iterPos = 0;
	private boolean closed = false;
	private BitSet claims = null;
	private BitSet mapped = null;
	private AtomicInteger sz = new AtomicInteger(0);
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
			this.hashlock.lock();
			try {
				byte[] key = new byte[FREE.length];
				keys.position(iterPos * FREE.length);
				keys.get(key);
				iterPos++;
				if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
					this.mapped.set(iterPos - 1);
					return key;
				} else {
					this.mapped.clear(iterPos - 1);
				}
			} finally {
				this.hashlock.unlock();
			}

		}
		return null;
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
		this.fn = new File(path + ".keys").getPath();
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
				try {
					FileInputStream fin = new FileInputStream(f);
					ObjectInputStream oon = new ObjectInputStream(fin);

					mapped = (BitSet) oon.readObject();
					oon.close();
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
		if (bgst < 0) {
			SDFSLogger.getLog()
					.info("Hashtable " + path
							+ " did not close correctly. scanning ");
			bgst = this.getBigestKey();

		}
		if (!closedCorrectly)
			this.recreateMap();
		this.sz.set(this.mapped.cardinality());
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
					pos = (pos / 8);
					this.claims.clear(pos);
					this.mapped.clear(pos);
					this.sz.decrementAndGet();
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
	
	public boolean put(byte[] key, long value) throws HashtableFullException {
		try {
			this.hashlock.lock();
			if (this.sz.get() >= size)
				throw new HashtableFullException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			int pos = this.insertionIndex(key);
			if (pos < 0) {
				int npos = -pos -1;
				npos = (npos / FREE.length);
				this.claims.set(npos);
				return false;
			}
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
			this.sz.incrementAndGet();
			// this.store.position(pos);
			// this.store.put(storeID);
			return pos > -1 ? true : false;
		} finally {
			this.hashlock.unlock();
		}
	}
	
	public boolean put(ChunkData cm) throws HashtableFullException, IOException {
		try {
			byte [] key = cm.getHash();
			this.hashlock.lock();
			if (this.sz.get() >= size)
				throw new HashtableFullException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			int pos = this.insertionIndex(key);
			if (pos < 0) {
				int npos = -pos -1;
				npos = (npos / FREE.length);
				this.claims.set(npos);
				return false;
			}
			this.keys.position(pos);
			this.keys.put(key);
			if (!cm.recoverd) {
				
				cm.persistData(true);
			}
			if (cm.getcPos() > bgst)
				bgst = cm.getcPos();
			pos = (pos / FREE.length) * 8;
			this.values.position(pos);
			this.values.putLong(cm.getcPos());
			pos = (pos / 8);
			this.claims.set(pos);
			this.mapped.set(pos);
			this.sz.incrementAndGet();
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
		return this.sz.get();
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
		return this.sz.get();
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
	public long claimRecords(LargeBloomFilter nbf) throws IOException {
		this.iterInit();
		long sz = 0;
		while (iterPos < size) {
			this.hashlock.lock();
			try {
				byte[] key = new byte[FREE.length];
				keys.position(iterPos * FREE.length);
				keys.get(key);
				if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
					if (!nbf.mightContain(key)
							&& !this.claims.get(iterPos)) {
						keys.position(iterPos * FREE.length);
						keys.put(REMOVED);
						this.values.position(iterPos * 8);
						long val = this.values.getLong();
						ChunkData ck = new ChunkData(val, key);
						ck.setmDelete(true);
						this.values.position(iterPos * 8);
						this.values.putLong(0);
						this.mapped.clear(iterPos);
						this.sz.decrementAndGet();
						
						sz++;
					} else {
						this.mapped.set(iterPos);
					}
					this.claims.clear(iterPos);
				}

			} finally {
				iterPos++;
				this.hashlock.unlock();
			}
		}
		return sz;
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
		File f = new File(path + ".vmp");
		FileOutputStream fout = new FileOutputStream(f);
		ObjectOutputStream oon = new ObjectOutputStream(fout);
		oon.writeObject(mapped);
		oon.flush();
		oon.close();
		fout.flush();
		fout.close();
	}
}
