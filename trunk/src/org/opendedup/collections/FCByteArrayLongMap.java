package org.opendedup.collections;

import java.io.IOException;



import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.Tiger16HashEngine;
import org.opendedup.util.SDFSLogger;

public class FCByteArrayLongMap {
	ByteBuffer claims = null;
	private int size = 0;
	private int entries = 0;
	private String path = null;
	private FileChannel kFC = null;
	private FileChannel vFC = null;
	private RandomAccessFile tRaf = null;
	private ReentrantLock hashlock = new ReentrantLock();
	public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	public static byte[] REMOVED = new byte[HashFunctionPool.hashLength];
	private int iterPos = 0;
	private boolean closed = false;
	ByteBuffer valueBuf = ByteBuffer.allocateDirect(8);
	ByteBuffer keyBuf = ByteBuffer.wrap(new byte[HashFunctionPool.hashLength]);
	BitSet map = null;
	long bgst = 0;

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
		map = new BitSet(size);
		map.clear();
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
			try{
			this.keyBuf.position(0);
			this.kFC.read(this.keyBuf, iterPos * FREE.length);
			byte [] key = keyBuf.array();
			
			if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
				return key;
			}
			}catch(IOException e){
				SDFSLogger.getLog().error("Unable to iterate through hashes",e);
			}
			finally {
				iterPos++;
				this.hashlock.unlock();
			}
		}
		return null;
	}

	public byte[] nextClaimedKey(boolean clearClaim) {
		while (iterPos < size) {

			this.hashlock.lock();
			try {
				this.keyBuf.position(0);
				this.kFC.read(this.keyBuf, iterPos * FREE.length);
				byte [] key = keyBuf.array();
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

	public long nextClaimedValue(boolean clearClaim) throws IOException {
		
		while (iterPos < size) {
			long val = -1;
			this.hashlock.lock();
			try {
				valueBuf.position(0);
				vFC.read(valueBuf,iterPos * 8);
				valueBuf.position(0);
				val = valueBuf.getLong();
				if (val >= 0) {
					claims.position(iterPos);
					byte claimed = claims.get();
					if (clearClaim) {
						claims.position(iterPos);
						claims.put((byte) 0);
						this.tRaf.seek(iterPos);
						this.tRaf.writeLong(System.currentTimeMillis());
					}
					if (claimed == 1)
						return val;
				}
				
			} finally {
				iterPos++;
				this.hashlock.unlock();
			}
		}
		return -1;
	}
	
	public void sync() throws IOException{
		this.kFC.force(true);
		this.vFC.force(true);
		this.tRaf.getFD().sync();
	}
	
	public long getBigestKey() throws IOException {
		this.iterInit();
		long _bgst = 0;
		try {
			this.hashlock.lock();
			while (iterPos < size) {
				long val = -1;
				valueBuf.position(0);
				vFC.read(valueBuf, iterPos * 8);
				valueBuf.position(0);
				val = valueBuf.getLong();
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
		RandomAccessFile kRaf = new RandomAccessFile(path + ".keys", "rw");
		kRaf.setLength(size * FREE.length);
		RandomAccessFile vRaf = new RandomAccessFile(path + ".pos", "rw");
		vRaf.setLength(size * 8);
		tRaf = new RandomAccessFile(path + ".ctimes", "rw");
		tRaf.setLength(size * 8);
		this.kFC = kRaf.getChannel();
		this.vFC = vRaf.getChannel();
		RandomAccessFile _bpos = new RandomAccessFile(path + ".bpos", "rw");
		_bpos.setLength(8);
		bgst = _bpos.readLong();
		if(bgst < 0) {
			SDFSLogger.getLog().info("Hashtable " + path + " did not close correctly. scanning ");
			bgst = this.getBigestKey();
		}
		_bpos.seek(0);
		_bpos.writeLong(-1);
		_bpos.close();
		claims = ByteBuffer.allocateDirect(size);
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
				this.claims.position(pos);
				this.claims.put((byte) 1);
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
				valueBuf.position(0);
				valueBuf.putLong(value);
				valueBuf.position(0);
				if(value > bgst)
					bgst = value;
				pos = (pos / FREE.length) * 8;
				this.vFC.write(valueBuf,pos);
				pos = (pos / 8);
				this.claims.position(pos);
				this.claims.put((byte) 1);
				this.map.set(pos);
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
			this.claims.position(pos / FREE.length);
			byte claimed = this.claims.get();
			if (pos == -1) {
				return false;
			} else if (claimed == 1) {
				return false;
			} else {
				this.keyBuf.position(0);
				this.keyBuf.put(REMOVED);
				this.keyBuf.position(0);
				this.kFC.write(this.keyBuf, pos);
				valueBuf.position(0);
				valueBuf.putLong(-1);
				valueBuf.position(0);
				
				pos = (pos / FREE.length) * 8;
				this.vFC.write(valueBuf,pos);
				this.tRaf.seek(pos);
				this.tRaf.write(-1);
				pos = (pos / 8);
				this.claims.position(pos);
				this.claims.put((byte) 0);
				
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
	 * @throws IOException 
	 */
	protected int index(byte[] key) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int bindex = this.hashFunc1(hash);
		int index = bindex * FREE.length;
		// int stepSize = hashFunc2(hash);
		if(!map.get(bindex))
			return -1;
		this.keyBuf.position(0);
		this.kFC.read(keyBuf, index);
		byte[] cur =keyBuf.array();

		if (Arrays.equals(cur, key)) {
			return index;
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
				bindex = index/FREE.length;
				if(!map.get(bindex))
					return -1;
				this.keyBuf.position(0);
				this.kFC.read(keyBuf, index);
				cur =keyBuf.array();
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
	 * @throws IOException 
	 */
	protected int insertionIndex(byte[] key) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int bindex = this.hashFunc1(hash);
		int index = bindex * FREE.length;
		// int stepSize = hashFunc2(hash);
		this.keyBuf.position(0);
		this.kFC.read(keyBuf, index);
		byte[] cur =keyBuf.array();

		if (!map.get(bindex)) {
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
					bindex = index/FREE.length;
					if(!map.get(bindex))
						return index;
					this.keyBuf.position(0);
					this.kFC.read(keyBuf, index);
					cur =keyBuf.array();
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
					bindex = index/FREE.length;
					if(!map.get(bindex))
						return index;
					this.keyBuf.position(0);
					this.kFC.read(keyBuf, index);
					cur =keyBuf.array();
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
			this.kFC.write(ByteBuffer.wrap(key),pos);
			valueBuf.position(0);
			valueBuf.putLong(value);
			valueBuf.position(0);
			pos = (pos / FREE.length) * 8;
			this.vFC.write(valueBuf,pos);
			pos = (pos / 8);
			this.claims.position(pos);
			this.claims.put((byte) 1);
			this.map.set(pos);
			if(value > bgst)
				bgst = value;
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
				valueBuf.position(0);
				pos = (pos / FREE.length) * 8;
				this.vFC.read(valueBuf,pos);
				valueBuf.position(0);
				long val = valueBuf.getLong();
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
			this.hashlock.unlock();
		}

	}

	public int size() {
		return this.size;
	}

	public void close() {
		this.hashlock.lock();
		this.closed = true;
		try {
			this.vFC.force(true);
			this.vFC.close();
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
		FCByteArrayLongMap b = new FCByteArrayLongMap(
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
			long val = 0;

			while (val != -1 && !this.closed) {
				k++;
				if (k > 310) {
					k = 0;
					try {
					} catch (Exception e) {
					}
				}
				try {
					val = this.nextClaimedValue(true);
				} catch (Exception e) {
					SDFSLogger.getLog().warn(
							"Unable to get claimed value for map " + this.path,
							e);
				}

			}
		} catch (NullPointerException e) {

		}
		return k;
	}

	public synchronized long removeNextOldRecord(long time)
			throws IOException {
		while (iterPos < size) {
			long val = -1;
			this.hashlock.lock();
			try {
				valueBuf.position(0);
				this.vFC.read(valueBuf,iterPos * 8);
				valueBuf.position(0);
				val = valueBuf.getLong();
				if (val >= 0) {
						this.tRaf.seek(iterPos);
						long tm = this.tRaf.readLong();
					if (tm < time && tm > 0) {
						int pos = iterPos / 8;
						this.claims.position(pos);
						byte claimed = this.claims.get();
						if (claimed == 0) {
							pos = pos * FREE.length;
							this.kFC.write(ByteBuffer.wrap(REMOVED),pos);
							pos = (pos / FREE.length) * 8;
							valueBuf.position(0);
							valueBuf.putLong(-1);
							valueBuf.position(0);
							this.vFC.write(valueBuf,pos);
							pos = (pos / 8);
							this.claims.position(pos);
							this.claims.put((byte) 0);
							this.tRaf.seek(pos);
							this.tRaf.write(-1);
							// this.store.position(pos);
							// this.store.put((byte)0);
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
