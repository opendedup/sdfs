package org.opendedup.collections;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.util.NextPrime;
import org.opendedup.util.StringUtils;


public class SimpleByteArrayLongMap {
	// MappedByteBuffer keys = null;
	private int size = 0;
	private String path = null;
	private FileChannel kFC = null;
	RandomAccessFile rf = null;
	private ReentrantLock hashlock = new ReentrantLock();
	public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	transient protected static final int EL = HashFunctionPool.hashLength + 4;
	transient private static final int VP = HashFunctionPool.hashLength;
	private int iterPos = 0;
	private int currentSz = 0;
	
	static {
		FREE = new byte[HashFunctionPool.hashLength];
		Arrays.fill(FREE, (byte) 0);
	}

	public SimpleByteArrayLongMap(String path, int sz)
			throws IOException {
		this.size = NextPrime.getNextPrimeI(sz);
		this.path = path;
		this.setUp();
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
		this.hashlock.lock();
		try {
			return this.currentSz;
		} finally {
			this.hashlock.unlock();
		}
	}

	public KeyValuePair next() throws IOException {
		while (iterPos < this.kFC.size()) {
			this.hashlock.lock();
			try {
				if(iterPos < this.kFC.size()) {
					byte[] key = new byte[FREE.length];
					this.vb.position(0);
					kFC.read(vb, iterPos);
					vb.position(0);
					iterPos = iterPos + EL;
					vb.get(key);
					if (!Arrays.equals(key, FREE)
							) {
						return new KeyValuePair(key,vb.getInt());
					}
				} else {
					iterPos = iterPos + EL;
				}
			} finally {
				this.hashlock.unlock();
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
		if(new File(path).exists())
			size =(int)new File(path).length()/EL;
		rf = new RandomAccessFile(path,"rw");
		rf.setLength(EL*size);
		this.kFC = rf.getChannel();
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
		try {
			this.hashlock.lock();
			if(this.closed)
				throw new MapClosedException();
			int index = index(key);
			if (index >= 0) {
				return true;
			}
			return false;
		} catch(MapClosedException e) {
			throw e;
		}	catch (Exception e) {
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
		int index = this.hashFunc1(hash) * EL;
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
		int length = size * EL;
		int probe = (1 + (hash % (size - 2))) * EL;

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
	
	boolean closed= false;
	
	public void vanish() {
		this.hashlock.lock();
		try {
			this.close();
		}catch(Exception e) {
			
		}
		try {
			File f = new File(this.path);
			f.delete();
		}catch(Exception e) {}
		this.hashlock.unlock();
	}

	protected int insertionIndex(byte[] key) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		int index = this.hashFunc1(hash) * EL;
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
			kFC.read(ByteBuffer.wrap(cur), index);

			// A FREE slot stops the search
			if (Arrays.equals(cur, FREE)) {
				
					return index;
			}

			if (Arrays.equals(cur, key)) {
				return -index - 1;
			}

			// Detect loop
		} while (index != loopIndex);

		// We inspected all reachable slots and did not find a FREE one
		// If we found a REMOVED slot we return the first one found
		

		// Can a resizing strategy be found that resizes the set?
		throw new IllegalStateException(
				"No free or removed slots available. Key set full?!!");
	}
	
	ByteBuffer vb = ByteBuffer.allocateDirect(EL);
	public boolean put(byte[] key, int value) throws MapClosedException{
		this.hashlock.lock();
		try {
			if(this.closed)
				throw new MapClosedException();
			int pos = this.insertionIndex(key);
			if (pos < 0) {
				int npos = -pos -1;
				npos = (npos / EL);
				return false;
			}
			vb.position(0);
			vb.put(key);
			vb.putInt(value);
			vb.position(0);
			this.kFC.write(vb, pos);
			vb.position(0);
			this.currentSz++;
			return pos > -1 ? true : false;
		} catch(MapClosedException e){
			throw e;
		}catch (Exception e) {
			SDFSLogger.getLog().fatal("error inserting record", e);
			e.printStackTrace();
			return false;
		} finally {
			this.hashlock.unlock();
		}
	}
		
	public int get(byte[] key) throws MapClosedException {
		try {
			this.hashlock.lock();
			if(this.closed)
				throw new MapClosedException();
			if (key == null)
				return -1;
			int pos = this.index(key);
			if (pos == -1) {
				return -1;
			} else {
				vb.position(0);
				this.kFC.read(vb,pos);
				vb.position(VP);
				int val = vb.getInt();
				vb.position(0);
				return val;

			}
		} catch(MapClosedException e){
			throw e;
		}catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return -1;
		} finally {
			this.hashlock.unlock();
		}

	}

	public void close() {
		this.hashlock.lock();
		this.closed = true;
		try {
			this.kFC.close();
		} catch (Exception e) {

		}
		try {
			this.rf.close();
		} catch (Exception e) {

		}
		this.hashlock.unlock();
		SDFSLogger.getLog().debug("closed " + this.path);
	}

	public static void main(String[] args) throws Exception {
		SimpleByteArrayLongMap b = new SimpleByteArrayLongMap("/home/samsilverberg/staging/outgoing/-5355749298482906702.map",
				10000000);
		b.iterInit();
		KeyValuePair p = b.next();
		int i = 0;
		while(p != null) {
			i++;
			System.out.println("key=" + StringUtils.getHexString(p.key) + " value=" +p.value);
			p = b.next();
			
		}
		System.out.println("sz="+i);
		
		/*
		Random rnd = new Random();
		byte[] hash = null;
		int val = -33;
		byte[] hash1 = null;
		int val1 = -33;
		for (int i = 0; i < 60000; i++) {
			hash = new byte[16];
			rnd.nextBytes(hash);
			val = rnd.nextInt();
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
			KeyValuePair p =  b.next();
			if(p == null)
				key = null;
			else {
			key = p.key;
			if (Arrays.equals(key, hash1))
				System.out.println("found it! at " + vals);
			vals++;
			}
		}
		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms " + vals);
		b.iterInit();
		key = new byte[16];
		start = System.currentTimeMillis();
		vals = 0;
		while (key != null) {
			KeyValuePair p =  b.next();
			if(p == null)
				key = null;
			else {
			key = p.key;
			int _val = p.value;
			if (Arrays.equals(key, hash1))
				System.out.println("found it! at " + vals);
			int cval = b.get(key);
			if(cval !=_val)
				System.out.println("poop " + cval + " " +_val);
			vals++;
			}
		}
		b.vanish();
		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms " + vals);
				*/
	}

	

	public void sync() throws SyncFailedException, IOException {
		this.kFC.force(false);
		
	}
	
	public static class KeyValuePair {
		int value;
		byte [] key;
		protected KeyValuePair(byte [] key, int value) {
			this.key = key;
			this.value = value;
		}
		
		public byte [] getKey() {
			return this.key;
		}
		
		public int getValue() {
			return this.value;
		}
	}
}
