package org.opendedup.sdfs.filestore;

import java.io.File;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.collections.AFByteArrayLongMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.gc.ChunkStoreGCScheduler;
import org.opendedup.util.StringUtils;


/**
 * 
 * @author Sam Silverberg
 * 
 *         The TCHashStore stores data locations within the ChunkStore service.
 *         Dedupe data is stored within a chunkstore but it is referenced within
 *         the TCHashStore for lookup and retrieval. Meta-data associated with
 *         the TCHashStore. The hashstore implements the B-Tree table within the
 *         TokyoCabinet library. Data is indexed by the hash and is stored as a
 *         serialized PersistantDedupChunk. The TCHashStore is located on disk
 *         based on @see com.annesam.sdfs.Main#hashDBStore.
 * 
 *         ChuckStore Service communication flows as follows:
 * 
 *         sdfs client <-TCP-> ClientThread <-> HashChunkService <->TCHashStore
 *         <->AbstractChunkStore
 * 
 */
public class MemoryHashStore implements  AbstractChunkStoreListener {

	// A lookup table for the specific hash store based on the first byte of the
	// hash.
	private static ArrayList<MemoryHashStore> hashStores = new ArrayList<MemoryHashStore>();
	private static long entries;
	AFByteArrayLongMap bdb = null;
	// the name of the hash store. This is usually associate with the first byte
	// of all possible hashes. There should
	// be 256 total hash stores.
	private String name;
	private int hash_id = 0;
	// Lock for hash queries
	private ReentrantLock hashlock = new ReentrantLock();

	private static ReentrantLock clock = new ReentrantLock();
	// The chunk store used to store the actual deduped data;
	private AbstractChunkStore chunkStore = null;
	// Instanciates a FileChunk store that is shared for all instances of
	// hashstores.
	private static AbstractChunkStore fileStore = new FileChunkStore("chunks");
	private static ChunkStoreGCScheduler gcSched = new ChunkStoreGCScheduler();
	private static Logger log = Logger.getLogger("sdfs");
	private boolean closed = true;

	/**
	 * Instantiates the TC hash store.
	 * 
	 * @param name
	 *            the name of the hash store.
	 * @throws IOException
	 */
	public MemoryHashStore(int hash_id) throws IOException {
		this.name = "sdfs-"+hash_id;
		try {
			this.connectDB();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.initChunkStore();
		long rows = this.getRowCount();
		entries = entries + rows;
		log.info(this.getName() + " Row Count : " + rows);
		log.info("Total Entries " + entries);
		hashStores.add(hash_id, this);
		log.info("Added " + this.name);
		this.closed = false;
	}

	/**
	 * 
	 * @return the total number of entries stored in this database
	 * 
	 */
	public static long getEntries() {
		return entries;
	}

	/**
	 * Initiates the chunkstore. It will create a S3 chunk store per HashStore
	 * if AWS is enabled. Otherwise it will use the default ChunkStore @see
	 * FileChunkStore.
	 * 
	 * @throws IOException
	 */
	private void initChunkStore() throws IOException {
		if (Main.AWSChunkStore)
			chunkStore = new S3ChunkStore(this.getName());
		else
			chunkStore = fileStore;
	}

	/**
	 * A static method used to store a hash to the database
	 * 
	 * @param storeName
	 *            the name of the TC hashstore. This is usually the first byte
	 *            of the hash
	 * @param hash
	 *            the md5 or sha hash
	 * @param start
	 *            the start position to store in the byte array. typically this
	 *            is 0
	 * @param len
	 *            the length of the data to be stored
	 * @param data
	 *            the data to be persisted to the chunk store
	 * @param compressed
	 *            whether or not the data has already been compress.
	 * @return returns true if the data was written. Data will not be written if
	 *         the hash already exists in the db.
	 * @throws IOException
	 */
	public static boolean addHash(int storeID, byte[] hash, long start,
			int len, byte[] data, boolean compressed) throws IOException {
		MemoryHashStore store = hashStores.get(storeID);
		if (store == null) {
			clock.lock();
			store = hashStores.get(storeID);
			if (store == null) {
				store = new MemoryHashStore(storeID);
			}
			clock.unlock();
		}
		return store.addHashChunk(new HashChunk(hash, start, len, data,
				compressed));
	}

	/**
	 * A static method used to determine if the hash already exists in the
	 * database
	 * 
	 * @param storeName
	 *            storeName the name of the TC hashstore. This is usually the
	 *            first byte of the hash
	 * @param hash
	 *            the md5 or sha hash to lookup
	 * @return returns true if the hash already exists.
	 * @throws IOException
	 */
	public static boolean hashExists(int storeID, byte[] hash)
			throws IOException {
		MemoryHashStore store = hashStores.get(storeID);
		if (store == null) {
			clock.lock();
			store = hashStores.get(storeID);
			if (store == null) {
				store = new MemoryHashStore(storeID);
			}
			clock.unlock();
		}
		return store.hashExists(hash);
	}

	/**
	 * a static method that claims a hash in the hashstore. Claimed hashes will
	 * not be purged is the claim date falls within the range of time that is
	 * specified. This is currently not implemented
	 * 
	 * @param storeName
	 *            the name of the TC hashstore. This is usually the first byte
	 *            of the hash
	 * @param hash
	 *            the md5 or sha hash to claim
	 * @return returns true if the hash was claimed
	 * @throws IOException
	 */
	public static synchronized boolean claimAllHashes()
			throws IOException {
		log.info("Claiming Hashes for garbage collection ...");
		Iterator<MemoryHashStore> iter = hashStores.iterator();
		boolean success = true;
		while(iter.hasNext()) {
			MemoryHashStore hs = iter.next();
			if(!hs.claimHashes()) {
				log.warning("Hash " + hs.getName() + " was not completed successfully.");
				success = false;
			}
		}
		return success;
	}

	/**
	 * A static method to return a chunk from the hash store.
	 * 
	 * @param storeName
	 *            the name of the TC hashstore. This is usually the first byte
	 *            of the hash
	 * @param hash
	 *            the md5 or sha hash to store
	 * @return a hashchunk or null if the hash is not in the database.
	 * @throws IOException
	 */
	public static HashChunk getHashChunk(int storeID, byte[] hash)
			throws IOException {
		MemoryHashStore store = hashStores.get(storeID);
		if (store == null) {
			clock.lock();
			store = hashStores.get(storeID);
			if (store == null) {
				store = new MemoryHashStore(storeID);
			}
			clock.unlock();
		}
		return store.getHashChunk(hash);
	}

	/**
	 * returns the name of the TCHashStore
	 * 
	 * @return the name of the hash store
	 */
	public String getName() {
		return name;
	}

	/**
	 * 
	 * @return the number of rows from the database
	 */
	public long getRowCount() {

		return bdb.getSize();
	}

	/**
	 *method used to determine if the hash already exists in the database
	 * 
	 * @param hash
	 *            the md5 or sha hash to lookup
	 * @return returns true if the hash already exists.
	 */
	public boolean hashExists(byte[] hash) {
		return this.bdb.containsKey(hash);
	}

	/**
	 * a method that claims a hash in the hashstore. Claimed hashes will not be
	 * purged is the claim date falls within the range of time that is
	 * specified. This is currently not implemented.
	 * 
	 * @param hash
	 *            the md5 or sha hash to claim
	 * @return returns true if the hash was claimed
	 */
	private synchronized boolean claimHashes() {
		try {
			this.bdb.iterInit();
			byte [] key = new byte[Main.hashLength];
			long claims = 0; 
			long start = System.currentTimeMillis();
			while(!this.isClosed()) {
				key = this.bdb.nextClaimedKey(true);
				if(key == null) {
					long duration = (System.currentTimeMillis()-start)/1000;
					log.info("claimed [" + claims + "] hashes in [" + duration + "] seconds");
					return true;
				}
				claims++;
				long pos = this.bdb.get(key);
				this.chunkStore.claimChunk(key, pos);
			}
			return true;
		} catch (Exception e) {
			
			return false;
		} finally {
			// hashlock.unlock();
		}
	}

	/**
	 * The method used to open and connect to the TC database.
	 * 
	 * @throws IOException
	 */

	private void connectDB() throws IOException {
		File directory = new File(Main.hashDBStore + File.separator);
		if (!directory.exists())
			directory.mkdirs();
		File dbf = new File(directory.getPath() + File.separator + "hashstore-"
				+ this.getName());
		int entries = (int) ((Main.chunkStoreAllocationSize / Main.chunkStorePageSize) / 8) + 10000;
		bdb = new AFByteArrayLongMap(entries, (short) Main.hashLength, dbf.getPath());
	}

	/**
	 * A method to return a chunk from the hash store.
	 * 
	 * @param hash
	 *            the md5 or sha hash to store
	 * @return a hashchunk or null if the hash is not in the database.
	 */
	public HashChunk getHashChunk(byte[] hash) throws IOException {
		HashChunk hs = null;
		try {
			long pos = this.bdb.get(hash);
			byte[] data = chunkStore.getChunk(hash, pos, -1);
			hs = new HashChunk(hash, pos, data.length, data, false);
		} catch (Exception e) {
			log.log(Level.SEVERE,"unable to get hash " + StringUtils.getHexString(hash),e);
		} finally {
			// hashlock.unlock();
		}
		return hs;
	}

	public static long evictAll(long age) {
		long i = 0;
		Iterator<MemoryHashStore> iter = hashStores.iterator();
		while (iter.hasNext()) {
			/*
			 * i = i + iter.next().evict(age);
			 */
		}
		return i;
	}

	public long evict(long age) {
		return 0;
	}

	/**
	 * Adds a block of data to the TC hash store and the chunk store.
	 * 
	 * @param chunk
	 *            the chunk to persist
	 * @return true returns true if the data was written. Data will not be
	 *         written if the hash already exists in the db.
	 */
	public boolean addHashChunk(HashChunk chunk) {
		boolean written = false;
		try {
			long start = chunkStore.reserveWritePosition(chunk.getLen());
			if (bdb.put(chunk.getName(), start)) {
				chunkStore.writeChunk(chunk.getName(), chunk.getData(), chunk
						.getLen(), start);
				entries++;
				written = true;
			} else {
				
			}
		} catch (Exception e) {
			if (hashlock.isLocked())
				hashlock.unlock();
			
			log.log(Level.SEVERE,"Unable to commit chunk "
					+ StringUtils.getHexString(chunk.getName()),e);
		} finally {

		}
		return written;
	}

	/**
	 * Closes the hash store. The hash store should always be closed.
	 * 
	 * 
	 */
	public void close() {
		this.closed = true;
		try {
			bdb.close();
			bdb = null;
		} catch (Exception e) {
			
		}
	}
	
	public boolean isClosed(){
		return this.closed;
	}

	/**
	 * closes all HashStores
	 */
	public static void closeAll() {
		gcSched.stopSchedules();
		Iterator<MemoryHashStore> iter = hashStores.iterator();
		while (iter.hasNext()) {
			try {
				iter.next().close();
			} catch (Exception e) {

			}
		}
		
	}

	@Override
	public void chunkMovedEvent(ChunkEvent e) throws IOException {
			bdb.update(e.getHash(), e.getNewLocation());
		
	}

	@Override
	public void chunkRemovedEvent(ChunkEvent e) throws IOException {
			bdb.remove(e.getHash());
	}

	@Override
	public int getID() {
		return this.hash_id;
	}

}
