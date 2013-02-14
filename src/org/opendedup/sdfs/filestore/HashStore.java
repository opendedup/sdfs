package org.opendedup.sdfs.filestore;

import java.io.File;



import java.io.IOException;
import java.util.Arrays;

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HashChunkServiceInterface;
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
public class HashStore {

	// A lookup table for the specific hash store based on the first byte of the
	// hash.
	public AbstractHashesMap bdb = null;
	HashChunkServiceInterface hcs = null;
	// the name of the hash store. This is usually associate with the first byte
	// of all possible hashes. There should
	// be 256 total hash stores.
	private String name;
	// Lock for hash queries
	// private ReentrantLock cacheLock = new ReentrantLock();
	

	// The chunk store used to store the actual deduped data;
	// private AbstractChunkStore chunkStore = null;
	// Instanciates a FileChunk store that is shared for all instances of
	// hashstores.

	// private static ChunkStoreGCScheduler gcSched = new
	// ChunkStoreGCScheduler();
	private boolean closed = true;
	private static byte[] blankHash = null;
	private static byte[] blankData = null;
	static {
		blankData = new byte[Main.chunkStorePageSize];
		try {
			blankHash = HashFunctionPool.getHashEngine().getHash(blankData);
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to hash blank hash", e);
		}
	}

	/**
	 * Instantiates the TC hash store.
	 * 
	 * @param name
	 *            the name of the hash store.
	 * @throws IOException
	 */
	public HashStore(HashChunkServiceInterface hcs) throws IOException {
		this.name = "sdfs";
		this.hcs = hcs;
		try {
			this.connectDB();
			hcs.getChuckStore().setSize(bdb.endStartingPosition());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// this.initChunkStore();
		SDFSLogger.getLog().info(
				"Cache Size = " + Main.chunkStorePageSize
						);
		SDFSLogger.getLog().info("Total Entries " + +bdb.getSize());
		SDFSLogger.getLog().info("Added " + this.name);
		this.closed = false;
	}

	/**
	 * 
	 * @return the total number of entries stored in this database
	 * 
	 */
	public long getEntries() {
		return bdb.getSize();
	}

	public long getMaxEntries() {
		return this.bdb.getMaxSize();
	}

	/**
	 * 
	 * @return the total number of free blocks available for re-use
	 * 
	 */
	public long getFreeBlocks() {
		return bdb.getFreeBlocks();
	}

	/**
	 * Initiates the chunkstore. It will create a S3 chunk store per HashStore
	 * if AWS is enabled. Otherwise it will use the default ChunkStore @see
	 * FileChunkStore.
	 * 
	 * @throws IOException
	 */
	/*
	 * private void initChunkStore() throws IOException { if
	 * (Main.AWSChunkStore) chunkStore = new S3ChunkStore(this.getName()); else
	 * chunkStore = fileStore; }
	 */

	/**
	 * returns the name of the TCHashStore
	 * 
	 * @return the name of the hash store
	 */
	public String getName() {
		return name;
	}

	/**
	 * method used to determine if the hash already exists in the database
	 * 
	 * @param hash
	 *            the md5 or sha hash to lookup
	 * @return returns true if the hash already exists.
	 * @throws IOException
	 */
	public boolean hashExists(byte[] hash) throws IOException {
		return this.bdb.containsKey(hash);
	}

	/**
	 * The method used to open and connect to the TC database.
	 * 
	 * @throws IOException
	 * @throws HashtableFullException
	 */
	private void connectDB() throws IOException, HashtableFullException {
		File directory = new File(Main.hashDBStore + File.separator);
		if (!directory.exists())
			directory.mkdirs();
		File dbf = new File(directory.getPath() + File.separator + "hashstore-"
				+ this.getName());
		long entries = ((Main.chunkStoreAllocationSize / Main.chunkStorePageSize)) + 8000;
		try {
			SDFSLogger.getLog().info("Loading hashdb class " + Main.hashesDBClass);
			bdb = (AbstractHashesMap) Class.forName(Main.hashesDBClass)
					.newInstance();
			bdb.init(entries, dbf.getPath());
		} catch (InstantiationException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			System.exit(-1);
		} catch (IllegalAccessException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			System.exit(-1);
		} catch (ClassNotFoundException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			System.exit(-1);
		} catch (IOException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			System.exit(-1);
		}
		
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
		// String hStr = StringUtils.getHexString(hash);
		/*
		 * hs = this.cacheBuffers.get(hStr); if (hs != null) { return hs; }
		 * 
		 * 
		 * if (this.readingBuffers.containsKey(hStr)) { int t = 0; while (t <
		 * Main.chunkStoreDirtyCacheTimeout) { try { Thread.sleep(1); hs =
		 * this.cacheBuffers.get(hStr); if (hs != null) { return hs; } } catch
		 * (Exception e) {
		 * 
		 * } t++; } } else { if(this.readingBuffers.size() < mapSize)
		 * this.readingBuffers.put(hStr, hs); }
		 */
		try {
			byte[] data = bdb.getData(hash);
			if (data == null && Arrays.equals(hash, blankHash)) {
				hs = new HashChunk(hash, 0, blankData.length, blankData, false);
			}
			hs = new HashChunk(hash, 0, data.length, data, false);
			// this.cacheBuffers.put(hStr, hs);

		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"unable to get hash " + StringUtils.getHexString(hash), e);
		} finally {
			// this.readingBuffers.remove(hStr);
		}
		return hs;
	}

	public void processHashClaims(SDFSEvent evt) throws IOException {
		this.bdb.claimRecords(evt);
	}

	public long evictChunks(long time, boolean forceRun,SDFSEvent evt) throws IOException {
		return this.bdb.removeRecords(time, forceRun,evt);
	}

	/**
	 * Adds a block of data to the TC hash store and the chunk store.
	 * 
	 * @param chunk
	 *            the chunk to persist
	 * @return true returns true if the data was written. Data will not be
	 *         written if the hash already exists in the db.
	 * @throws IOException
	 * @throws HashtableFullException
	 */
	public boolean addHashChunk(HashChunk chunk) throws IOException,
			HashtableFullException {
		boolean written = false;
		if (!bdb.containsKey(chunk.getName())) {
			try {
				// long start = chunkStore.reserveWritePosition(chunk.getLen());
				ChunkData cm = new ChunkData(chunk.getName(),
						Main.chunkStorePageSize, chunk.getData());
				written = bdb.put(cm);

			} catch (IOException e) {
				SDFSLogger.getLog().fatal(
						"Unable to commit chunk "
								+ StringUtils.getHexString(chunk.getName()), e);
				throw e;
			} catch (HashtableFullException e) {
				SDFSLogger.getLog().fatal(
						"Unable to commit chunk "
								+ StringUtils.getHexString(chunk.getName()), e);
				throw e;
			}

			finally {

			}
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

	public boolean isClosed() {
		return this.closed;
	}

}
