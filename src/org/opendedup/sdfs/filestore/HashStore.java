package org.opendedup.sdfs.filestore;

import java.io.File;
import java.io.UnsupportedEncodingException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.collections.AFByteArrayLongMap;
import org.opendedup.collections.CSByteArrayLongMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.gc.ChunkStoreGCScheduler;
import org.opendedup.util.HashFunctions;
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
	private long entries;
	CSByteArrayLongMap bdb = null;
	// the name of the hash store. This is usually associate with the first byte
	// of all possible hashes. There should
	// be 256 total hash stores.
	private String name;
	// Lock for hash queries
	private ReentrantLock hashlock = new ReentrantLock();

	private static ReentrantLock clock = new ReentrantLock();
	// The chunk store used to store the actual deduped data;
	//private AbstractChunkStore chunkStore = null;
	// Instanciates a FileChunk store that is shared for all instances of
	// hashstores.
	
	// private static ChunkStoreGCScheduler gcSched = new
	// ChunkStoreGCScheduler();
	private static Logger log = Logger.getLogger("sdfs");
	private boolean closed = true;
	private static byte [] blankHash = null;
	private static byte [] blankData = null;
	static {
		blankData = new byte [Main.chunkStorePageSize];
		try {
			blankHash = HashFunctions.getTigerHashBytes(blankData);
		} catch (NoSuchAlgorithmException e) {
			log.log(Level.SEVERE,"unable to hash blank hash",e);
		} catch (UnsupportedEncodingException e) {
			log.log(Level.SEVERE,"unable to hash blank hash",e);
		} catch (NoSuchProviderException e) {
			log.log(Level.SEVERE,"unable to hash blank hash",e);
		}
	}

	/**
	 * Instantiates the TC hash store.
	 * 
	 * @param name
	 *            the name of the hash store.
	 * @throws IOException
	 */
	public HashStore() throws IOException {
		this.name = "sdfs";
		try {
			this.connectDB();
		} catch (Exception e) {
			e.printStackTrace();
		}
		//this.initChunkStore();
		long rows = this.getRowCount();
		entries = entries + rows;
		log.info(this.getName() + " Row Count : " + rows);
		log.info("Total Entries " + entries);
		log.info("Added " + this.name);
		this.closed = false;
	}

	/**
	 * 
	 * @return the total number of entries stored in this database
	 * 
	 */
	public long getEntries() {
		return entries;
	}

	/**
	 * Initiates the chunkstore. It will create a S3 chunk store per HashStore
	 * if AWS is enabled. Otherwise it will use the default ChunkStore @see
	 * FileChunkStore.
	 * 
	 * @throws IOException
	 */
	/*
	private void initChunkStore() throws IOException {
		if (Main.AWSChunkStore)
			chunkStore = new S3ChunkStore(this.getName());
		else
			chunkStore = fileStore;
	}*/

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
	 * @throws IOException
	 */
	public boolean hashExists(byte[] hash) throws IOException {
		return this.bdb.containsKey(hash);
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
		long entries = ((Main.chunkStoreAllocationSize / (long) Main.chunkStorePageSize)) + 8000;
		bdb = new CSByteArrayLongMap(entries, (short) Main.hashLength, dbf
				.getPath());
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
			byte[] data = bdb.getData(hash);
			if(data == null && Arrays.equals(hash,blankHash)) {
				log.info("found blank data request");
				hs = new HashChunk(hash, 0, blankData.length, blankData, false);
			}
			hs = new HashChunk(hash, 0, data.length, data, false);
		} catch (Exception e) {
			log.log(Level.SEVERE, "unable to get hash "
					+ StringUtils.getHexString(hash), e);
		} finally {
			// hashlock.unlock();
		}
		return hs;
	}

	public void processHashClaims() throws IOException {
		this.bdb.claimRecords();
	}

	public void evictChunks(long time) throws IOException {
		this.bdb.removeRecords(time);
	}

	/**
	 * Adds a block of data to the TC hash store and the chunk store.
	 * 
	 * @param chunk
	 *            the chunk to persist
	 * @return true returns true if the data was written. Data will not be
	 *         written if the hash already exists in the db.
	 * @throws IOException 
	 */
	public boolean addHashChunk(HashChunk chunk) throws IOException {
		boolean written = false;
		if(!bdb.containsKey(chunk.getName())){
		try {
			//long start = chunkStore.reserveWritePosition(chunk.getLen());
			ChunkData cm = new ChunkData(chunk.getName(),
					Main.chunkStorePageSize, chunk.getData());
			if (bdb.put(cm)) {
				entries++;
				written = true;
			} else {

			}
		
		} catch (Exception e) {
			if (hashlock.isLocked())
				hashlock.unlock();

			log.log(Level.SEVERE, "Unable to commit chunk "
					+ StringUtils.getHexString(chunk.getName()), e);
		} finally {

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
