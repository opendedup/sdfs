package org.opendedup.sdfs.servers;

import java.io.IOException;

import java.util.ArrayList;

import org.opendedup.collections.HashtableFullException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.ConsistancyCheck;
import org.opendedup.sdfs.filestore.DSECompaction;
import org.opendedup.sdfs.filestore.FileChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.filestore.HashStore;
import org.opendedup.sdfs.network.HashClient;
import org.opendedup.sdfs.notification.SDFSEvent;

public class HashChunkService implements HashChunkServiceInterface {

	private double kBytesRead;
	private double kBytesWrite;
	private final long KBYTE = 1024L;
	private long chunksRead;
	private long chunksWritten;
	private long chunksFetched;
	private double kBytesFetched;
	private int unComittedChunks;
	private int MAX_UNCOMITTEDCHUNKS = 100;
	private HashStore hs = null;
	private AbstractChunkStore fileStore = null;

	// private HashClientPool hcPool = null;

	/**
	 * @return the chunksFetched
	 */
	public long getChunksFetched() {
		return chunksFetched;

	}

	public HashChunkService() {
		try {
			fileStore = (AbstractChunkStore) Class
					.forName(Main.chunkStoreClass).newInstance();
			fileStore.init(Main.chunkStoreConfig);
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
		try {
			hs = new HashStore(this);
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to start hashstore", e);
			System.exit(-1);
		}
	}

	private long dupsFound;

	public AbstractChunkStore getChuckStore() {
		return fileStore;
	}

	public boolean writeChunk(byte[] hash, byte[] aContents, int position,
			int len, boolean compressed) throws IOException,
			HashtableFullException {
		if (aContents.length > Main.chunkStorePageSize)
			throw new IOException("content size out of bounds ["
					+ aContents.length + "] > [" + Main.chunkStorePageSize
					+ "]");
		chunksRead++;
		kBytesRead = kBytesRead + (position / KBYTE);
		boolean written = hs.addHashChunk(new HashChunk(hash, 0, len,
				aContents, compressed));
		if (written) {
			unComittedChunks++;
			chunksWritten++;
			kBytesWrite = kBytesWrite + (position / KBYTE);
			if (unComittedChunks > MAX_UNCOMITTEDCHUNKS) {
				commitChunks();
			}
			return false;
		} else {
			dupsFound++;
			return true;
		}
	}

	public boolean localHashExists(byte[] hash) throws IOException {
		return hs.hashExists(hash);
	}

	public void remoteFetchChunks(ArrayList<String> al, String server,
			String password, int port, boolean useSSL) throws IOException,
			HashtableFullException {
		HCServer hserver = new HCServer(server, port, false, false, useSSL);
		HashClient hc = new HashClient(hserver, "replication", password,
				(byte) 0, null);
		try {
			ArrayList<HashChunk> hck = hc.fetchChunks(al);
			for (int i = 0; i < hck.size(); i++) {
				HashChunk _hc = hck.get(i);
				writeChunk(_hc.getName(), _hc.getData(), 0,
						_hc.getData().length, false);
			}
		} finally {
			hc.close();
		}
	}

	public boolean hashExists(byte[] hash) throws IOException,
			HashtableFullException {
		boolean exists = hs.hashExists(hash);
		return exists;
	}

	public HashChunk fetchChunk(byte[] hash) throws IOException {
		HashChunk hashChunk = hs.getHashChunk(hash);
		byte[] data = hashChunk.getData();
		kBytesFetched = kBytesFetched + (data.length / KBYTE);
		chunksFetched++;
		return hashChunk;
	}

	public byte getHashRoute(byte[] hash) {
		byte hashRoute = (byte) (hash[1] / (byte) 16);
		if (hashRoute < 0) {
			hashRoute += 1;
			hashRoute *= -1;
		}
		return hashRoute;
	}

	public void processHashClaims(SDFSEvent evt) throws IOException {
		hs.processHashClaims(evt);
	}

	public long removeStailHashes(long ms, boolean forceRun, SDFSEvent evt)
			throws IOException {
		return hs.evictChunks(ms, forceRun, evt);
	}

	public void commitChunks() {
		// H2HashStore.commitTransactions();
		unComittedChunks = 0;
	}

	public long getSize() {
		return hs.getEntries();
	}

	public long getMaxSize() {
		return hs.getMaxEntries();
	}

	public int getPageSize() {
		return Main.chunkStorePageSize;
	}

	public long getChunksRead() {
		return chunksRead;
	}

	public long getChunksWritten() {
		return chunksWritten;
	}

	public double getKBytesRead() {
		return kBytesRead;
	}

	public double getKBytesWrite() {
		return kBytesWrite;
	}

	public long getDupsFound() {
		return dupsFound;
	}

	public void close() {
		fileStore.close();
		hs.close();

	}

	public void init() throws IOException {
		if (Main.runCompact) {
			DSECompaction.runCheck(hs.bdb,
					(FileChunkStore) this.getChuckStore());
			SDFSLogger.getLog().info("Finished compaction");

		}
	}

	@Override
	public void runConsistancyCheck() {
		SDFSLogger.getLog().info(
				"DSE did not close gracefully, running consistancy check");
		ConsistancyCheck.runCheck(hs.bdb, getChuckStore());

	}

}
