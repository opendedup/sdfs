package org.opendedup.sdfs.servers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.BloomFileByteArrayLongMap.KeyBlob;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.FDisk;
import org.opendedup.mtools.FDiskException;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.ClusterSocket;
import org.opendedup.sdfs.cluster.DSEClientSocket;
import org.opendedup.sdfs.cluster.cmds.BatchHashExistsCmd;
import org.opendedup.sdfs.cluster.cmds.BatchWriteHashCmd;
import org.opendedup.sdfs.cluster.cmds.ClaimHashesCmd;
import org.opendedup.sdfs.cluster.cmds.DirectFetchChunkCmd;
import org.opendedup.sdfs.cluster.cmds.DirectWriteHashCmd;
import org.opendedup.sdfs.cluster.cmds.FDiskCmd;
import org.opendedup.sdfs.cluster.cmds.FetchChunkCmd;
import org.opendedup.sdfs.cluster.cmds.HashExistsCmd;
import org.opendedup.sdfs.cluster.cmds.RedundancyNotMetException;
import org.opendedup.sdfs.cluster.cmds.RemoveChunksCmd;
import org.opendedup.sdfs.cluster.cmds.WriteHashCmd;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.io.DedupChunkInterface;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.notification.SDFSEvent;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.BloomFilter;

public class HCServiceProxy {

	private static HashChunkServiceInterface hcService = null;
	private static DSEClientSocket socket = null;
	public static ClusterSocket cs = null;
	private static int cacheSize = 104857600 / Main.CHUNK_LENGTH;
	private static final LoadingCache<ByteArrayWrapper, byte[]> chunks = CacheBuilder
			.newBuilder().maximumSize(cacheSize).concurrencyLevel(72)
			.build(new CacheLoader<ByteArrayWrapper, byte[]>() {
				public byte[] load(ByteArrayWrapper key) throws IOException {
					if (Main.DSEClusterDirectIO) {
						DirectFetchChunkCmd cmd = new DirectFetchChunkCmd(
								key.data, key.hashloc);
						cmd.executeCmd(socket);
						return cmd.getChunk();
					} else {
						FetchChunkCmd cmd = new FetchChunkCmd(key.data,
								key.hashloc);
						cmd.executeCmd(socket);
						return cmd.getChunk();
					}

				}
			});

	// private static boolean initialized = false;

	public static synchronized void processHashClaims(SDFSEvent evt)
			throws IOException {
		if (Main.chunkStoreLocal)
			hcService.processHashClaims(evt);
		else {
			new ClaimHashesCmd(evt).executeCmd(cs);

		}
	}

	public static synchronized long processHashClaims(SDFSEvent evt,
			BloomFilter<KeyBlob> bf) throws IOException {
		if (Main.chunkStoreLocal)
			return hcService.processHashClaims(evt, bf);
		else {
			new ClaimHashesCmd(evt).executeCmd(cs);

		}
		return 0;
	}

	public static synchronized boolean hashExists(byte[] hash)
			throws IOException, HashtableFullException {
		return hcService.hashExists(hash);
	}

	public static HashChunk fetchHashChunk(byte[] hash) throws IOException {
		return hcService.fetchChunk(hash);
	}

	public static synchronized long removeStailHashes(long ms,
			boolean forceRun, SDFSEvent evt) throws IOException {
		if (Main.chunkStoreLocal) {
			long tm = System.currentTimeMillis() - ms;
			return hcService.removeStailHashes(tm, forceRun, evt);
		} else {
			RemoveChunksCmd cmd = new RemoveChunksCmd(ms, forceRun, evt);
			cmd.executeCmd(cs);
			return cmd.removedHashesCount();
		}

	}

	public static long getChunksFetched() {
		return -1;
	}

	public static synchronized void init(ArrayList<String> volumes) {
		try {
			if (Main.chunkStoreLocal) {
				SDFSLogger.getLog().info("Starting local chunkstore");
				hcService = new HashChunkService();
				hcService.init();
				File file = new File(Main.hashDBStore + File.separator
						+ ".lock");
				if (Main.runConsistancyCheck
						&& (!Main.closedGracefully || file.exists())) {
					hcService.runConsistancyCheck();
				}
				touchRunFile();
			}

			else {
				SDFSLogger.getLog().info(
						"Starting clustered Volume with id="
								+ Main.DSEClusterID + " config="
								+ Main.DSEClusterConfig);
				socket = new DSEClientSocket(Main.DSEClusterConfig,
						Main.DSEClusterID, volumes);
				cs = socket;
				socket.startGCIfNone();
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to initialize HashChunkService ",
					e);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static byte getDseCount() {
		if (Main.chunkStoreLocal)
			return 1;
		else {

			return (byte) socket.serverState.size();
		}
	}
	
	public static AbstractHashesMap getHashesMap() {
		if(Main.chunkStoreLocal)
		return hcService.getHashesMap();
		else return null;
	}

	public static long getSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.hcService.getSize();
		} else {
			return socket.getCurrentSize();
		}
	}

	public static long getDSESize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.getChunkStore().size();
		} else {
			return socket.getCurrentDSESize();
		}
	}

	public static long getDSECompressedSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.getChunkStore().compressedSize();
		} else {
			return socket.getCurrentDSECompSize();
		}
	}

	public static long getDSEMaxSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.getChunkStore().maxSize();
		} else {
			return socket.getDSEMaxSize();
		}
	}

	public static long getMaxSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.hcService.getMaxSize();
		} else {
			return socket.getMaxSize();
		}
	}

	public static long getFreeBlocks() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.getChunkStore().getFreeBlocks();
		} else {
			return socket.getFreeBlocks();
		}
	}

	public static AbstractChunkStore getChunkStore() {
		return hcService.getChuckStore();
	}

	public static int getPageSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.hcService.getPageSize();
		} else {
			return Main.CHUNK_LENGTH;
		}
	}

	private static byte[] _write(byte[] hash, byte[] aContents, byte[] hashloc)
			throws IOException, RedundancyNotMetException {
		if (Main.DSEClusterDirectIO)
			return directWriteChunk(hash, aContents, hashloc);
		else {
			int ncopies = 0;
			for (int i = 1; i < 8; i++) {
				if (hashloc[i] > (byte) 0) {
					ncopies++;
				}
			}
			if (ncopies >= Main.volume.getClusterCopies()) {
				return hashloc;
			} else if (ncopies > 0) {
				byte[] ignoredHosts = new byte[ncopies];
				for (int i = 0; i < ncopies; i++)
					ignoredHosts[i] = hashloc[i + 1];
				WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false,
						Main.volume.getClusterCopies(), ignoredHosts);

				cmd.executeCmd(socket);
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"wrote data when found some but not all");
				return cmd.reponse();
			} else {
				WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false,
						Main.volume.getClusterCopies());
				cmd.executeCmd(socket);
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("wrote data when found none");

				return cmd.reponse();
			}
		}
	}

	public static byte[] writeChunk(byte[] hash, byte[] aContents,
			byte[] hashloc) throws IOException {

		int tries = 0;
		while (true) {
			try {
				return _write(hash, aContents, hashloc);
			} catch (IOException e) {
				tries++;
				if (tries > 10) {
					throw e;
				}
			} catch (RedundancyNotMetException e) {
				tries++;
				hashloc = e.hashloc;
				if (tries > 10) {
					SDFSLogger.getLog().warn(
							"Redundancy Requirements have not been met");
					// throw e;
				}
			}
		}

	}

	public static byte[] directWriteChunk(byte[] hash, byte[] aContents,
			byte[] hashloc) throws IOException {
		int ncopies = 0;
		for (int i = 1; i < 8; i++) {
			if (hashloc[i] > (byte) 0) {
				ncopies++;
			}
		}
		if (ncopies >= Main.volume.getClusterCopies()) {
			return hashloc;
		} else if (ncopies > 0) {
			byte[] ignoredHosts = new byte[ncopies];
			for (int i = 0; i < ncopies; i++)
				ignoredHosts[i] = hashloc[i + 1];
			DirectWriteHashCmd cmd = new DirectWriteHashCmd(hash, aContents,
					aContents.length, false, Main.volume.getClusterCopies(),
					ignoredHosts);
			cmd.executeCmd(socket); //
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"wrote data when found some but not all");
			return cmd.reponse();

		} else {
			DirectWriteHashCmd cmd = new DirectWriteHashCmd(hash, aContents,
					aContents.length, false, Main.volume.getClusterCopies());
			cmd.executeCmd(socket);
			// SDFSLogger.getLog().debug("wrote data when found none");
			if (cmd.getExDn() > 0) {
				SDFSLogger
						.getLog()
						.warn("Was unable to write to all storage nodes, trying again");
				cmd = new DirectWriteHashCmd(hash, aContents, aContents.length,
						false, Main.volume.getClusterCopies(), cmd.reponse());
			}

			return cmd.reponse();
		}

	}

	public static byte[] writeChunk(byte[] hash, byte[] aContents,
			int position, int len, boolean sendChunk) throws IOException,
			HashtableFullException {
		boolean doop = false;
		byte[] b = new byte[8];
		if (Main.chunkStoreLocal) {
			// doop = HCServiceProxy.hcService.hashExists(hash);
			if (!doop && sendChunk) {
				doop = HCServiceProxy.hcService.writeChunk(hash, aContents, 0,
						Main.CHUNK_LENGTH, false);
			}
			b[1] = -2;
		} else {
			try {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("looking for hash");
				HashExistsCmd hcmd = new HashExistsCmd(hash, false,
						Main.volume.getClusterCopies());
				hcmd.executeCmd(socket);
				if (hcmd.meetsRedundancyRequirements()) {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("found all");
					return hcmd.getResponse();
				} else if (hcmd.exists()) {
					byte[] ignoredHosts = new byte[hcmd.responses()];
					for (int i = 0; i < hcmd.responses(); i++)
						ignoredHosts[i] = hcmd.getResponse()[i + 1];
					WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false,
							Main.volume.getClusterCopies(), ignoredHosts);
					int tries = 0;
					while (true) {
						try {
							cmd.executeCmd(socket);
							break;
						} catch (IOException e) {
							tries++;
							if (tries > 10)
								throw e;
						}
					}
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug(
								"wrote data when found some but not all");
					return cmd.reponse();
				} else {
					WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false,
							Main.volume.getClusterCopies());
					int tries = 0;
					while (true) {
						try {
							cmd.executeCmd(socket);
							break;
						} catch (IOException e) {
							tries++;
							if (tries > 10)
								throw e;
						}
					}
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("wrote data when found none");

					// if(cmd.getExDn() > 0) {
					// SDFSLogger.getLog().warn("Was unable to write to all storage nodes.");
					/*
					 * cmd = new DirectWriteHashCmd(hash, aContents,
					 * aContents.length, false, Main.volume.getClusterCopies(),
					 * cmd.reponse());
					 */
					// }
					return cmd.reponse();
				}
			} catch (Exception e1) {
				SDFSLogger.getLog().fatal("Unable to write chunk " + hash, e1);
				throw new IOException("Unable to write chunk " + hash);
			} finally {

			}
		}
		if (doop)
			b[0] = 1;

		return b;
	}

	public static byte[] writeChunk(byte[] hash, byte[] aContents,
			int position, int len, boolean sendChunk, byte[] ignoredHosts)
			throws IOException, HashtableFullException {
		boolean doop = false;
		byte[] b = new byte[8];
		if (Main.chunkStoreLocal) {
			// doop = HCServiceProxy.hcService.hashExists(hash);
			if (!doop && sendChunk) {
				doop = HCServiceProxy.hcService.writeChunk(hash, aContents, 0,
						Main.CHUNK_LENGTH, false);
			}
			b[1] = -2;
		} else {

			try {
				if (ignoredHosts != null) {
					WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false,
							Main.volume.getClusterCopies(), ignoredHosts);
					cmd.executeCmd(socket);
					return cmd.reponse();
				} else {
					WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false,
							Main.volume.getClusterCopies());
					cmd.executeCmd(socket);
					return cmd.reponse();
				}
			} catch (Exception e1) {
				// SDFSLogger.getLog().fatal("Unable to write chunk " + hash,
				// e1);
				throw new IOException("Unable to write chunk " + hash);
			} finally {

			}
		}
		if (doop)
			b[0] = 1;

		return b;
	}

	public static void runFDisk(SDFSEvent evt) throws FDiskException,
			IOException {
		if (Main.chunkStoreLocal)
			new FDisk(evt);
		else {
			FDiskCmd cmd = new FDiskCmd();
			cmd.executeCmd(cs);
			List<SDFSEvent> events = cmd.getResults();
			for (SDFSEvent event : events) {
				evt.addChild(event.getChildren().get(0));
			}

		}
	}

	/*
	 * public static void fetchChunks(ArrayList<String> hashes, String server,
	 * String password, int port, boolean useSSL) throws IOException,
	 * HashtableFullException { if (Main.chunkStoreLocal) {
	 * HCServiceProxy.hcService.remoteFetchChunks(hashes, server, password,
	 * port, useSSL); } else { throw new IllegalStateException(
	 * "not implemented for remote chunkstores"); } }
	 */

	public static byte[] hashExists(byte[] hash, boolean findAll)
			throws IOException, HashtableFullException {
		byte[] exists = new byte[8];
		if (Main.chunkStoreLocal) {
			if (HCServiceProxy.hcService.hashExists(hash)) {
				exists[0] = 1;
				return exists;
			} else {
				exists[0] = -1;
				return exists;
			}

		} else {
			HashExistsCmd cmd = new HashExistsCmd(hash, findAll,
					Main.volume.getClusterCopies());
			cmd.executeCmd(socket);
			return cmd.getResponse();
		}
	}

	public static List<SparseDataChunk> batchHashExists(
			List<SparseDataChunk> hashes) throws IOException {
		if (Main.chunkStoreLocal) {
			throw new IOException("not implemented for localstore");

		} else {
			BatchHashExistsCmd cmd = new BatchHashExistsCmd(hashes);
			cmd.executeCmd(socket);
			return cmd.getHashes();
		}
	}

	public static List<DedupChunkInterface> batchWriteHash(
			List<DedupChunkInterface> hashes) throws IOException {
		if (Main.chunkStoreLocal) {
			throw new IOException("not implemented for localstore");

		} else {
			BatchWriteHashCmd cmd = new BatchWriteHashCmd(hashes);
			cmd.executeCmd(socket);
			return cmd.getHashes();
		}
	}

	public static byte[] hashExists(byte[] hash, boolean findAll,
			byte numtowaitfor) throws IOException, HashtableFullException {
		byte[] exists = new byte[8];
		if (Main.chunkStoreLocal) {
			if (HCServiceProxy.hcService.hashExists(hash))
				return exists;
			else {
				exists[0] = -1;
				return exists;
			}

		} else {
			HashExistsCmd cmd = new HashExistsCmd(hash, findAll, numtowaitfor);
			cmd.executeCmd(socket);
			return cmd.getResponse();
		}
	}

	public static byte[] fetchChunk(byte[] hash, byte[] hashloc)
			throws IOException {

		if (Main.chunkStoreLocal) {
			HashChunk hc = HCServiceProxy.hcService.fetchChunk(hash);
			return hc.getData();
		} else {
			ByteArrayWrapper wrapper = new ByteArrayWrapper(hash, hashloc);
			try {
				byte[] _bz = chunks.get(wrapper);
				byte[] bz = org.bouncycastle.util.Arrays.clone(_bz);
				return bz;
			} catch (ExecutionException e) {
				throw new IOException(e);
			}
		}
	}

	public static long getChunksRead() {
		return hcService.getChunksRead();
	}

	public static double getChunksWritten() {
		return hcService.getChunksWritten();
	}

	public static double getKBytesRead() {
		return hcService.getKBytesRead();
	}

	public static double getKBytesWrite() {
		return hcService.getKBytesWrite();
	}

	public static long getDupsFound() {
		return hcService.getDupsFound();
	}

	public static void close() {
		hcService.close();
		File file = new File(Main.hashDBStore + File.separator + ".lock");
		file.delete();
	}

	private static void touchRunFile() {
		File file = new File(Main.hashDBStore + File.separator + ".lock");
		try {

			if (!file.exists())
				new FileOutputStream(file).close();
			file.setLastModified(System.currentTimeMillis());
			SDFSLogger.getLog().warn("Write lock file " + file.getPath());
		} catch (IOException e) {
			SDFSLogger.getLog().warn(
					"unable to create lock file " + file.getPath(), e);
		}
	}

	private static final class ByteArrayWrapper {
		private final byte[] data;
		public final byte[] hashloc;

		public ByteArrayWrapper(byte[] data, byte[] hashloc) {
			if (data == null) {
				throw new NullPointerException();
			}
			this.data = data;
			this.hashloc = hashloc;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof ByteArrayWrapper)) {
				return false;
			}
			return Arrays.equals(data, ((ByteArrayWrapper) other).data);
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(data);
		}
	}

}
