/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.servers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.LocalLookupFilter;
import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.hashing.Murmur3HashEngine;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.FDisk;
import org.opendedup.mtools.FDiskException;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.ClusterSocket;
import org.opendedup.sdfs.cluster.DSEClientSocket;
import org.opendedup.sdfs.cluster.cmds.BFClaimHashesCmd;
import org.opendedup.sdfs.cluster.cmds.BatchHashExistsCmd;
import org.opendedup.sdfs.cluster.cmds.BatchWriteHashCmd;
import org.opendedup.sdfs.cluster.cmds.ClaimHashesCmd;
import org.opendedup.sdfs.cluster.cmds.DirectFetchChunkCmd;
import org.opendedup.sdfs.cluster.cmds.DirectWriteHashCmd;
import org.opendedup.sdfs.cluster.cmds.HashExistsCmd;
import org.opendedup.sdfs.cluster.cmds.RedundancyNotMetException;
import org.opendedup.sdfs.cluster.cmds.WriteHashCmd;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.events.CloudSyncDLRequest;
import org.opendedup.sdfs.notification.FDiskEvent;
import org.opendedup.sdfs.notification.SDFSEvent;

import com.google.common.eventbus.EventBus;
import com.google.common.primitives.Longs;

public class HCServiceProxy {

	private static HashChunkServiceInterface hcService = null;
	private static DSEClientSocket socket = null;
	private static EventBus eventBus = new EventBus();
	public static ClusterSocket cs = null;

	

	// private static boolean initialized = false;
	
	public static LocalLookupFilter getLookupFilter(String filter) throws IOException {
			return LocalLookupFilter.getLocalLookupFilter(filter);
	}

	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

	public static synchronized long processHashClaims(SDFSEvent evt) throws IOException {
		if (Main.chunkStoreLocal)
			return hcService.processHashClaims(evt);
		else {
			new ClaimHashesCmd(evt).executeCmd(cs);
			return 0;

		}
	}

	public static synchronized long processHashClaims(SDFSEvent evt, LargeBloomFilter bf) throws IOException {
		if (Main.chunkStoreLocal)
			return hcService.processHashClaims(evt, bf);
		else {
			new BFClaimHashesCmd(evt).executeCmd(cs);
		}
		return 0;
	}

	public static void clearRefMap() throws IOException {
		hcService.clearRefMap();
	}

	public static synchronized boolean hashExists(byte[] hash, String guid) throws IOException, HashtableFullException {
		if (guid != null && LocalLookupFilter.getLocalLookupFilter(guid).containsKey(hash)) {
				return true;
			}
		
		long pos = hcService.hashExists(hash);
		if (pos != -1)
			return true;
		else
			return false;
	}

	public static HashChunk fetchHashChunk(byte[] hash) throws IOException, DataArchivedException {
		return hcService.fetchChunk(hash, -1);
	}

	public static synchronized long getCacheSize() {
		if (Main.chunkStoreLocal) {
			return hcService.getCacheSize();
		} else
			return 0;
	}

	public static synchronized long getMaxCacheSize() {
		if (Main.chunkStoreLocal) {
			return hcService.getMaxCacheSize();
		} else
			return 0;
	}

	public static synchronized int getReadSpeed() {
		if (Main.chunkStoreLocal) {
			return hcService.getReadSpeed();
		} else
			return 0;
	}

	public static synchronized int getWriteSpeed() {
		if (Main.chunkStoreLocal) {
			return hcService.getWriteSpeed();
		} else
			return 0;
	}

	public static synchronized void setReadSpeed(int speed) {
		if (Main.chunkStoreLocal) {
			hcService.setReadSpeed(speed);
		}
	}

	public static synchronized void setWriteSpeed(int speed) {
		if (Main.chunkStoreLocal) {
			hcService.setWriteSpeed(speed);
		}
	}

	public static synchronized void setCacheSize(long sz) throws IOException {
		if (Main.chunkStoreLocal) {
			hcService.setCacheSize(sz);
		}
	}

	public static boolean claimKey(byte[] key, long val, long ct, String guid) throws IOException {
		if (Main.chunkStoreLocal) {
			if (guid != null) {
				long nc;
					nc = LocalLookupFilter.getLocalLookupFilter(guid).claimKey(key, val, ct);
				if (nc == 0) {
					return true;
				}
			}
			return hcService.claimKey(key, val, ct);
		} else
			return false;
	}

	public static long getChunksFetched() {
		return -1;
	}

	public static synchronized void init(ArrayList<String> volumes) {
		try {
			if (Main.chunkStoreLocal) {
				/*
				File file = new File(Main.hashDBStore + File.separator + ".lock");
				if(file.exists()) {
					SDFSLogger.getLog().fatal("lock file exists " + file.getPath());
					SDFSLogger.getLog().fatal("Please remove lock file to proceed");
					System.out.println("lock file exists " + file.getPath());
					System.out.println("Please remove lock file to proceed");
					System.exit(2);
				}
				*/
				SDFSLogger.getLog().info("Starting local chunkstore");
				
				hcService = new HashChunkService();
				hcService.init();
				
				if (Main.runConsistancyCheck) {
					hcService.runConsistancyCheck();
				}

				if (Main.syncDL) {
					eventBus.post(new CloudSyncDLRequest(Main.DSEID, true, false));
				}

				if (Main.syncDL) {
					SDFSLogger.getLog().info("running consistency check");
					SDFSEvent evt = SDFSEvent
							.gcInfoEvent("SDFS Volume Reference Recreation Starting for " + Main.volume.getName());
					new FDisk(evt);
				}
				touchRunFile();
			}

			else {
				SDFSLogger.getLog().info(
						"Starting clustered Volume with id=" + Main.DSEClusterID + " config=" + Main.DSEClusterConfig);
				socket = new DSEClientSocket(Main.DSEClusterConfig, Main.DSEClusterID, volumes);
				cs = socket;
				socket.startGCIfNone();
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to initialize HashChunkService ", e);
			System.err.println("Unable to initialize HashChunkService ");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void syncVolume(long volumeID, boolean syncMap) {
		if (Main.chunkStoreLocal) {
			eventBus.post(new CloudSyncDLRequest(volumeID, syncMap, true));
		}
	}

	public static byte getDseCount() {
		if (Main.chunkStoreLocal)
			return 1;
		else {

			return (byte) socket.serverState.size();
		}
	}

	public static boolean mightContainKey(byte[] key, String guid) {
		return hcService.mightContainKey(key);
	}

	public static AbstractHashesMap getHashesMap() {
		if (Main.chunkStoreLocal)
			return hcService.getHashesMap();
		else
			return null;
	}

	public static long getSize() {
		if (Main.chunkStoreLocal) {
			return hcService.getSize();
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
		if (Main.chunkStoreLocal)
			return hcService.getChuckStore();
		else
			return null;
	}

	public static int getPageSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.hcService.getPageSize();
		} else {
			return Main.CHUNK_LENGTH;
		}
	}

	public static void sync() throws IOException {
		if (Main.chunkStoreLocal)
			hcService.sync();
	}

	private static InsertRecord _write(byte[] hash, byte[] aContents, byte[] hashloc, String guid)
			throws IOException, RedundancyNotMetException {
		if (Main.DSEClusterDirectIO)
			return new InsertRecord(true, directWriteChunk(hash, aContents, hashloc, guid));
		else {
			int ncopies = 0;
			for (int i = 1; i < 8; i++) {
				if (hashloc[i] > (byte) 0) {
					ncopies++;
				}
			}
			if (ncopies >= Main.volume.getClusterCopies()) {
				return new InsertRecord(true, hashloc);
			} else if (ncopies > 0) {
				byte[] ignoredHosts = new byte[ncopies];
				for (int i = 0; i < ncopies; i++)
					ignoredHosts[i] = hashloc[i + 1];
				WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false, Main.volume.getClusterCopies(),
						ignoredHosts);

				cmd.executeCmd(socket);
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("wrote data when found some but not all");
				return new InsertRecord(true, cmd.reponse());
			} else {
				WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false, Main.volume.getClusterCopies());
				cmd.executeCmd(socket);
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("wrote data when found none");

				return new InsertRecord(true, cmd.reponse());
			}
		}
	}

	public static InsertRecord writeChunk(byte[] hash, byte[] aContents, byte[] hashloc, int ct, String guid)
			throws IOException {

		int tries = 0;
		while (true) {
			try {
				return _write(hash, aContents, hashloc, guid);
			} catch (IOException e) {
				tries++;
				if (tries > 10) {
					throw e;
				}
			} catch (RedundancyNotMetException e) {
				tries++;
				hashloc = e.hashloc;
				if (tries > 10) {
					SDFSLogger.getLog().warn("Redundancy Requirements have not been met");
					// throw e;
				}
			}
		}

	}

	public static byte[] directWriteChunk(byte[] hash, byte[] aContents, byte[] hashloc, String guid)
			throws IOException {
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
			DirectWriteHashCmd cmd = new DirectWriteHashCmd(hash, aContents, aContents.length, false,
					Main.volume.getClusterCopies(), ignoredHosts);
			cmd.executeCmd(socket); //
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("wrote data when found some but not all");
			return cmd.reponse();

		} else {
			DirectWriteHashCmd cmd = new DirectWriteHashCmd(hash, aContents, aContents.length, false,
					Main.volume.getClusterCopies());
			cmd.executeCmd(socket);
			SDFSLogger.getLog().debug("wrote data when found none");
			if (cmd.getExDn() > 0) {
				SDFSLogger.getLog().warn("Was unable to write to all storage nodes, trying again");
				cmd = new DirectWriteHashCmd(hash, aContents, aContents.length, false, Main.volume.getClusterCopies(),
						cmd.reponse());
			}

			return cmd.reponse();
		}

	}

	public static InsertRecord writeChunk(byte[] hash, byte[] aContents, int ct, String guid)
			throws IOException, HashtableFullException {
		if (Main.chunkStoreLocal) {
			// doop = HCServiceProxy.hcService.hashExists(hash);
			if (guid != null) {
					long pos = LocalLookupFilter.getLocalLookupFilter(guid).put(hash, ct);
					if (pos == -1) {
						InsertRecord ir = HCServiceProxy.hcService.writeChunk(hash, aContents, false, 1);
						LocalLookupFilter.getLocalLookupFilter(guid).put(hash, Longs.fromByteArray(ir.getHashLocs()), ct);
						return ir;
					} else {
						return new InsertRecord(false, pos);
					}

			} else
				return HCServiceProxy.hcService.writeChunk(hash, aContents, false, ct);
		} else {
			try {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("looking for hash");
				HashExistsCmd hcmd = new HashExistsCmd(hash, false, Main.volume.getClusterCopies());
				hcmd.executeCmd(socket);
				if (hcmd.meetsRedundancyRequirements()) {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("found all");
					return new InsertRecord(false, hcmd.getResponse());
				} else if (hcmd.exists()) {
					byte[] ignoredHosts = new byte[hcmd.responses()];
					for (int i = 0; i < hcmd.responses(); i++)
						ignoredHosts[i] = hcmd.getResponse()[i + 1];
					WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false, Main.volume.getClusterCopies(),
							ignoredHosts);
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
						SDFSLogger.getLog().debug("wrote data when found some but not all");
					return new InsertRecord(true, cmd.reponse());
				} else {
					WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false, Main.volume.getClusterCopies());
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
					 * cmd = new DirectWriteHashCmd(hash, aContents, aContents.length, false,
					 * Main.volume.getClusterCopies(), cmd.reponse());
					 */
					// }
					return new InsertRecord(false, cmd.reponse());
				}
			} catch (Exception e1) {
				SDFSLogger.getLog().fatal("Unable to write chunk " + hash, e1);
				throw new IOException("Unable to write chunk " + hash);
			} finally {

			}
		}

	}

	/*
	 * public static InsertRecord writeChunk(byte[] hash, byte[] aContents, byte[]
	 * ignoredHosts) throws IOException, HashtableFullException { if
	 * (Main.chunkStoreLocal) { // doop = HCServiceProxy.hcService.hashExists(hash);
	 * return HCServiceProxy.hcService.writeChunk(hash, aContents, false); } else {
	 * 
	 * try { if (ignoredHosts != null) { WriteHashCmd cmd = new WriteHashCmd(hash,
	 * aContents, false, Main.volume.getClusterCopies(), ignoredHosts);
	 * cmd.executeCmd(socket); return new InsertRecord(true,cmd.reponse()); } else {
	 * WriteHashCmd cmd = new WriteHashCmd(hash, aContents, false,
	 * Main.volume.getClusterCopies()); cmd.executeCmd(socket); return new
	 * InsertRecord(true,cmd.reponse()); } } catch (Exception e1) { //
	 * SDFSLogger.getLog().fatal("Unable to write chunk " + hash, // e1); throw new
	 * IOException("Unable to write chunk " + hash); } finally {
	 * 
	 * } } }
	 */

	public static void runFDisk(FDiskEvent evt) throws FDiskException, IOException {
		throw new IOException("not implemented");
	}

	/*
	 * public static void fetchChunks(ArrayList<String> hashes, String server,
	 * String password, int port, boolean useSSL) throws IOException,
	 * HashtableFullException { if (Main.chunkStoreLocal) {
	 * HCServiceProxy.hcService.remoteFetchChunks(hashes, server, password, port,
	 * useSSL); } else { throw new IllegalStateException(
	 * "not implemented for remote chunkstores"); } }
	 */

	public static long hashExists(byte[] hash, boolean findAll, String guid)
			throws IOException, HashtableFullException {
		if (Main.chunkStoreLocal) {
			if (guid != null) {
				long pos;
					pos = LocalLookupFilter.getLocalLookupFilter(guid).get(hash);

					if (pos != -1) {
						return pos;
					}
			}

			return HCServiceProxy.hcService.hashExists(hash);

		} else {
			HashExistsCmd cmd = new HashExistsCmd(hash, findAll, Main.volume.getClusterCopies());
			cmd.executeCmd(socket);
			return Longs.fromByteArray(cmd.getResponse());
		}
	}

	public static List<HashLocPair> batchHashExists(List<HashLocPair> hashes) throws IOException {
		if (Main.chunkStoreLocal) {
			throw new IOException("not implemented for localstore");

		} else {
			BatchHashExistsCmd cmd = new BatchHashExistsCmd(hashes);
			cmd.executeCmd(socket);
			return cmd.getHashes();
		}
	}

	public static List<HashLocPair> batchWriteHash(List<HashLocPair> hashes) throws IOException {
		if (Main.chunkStoreLocal) {
			throw new IOException("not implemented for localstore");

		} else {
			BatchWriteHashCmd cmd = new BatchWriteHashCmd(hashes);
			cmd.executeCmd(socket);
			return cmd.getHashes();
		}
	}

	public static long hashExists(byte[] hash, boolean findAll, byte numtowaitfor, String guid)
			throws IOException, HashtableFullException {
		if (Main.chunkStoreLocal) {
			if (guid != null) {
				long pos;
					pos = LocalLookupFilter.getLocalLookupFilter(guid).get(hash);

					if (pos != -1) {
						return pos;
					}
				
			}

			return HCServiceProxy.hcService.hashExists(hash);
		} else {
			HashExistsCmd cmd = new HashExistsCmd(hash, findAll, numtowaitfor);
			cmd.executeCmd(socket);
			return Longs.fromByteArray(cmd.getResponse());
		}
	}

	static Murmur3HashEngine he = new Murmur3HashEngine();

	public static byte[] fetchChunk(byte[] hash, byte[] hashloc, boolean direct)
			throws IOException, DataArchivedException {

		if (Main.chunkStoreLocal) {
			byte[] data = null;
			long pos = -1;
			if (direct) {
				pos = Longs.fromByteArray(hashloc);
			}

			data = HCServiceProxy.hcService.fetchChunk(hash, pos).getData();

			return data;
		} else {
			try {
				DirectFetchChunkCmd cmd = new DirectFetchChunkCmd(hash, hashloc);
				cmd.executeCmd(socket);
				return cmd.getChunk();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}

	public static void cacheData(long pos) throws IOException, DataArchivedException {

		if (Main.chunkStoreLocal) {
			HCServiceProxy.hcService.cacheChunk(pos);
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
		LocalLookupFilter.closeAll();
		hcService.close();
		SDFSLogger.getLog().info("Deleting lock file");
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
			SDFSLogger.getLog().warn("unable to create lock file " + file.getPath(), e);
		}
	}

	public static String restoreBlock(byte[] hash) throws IOException {
		return hcService.restoreBlock(hash);
	}

	public static boolean blockRestored(String id) throws IOException {
		return hcService.blockRestored(id);
	}

}
