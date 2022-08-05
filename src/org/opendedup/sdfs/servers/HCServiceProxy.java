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

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.FDiskException;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.io.events.CloudSyncDLRequest;
import org.opendedup.sdfs.notification.FDiskEvent;
import org.opendedup.sdfs.notification.SDFSEvent;

import com.google.common.eventbus.EventBus;
import com.google.common.primitives.Longs;

public class HCServiceProxy {

	public static HashChunkServiceInterface hcService = null;
	private static EventBus eventBus = new EventBus();

	// private static boolean initialized = false;

	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

	public static synchronized long processHashClaims(SDFSEvent evt, boolean compact) throws IOException {
		return hcService.processHashClaims(evt, compact);

	}

	public static synchronized void compactDB() throws IOException {
		hcService.compactDB();

	}

	public static void clearRefMap() throws IOException {
		hcService.clearRefMap();
	}

	public static synchronized boolean hashExists(byte[] hash, String guid) throws IOException, HashtableFullException {

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
		return hcService.getCacheSize();
	}

	public static synchronized long getMaxCacheSize() {
		return hcService.getMaxCacheSize();

	}

	public static synchronized int getReadSpeed() {
		return hcService.getReadSpeed();
	}

	public static synchronized int getWriteSpeed() {
		return hcService.getWriteSpeed();
	}

	public static synchronized void setReadSpeed(int speed) {
		hcService.setReadSpeed(speed);
	}

	public static synchronized void setWriteSpeed(int speed) {
		hcService.setWriteSpeed(speed);
	}

	public static synchronized void setCacheSize(long sz) throws IOException {
		hcService.setCacheSize(sz);
	}

	public static synchronized void setDseSize(long sz) throws IOException {
		hcService.setDseSize(sz);
	}

	public static long claimKey(byte[] key, long val, long ct) throws IOException {

		return hcService.claimKey(key, val, ct);
	}

	public static long getChunksFetched() {
		return -1;
	}

	public static synchronized void init(ArrayList<String> volumes) throws InterruptedException {
		try {
			/*
			 * File file = new File(Main.hashDBStore + File.separator + ".lock");
			 * if(file.exists()) {
			 * SDFSLogger.getLog().fatal("lock file exists " + file.getPath());
			 * SDFSLogger.getLog().fatal("Please remove lock file to proceed");
			 * System.out.println("lock file exists " + file.getPath());
			 * System.out.println("Please remove lock file to proceed");
			 * System.exit(2);
			 * }
			 */
			SDFSLogger.getLog().info("Starting local chunkstore");
			try {
				hcService = new HashChunkService();
				hcService.init();
			} catch (InterruptedException e) {
				SDFSLogger.getLog().info(e.getMessage());
			}

			if (Main.runConsistancyCheck) {
				hcService.runConsistancyCheck();
			}

			if (Main.syncDL) {
				long vol = Main.DSEID;
				if (Main.syncDLAll) {
					vol = -1;
				}
				SDFSEvent evt = SDFSEvent.syncVolEvent("Syncing from [" + vol + "]");
				eventBus.post(new CloudSyncDLRequest(vol, true, false,false, evt));
			}
			touchRunFile();

		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to initialize HashChunkService ", e);
			System.err.println("Unable to initialize HashChunkService ");
			e.printStackTrace();
			if (Main.sdfsSyncEnabled) {
				throw new InterruptedException("Exception occured while syncSDFS is enabled. " + e);
			} else {
				System.exit(-1);
			}
		}
	}

	public static void syncVolume(long volumeID, boolean syncMap, boolean overwrite,SDFSEvent evt) {
		eventBus.post(new CloudSyncDLRequest(volumeID, syncMap, true,overwrite, evt));
	}

	public static byte getDseCount() {
		return 1;

	}

	public static boolean mightContainKey(byte[] key, String guid, long id) {
		return hcService.mightContainKey(key, id);
	}

	public static AbstractHashesMap getHashesMap() {
		return hcService.getHashesMap();
	}

	public static long getSize() {
		return hcService.getSize();

	}

	public static long getDSESize() {
		return HCServiceProxy.getChunkStore().size();

	}

	public static long getDSECompressedSize() {
		return HCServiceProxy.getChunkStore().compressedSize();

	}

	public static long getDSEMaxSize() {
		return HCServiceProxy.getChunkStore().maxSize();

	}

	public static long getMaxSize() {
		return HCServiceProxy.hcService.getMaxSize();

	}

	public static long getFreeBlocks() {
		return HCServiceProxy.getChunkStore().getFreeBlocks();

	}

	public static AbstractChunkStore getChunkStore() {
		return hcService.getChuckStore();

	}

	public static int getPageSize() {
		return HCServiceProxy.hcService.getPageSize();
	}

	public static void sync() throws IOException {
		hcService.sync();
	}

	public static InsertRecord writeChunk(byte[] hash, byte[] aContents, int ct, String uuid)
			throws IOException, HashtableFullException {
		// doop = HCServiceProxy.hcService.hashExists(hash);

		return HCServiceProxy.hcService.writeChunk(hash, aContents, false, ct, uuid);

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

	public static long hashExists(byte[] hash)
			throws IOException, HashtableFullException {

		return HCServiceProxy.hcService.hashExists(hash);

	}

	public static byte[] fetchChunk(byte[] hash, byte[] hashloc, boolean direct)
			throws IOException, DataArchivedException {

		byte[] data = null;
		long pos = -1;
		if (direct) {
			pos = Longs.fromByteArray(hashloc);
		}

		data = HCServiceProxy.hcService.fetchChunk(hash, pos).getData();

		return data;

	}

	public static void cacheData(long pos) throws IOException, DataArchivedException {
		HCServiceProxy.hcService.cacheChunk(pos);
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

	public static String restoreBlock(byte[] hash, long id) throws IOException {
		return hcService.restoreBlock(hash, id);
	}

	public static boolean blockRestored(String id) throws IOException {
		return hcService.blockRestored(id);
	}

	public static boolean get_move_blob() {
		return hcService.get_move_blob();
	}

	public static void set_move_blob(boolean status) {
		hcService.set_move_blob(status);
	}

}
