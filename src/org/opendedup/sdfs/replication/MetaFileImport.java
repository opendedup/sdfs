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
package org.opendedup.sdfs.replication;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.mgmt.cli.ProcessBatchGetBlocks;
import org.opendedup.sdfs.notification.BlockImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;

import com.google.common.primitives.Longs;

import jonelo.jacksum.adapt.org.bouncycastle.util.Arrays;

public class MetaFileImport implements Serializable {
	private static final long serialVersionUID = 2281680761909041919L;
	private long filesProcessed = 0;
	private transient ArrayList<byte[]> hashes = null;
	private int MAX_SZ = (500);
	private static final int MAX_BATCHHASH_SIZE = 100;
	boolean corruption = false;
	private long entries = 0;
	private long passEntries = 0;
	private AtomicLong bytesTransmitted = new AtomicLong(0);
	private AtomicLong virtualBytesTransmitted = new AtomicLong(0);
	private String server = null;
	private String path = null;
	private transient String password = null;
	private int port = 2222;
	long startTime = 0;
	long endTime = 0;
	BlockImportEvent levt = null;
	private boolean closed = false;
	boolean firstrun = true;
	private boolean useSSL;
	private Exception lastException;
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	private transient ThreadPoolExecutor executor = new ThreadPoolExecutor(1,
			4, 1, TimeUnit.MINUTES, worksQueue, executionHandler);

	protected MetaFileImport(String path, String server, String password,
			int port, int maxSz, SDFSEvent evt, boolean useSSL)
			throws IOException {
		SDFSLogger.getLog().info(
				"Starting MetaFile FDISK. Max entries per batch are " + MAX_SZ
						+ " use ssl " + useSSL);
		levt = SDFSEvent.metaImportEvent(
				"Starting MetaFile FDISK. Max entries per batch are " + MAX_SZ,
				evt);
		// this.useSSL = useSSL;
		if (maxSz > 0)
			MAX_SZ = (maxSz * 1024 * 1024) / Main.CHUNK_LENGTH;
		hashes = new ArrayList<byte[]>();
		startTime = System.currentTimeMillis();
		File f = new File(path);
		SDFSLogger.getLog().info("getting file counts for  " + f.getPath());
		levt.maxCt = FileCounts.getDBFileSize(f, false);
		SDFSLogger.getLog().info("got file counts");
		this.server = server;
		this.password = password;
		this.port = port;
		this.path = path;
		this.useSSL = useSSL;

	}

	public void close() {
		this.closed = true;
	}

	public void runImport() throws IOException, ReplicationCanceledException,
			InterruptedException {
		int i = 1;
		SDFSLogger.getLog().info(
				"Running Import of " + path + " total runs=" + i);
		this.traverse(new File(this.path));
		executor.shutdown();
		try {
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				SDFSLogger.getLog().debug(
						"Awaiting fdisk completion of threads.");
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		if (hashes.size() != 0) {
			try {
				long sz = ProcessBatchGetBlocks.runCmd(hashes, server, port,
						password, useSSL);
				this.bytesTransmitted.addAndGet(sz);
				levt.bytesImported = this.bytesTransmitted.get();
			} catch (Throwable e) {
				SDFSLogger.getLog().error("Corruption Suspected on import", e);
				corruption = true;
			}
		}
		this.firstrun = false;
		while (passEntries > 0) {
			i++;
			SDFSLogger.getLog().info(
					"Running Import of " + path + " total runs=" + i + " entries imported=" +passEntries);
			passEntries = 0;
			this.traverse(new File(this.path));
			
			executor.shutdown();
			try {
				while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
					SDFSLogger.getLog().debug(
							"Awaiting fdisk completion of threads.");
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			if (hashes.size() != 0) {
				try {
					long sz = ProcessBatchGetBlocks.runCmd(hashes, server, port,
							password, useSSL);
					this.bytesTransmitted.addAndGet(sz);
					levt.bytesImported = this.bytesTransmitted.get();
				} catch (Throwable e) {
					SDFSLogger.getLog().error("Corruption Suspected on import", e);
					corruption = true;
				}
			}

		}
		// Wait for everything to finish.
		while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
			levt.shortMsg = "Awaiting finalization";
		}
		
		endTime = System.currentTimeMillis();
		levt.endEvent("took [" + (System.currentTimeMillis() - startTime)
				/ 1000 + "] seconds to import [" + filesProcessed
				+ "] files and [" + entries + "] blocks.");
		SDFSLogger.getLog().info(
				"took [" + (System.currentTimeMillis() - startTime) / 1000
						+ "] seconds to import [" + filesProcessed
						+ "] files and [" + entries + "] blocks.");
	}

	private void traverse(File dir) throws IOException,
			ReplicationCanceledException {
		if (this.closed)
			throw new ReplicationCanceledException("MetaFile Import Canceled");
		if (Files.isSymbolicLink(dir.toPath())) {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("File is symlink");
		} else if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				traverse(new File(dir, children[i]));
			}
		} else {

			this.checkDedupFile(dir);
		}
	}

	public boolean isCorrupt() {
		return this.corruption;
	}

	private boolean batchCheck(ArrayList<HashLocPair> chunks,
			MetaDataDedupFile mf) throws IOException {
		List<HashLocPair> pchunks = HCServiceProxy.batchHashExists(chunks);
		if (pchunks.size() != chunks.size()) {
			SDFSLogger.getLog().warn(
					"requested " + chunks.size() + " but received "
							+ pchunks.size());
		}
		corruption = false;
		for (HashLocPair p : chunks) {
			byte[] eb = p.hashloc;
			boolean exists = false;
			if (eb[0] == 1)
				exists = true;
			mf.getIOMonitor().addVirtualBytesWritten(Main.CHUNK_LENGTH, true);
			if (!exists) {
				hashes.add(p.hash);
				entries++;
				levt.blocksImported = entries;
				mf.getIOMonitor()
						.addActualBytesWritten(Main.CHUNK_LENGTH, true);
			} else {
				if (HashFunctionPool.max_hash_cluster == 1)
					mf.getIOMonitor().addDulicateData(Main.CHUNK_LENGTH, true);
			}
			if (hashes.size() >= MAX_SZ) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug(
								"fetching " + hashes.size() + " blocks");
					ProcessBatchGetBlocks.runCmd(hashes, server, port,
							password, useSSL);
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug(
								"fetched " + hashes.size() + " blocks");
					this.bytesTransmitted
							.addAndGet((hashes.size() * Main.CHUNK_LENGTH));
					levt.bytesImported = this.bytesTransmitted.get();
					hashes = null;
					hashes = new ArrayList<byte[]>();
				} catch (Throwable e) {
					SDFSLogger.getLog().error("Corruption Suspected on import",
							e);
					corruption = true;
				}
			}
		}
		return corruption;
	}

	private void checkDedupFile(File metaFile) throws IOException,
			ReplicationCanceledException {
		if (this.closed)
			throw new ReplicationCanceledException("MetaFile Import Canceled");
		MetaDataDedupFile mf = MetaDataDedupFile.getFile(metaFile.getPath());
		ArrayList<HashLocPair> bh = new ArrayList<HashLocPair>(
				MAX_BATCHHASH_SIZE);
		mf.getIOMonitor().clearFileCounters(true);
		String dfGuid = mf.getDfGuid();
		if (dfGuid != null) {
			LongByteArrayMap mp = LongByteArrayMap.getMap(dfGuid);
			try {
				SparseDataChunk ck = new SparseDataChunk();
				long prevpos = 0;
				mp.iterInit();
				while (ck != null) {
					if (this.closed)
						throw new ReplicationCanceledException(
								"MetaFile Import Canceled");
					if (this.lastException != null)
						throw this.lastException;
					levt.curCt += (mp.getIterPos() - prevpos);
					prevpos = mp.getIterPos();
					ck  = mp.nextValue(Main.refCount);
					if (ck != null) {
						ck.setFpos((prevpos / mp.getFree().length)
								* Main.CHUNK_LENGTH);
						List<HashLocPair> al = ck.getFingers();

						if (Main.chunkStoreLocal) {
							mf.getIOMonitor().addVirtualBytesWritten(
									Main.CHUNK_LENGTH, true);
							// Todo : Must fix how this is counted
							if (HashFunctionPool.max_hash_cluster > 1)
								mf.getIOMonitor().addDulicateData(
										Main.CHUNK_LENGTH, true);
							boolean hpc = false;
							for (HashLocPair p : al) {
								long pos = 0 ;
								if(Main.refCount && Arrays.areEqual(WritableCacheBuffer.bk,p.hash))
										pos= 0;
								pos = HCServiceProxy.hashExists(p.hash,
										false);
								boolean exists = false;
								if (pos != -1) {
									p.hashloc = Longs.toByteArray(pos);
									hpc = true;
									exists = true;
								}
								if (!exists) {
									hashes.add(p.hash);
									entries++;
									passEntries++;
									levt.blocksImported = entries;
								} else {
									if (HashFunctionPool.max_hash_cluster == 1)
										mf.getIOMonitor().addDulicateData(
												Main.CHUNK_LENGTH, true);
								}
								if (hashes.size() >= MAX_SZ) {
									executor.execute(new DataImporter(this,
											hashes));
									hashes = new ArrayList<byte[]>();
								}
							}
							if (hpc) {
								mp.put(ck.getFpos(), ck);
							}
						} else {
							bh.addAll(ck.getFingers());
							if (bh.size() >= MAX_BATCHHASH_SIZE) {
								boolean cp = batchCheck(bh, mf);
								if (cp)
									corruption = true;
								bh = new ArrayList<HashLocPair>(
										MAX_BATCHHASH_SIZE);
							}

						}
					}
				}
				if (bh.size() > 0) {
					boolean cp = batchCheck(bh, mf);
					if (cp)
						corruption = true;
				}
				Main.volume.updateCurrentSize(mf.length(), true);
				if (corruption) {
					MetaFileStore.removeMetaFile(mf.getPath(), true);
					throw new IOException(
							"Unable to continue MetaFile Import because there are too many missing blocks");
				}
				mf.setDirty(true);
				mf.sync();
				mf.getDedupFile(false).forceRemoteSync();
				Main.volume.addFile();
			} catch (Throwable e) {
				SDFSLogger.getLog()
						.warn("error while checking file [" + dfGuid
								+ "]", e);
				levt.endEvent("error while checking file [" + dfGuid
						+ "]", SDFSEvent.WARN, e);
				throw new IOException(e);
			} finally {
				
				mp.close();
				mp = null;
				if(this.firstrun){
				this.virtualBytesTransmitted.addAndGet(mf.length());
				levt.virtualDataImported = this.virtualBytesTransmitted.get();
				}
			}
		}
		this.filesProcessed++;
		if(this.firstrun)
		levt.filesImported = this.filesProcessed;
	}

	public long getFilesProcessed() {
		return filesProcessed;
	}

	public int getMAX_SZ() {
		return MAX_SZ;
	}

	public boolean isCorruption() {
		return corruption;
	}

	public long getEntries() {
		return entries;
	}

	public long getBytesTransmitted() {
		return bytesTransmitted.get();
	}

	public long getVirtualBytesTransmitted() {
		return virtualBytesTransmitted.get();
	}

	public String getServer() {
		return server;
	}

	public int getPort() {
		return port;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	private static class DataImporter implements Runnable {
		MetaFileImport mfi;
		ArrayList<byte[]> hashes;

		DataImporter(MetaFileImport mfi, ArrayList<byte[]> hashes) {
			this.mfi = mfi;
			this.hashes = hashes;
		}

		@Override
		public void run() {
			try {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"thread fetching " + hashes.size() + " blocks");
				int tries = 0;
				for (;;) {
					try {

						long sz = ProcessBatchGetBlocks.runCmd(hashes,
								mfi.server, mfi.port, mfi.password, mfi.useSSL);
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug(
									"fetched " + hashes.size() + " blocks");
						Main.volume.addDuplicateBytes(-1 * sz, true);
						mfi.bytesTransmitted.addAndGet(sz);
						mfi.levt.bytesImported = mfi.bytesTransmitted.get();
						hashes = null;

						break;
					} catch (Exception e) {
						tries++;
						if (tries > 3)
							throw e;
					}

				}
			} catch (Throwable e) {
				SDFSLogger.getLog().error("Corruption Suspected on import", e);
			}
		}

	}
}