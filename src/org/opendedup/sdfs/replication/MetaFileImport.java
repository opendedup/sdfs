package org.opendedup.sdfs.replication;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.mgmt.cli.ProcessBatchGetBlocks;
import org.opendedup.sdfs.notification.BlockImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;

public class MetaFileImport implements Serializable {
	private static final long serialVersionUID = 2281680761909041919L;
	private long filesProcessed = 0;
	private transient ArrayList<byte[]> hashes = null;
	private int MAX_SZ = ((30 * 1024 * 1024) / Main.CHUNK_LENGTH);
	private static final int MAX_BATCHHASH_SIZE = 100;
	boolean corruption = false;
	private long entries = 0;
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
	private boolean useSSL;

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
		levt.maxCt = FileCounts.getDBFileSize(f, false);
		this.server = server;
		this.password = password;
		this.port = port;
		this.path = path;
		this.useSSL = useSSL;

	}

	public void close() {
		this.closed = true;
	}

	public void runImport() throws IOException, ReplicationCanceledException {
		SDFSLogger.getLog().info("Running Import of " + path);
		this.traverse(new File(this.path));
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
		boolean corruption = false;
			for(HashLocPair p : chunks) {
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
			File mapFile = new File(Main.dedupDBStore + File.separator
					+ dfGuid.substring(0, 2) + File.separator + dfGuid
					+ File.separator + dfGuid + ".map");
			if (!mapFile.exists()) {
				return;
			}
			LongByteArrayMap mp = new LongByteArrayMap(mapFile.getPath());
			try {
				byte[] val = new byte[0];
				long prevpos = 0;
				mp.iterInit();
				while (val != null) {
					if (this.closed)
						throw new ReplicationCanceledException(
								"MetaFile Import Canceled");
					levt.curCt += (mp.getIterPos() - prevpos);
					prevpos = mp.getIterPos();
					val = mp.nextValue();
					if (val != null) {
						SparseDataChunk ck = new SparseDataChunk(val,mp.getVersion());
						List<HashLocPair> al = ck.getFingers();

						if (Main.chunkStoreLocal) {
							mf.getIOMonitor().addVirtualBytesWritten(
									Main.CHUNK_LENGTH, true);
							// Todo : Must fix how this is counted
							if (HashFunctionPool.max_hash_cluster > 1)
								mf.getIOMonitor().addDulicateData(
										Main.CHUNK_LENGTH, true);
							for (HashLocPair p : al) {
								byte[] eb = HCServiceProxy.hashExists(p.hash,
										false);
								boolean exists = false;
								if (eb[0] == 1)
									exists = true;
								if (!exists) {
									hashes.add(p.hash);
									entries++;
									levt.blocksImported = entries;
								} else {
									if (HashFunctionPool.max_hash_cluster == 1)
										mf.getIOMonitor().addDulicateData(
												Main.CHUNK_LENGTH, true);
								}
								if (hashes.size() >= MAX_SZ) {
									try {
										if (SDFSLogger.isDebug())
											SDFSLogger.getLog().debug(
													"fetching " + hashes.size()
															+ " blocks");
										int tries = 0;
										for (;;) {
											try {

												long sz = ProcessBatchGetBlocks
														.runCmd(hashes, server,
																port, password,
																useSSL);
												if (SDFSLogger.isDebug())
													SDFSLogger
															.getLog()
															.debug("fetched "
																	+ hashes.size()
																	+ " blocks");
												Main.volume.addDuplicateBytes(
														-1 * sz, true);
												this.bytesTransmitted
														.addAndGet(sz);
												levt.bytesImported = this.bytesTransmitted
														.get();
												hashes = null;
												hashes = new ArrayList<byte[]>();
												break;
											} catch (Exception e) {
												tries++;
												if (tries > 3)
													throw e;
											}

										}
									} catch (Throwable e) {
										SDFSLogger
												.getLog()
												.error("Corruption Suspected on import",
														e);
										corruption = true;
									}
								}
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
			} catch (Throwable e) {
				SDFSLogger.getLog()
						.warn("error while checking file [" + mapFile.getPath()
								+ "]", e);
				levt.endEvent("error while checking file [" + mapFile.getPath()
						+ "]", SDFSEvent.WARN, e);
				throw new IOException(e);
			} finally {
				mp.close();
				mp = null;
				this.virtualBytesTransmitted.addAndGet(mf.length());
				levt.virtualDataImported = this.virtualBytesTransmitted.get();
			}
		}
		this.filesProcessed++;
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
}
