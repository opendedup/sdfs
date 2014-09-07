package org.opendedup.mtools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opendedup.collections.DataMapInterface;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;
import org.opendedup.util.StringUtils;

public class ClusterRedundancyCheck {
	private long files = 0;
	private long corruptFiles = 0;
	private long newRendundantBlocks = 0;
	private long failedRendundantBlocks = 0;
	SDFSEvent fEvt = null;
	private static final int MAX_BATCH_SIZE = 200;
	private boolean metaTree = false;

	public ClusterRedundancyCheck(SDFSEvent fEvt, File f, boolean metaTree)
			throws IOException {
		this.metaTree = metaTree;
		init(fEvt, f);
	}

	private void init(SDFSEvent fEvt, File f) throws IOException {
		this.fEvt = fEvt;
		if (!f.exists()) {
			fEvt.endEvent("Cluster Redundancy Check Will not start because the volume has not been written too");
			throw new IOException(
					"Cluster Redundancy Check Will not start because the volume has not been written too");
		}
		if (Main.chunkStoreLocal) {
			fEvt.endEvent("Cluster Redundancy Check Will not start because the volume storage is local");
			throw new IOException(
					"Cluster Redundancy Check Will not start because the volume storage is local");
		}
		fEvt.shortMsg = "Cluster Redundancy for " + Main.volume.getName()
				+ " file count = " + FileCounts.getCount(f, false)
				+ " file size = " + FileCounts.getSize(f, false)
				+ " file-path=" + f.getPath();
		fEvt.maxCt = FileCounts.getSize(f, false);
		SDFSLogger.getLog().info(
				"Starting Cluster Redundancy Check on " + f.getPath());
		long start = System.currentTimeMillis();

		try {
			this.traverse(f);
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check [" + files + "]. Found ["
							+ this.corruptFiles + "] corrupt files. Made ["
							+ this.newRendundantBlocks
							+ "] blocks redundant. Failed to make ["
							+ this.failedRendundantBlocks
							+ "] blocks redundant for path [" + f.getPath()
							+ "].");

			fEvt.endEvent("took [" + (System.currentTimeMillis() - start)
					/ 1000 + "] seconds to check [" + files + "]. Found ["
					+ this.corruptFiles + "] corrupt files. Made ["
					+ this.newRendundantBlocks
					+ "] blocks redundant. Failed to make ["
					+ this.failedRendundantBlocks
					+ "] blocks redundant for path [" + f.getPath() + "].");
		} catch (Exception e) {
			SDFSLogger.getLog().info("cluster redundancy failed", e);
			fEvt.endEvent("cluster redundancy failed because [" + e.toString()
					+ "]", SDFSEvent.ERROR);
			throw new IOException(e);
		}
	}

	public ClusterRedundancyCheck(SDFSEvent fEvt) throws IOException {
		// this.fEvt = fEvt;
		File f = new File(Main.dedupDBStore);
		init(fEvt, f);
	}

	private void traverse(File dir) throws IOException {
		if (dir.isDirectory()) {
			try {
				String[] children = dir.list();
				for (int i = 0; i < children.length; i++) {
					traverse(new File(dir, children[i]));
				}
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error traversing " + dir.getPath(),
						e);
			}
		} else if (metaTree) {
			MetaDataDedupFile mf = MetaDataDedupFile.getFile(dir.getPath());
			mf.getIOMonitor().clearFileCounters(true);
			String dfGuid = mf.getDfGuid();
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"checking " + dir.getPath() + " with guid" + dfGuid);
			if (dfGuid != null) {
				File mapFile = new File(Main.dedupDBStore + File.separator
						+ dfGuid.substring(0, 2) + File.separator + dfGuid
						+ File.separator + dfGuid + ".map");
				if (!mapFile.exists()) {
					return;
				}
				this.checkDedupFile(mapFile);
			}
		} else {
			if (dir.getPath().endsWith(".map")) {
				this.checkDedupFile(dir);
			}
		}
	}

	private int batchCheck(ArrayList<SparseDataChunk> chunks,
			DataMapInterface mp) throws IOException, HashtableFullException {
		List<SparseDataChunk> pchunks = HCServiceProxy.batchHashExists(chunks);
		int corruptBlocks = 0;
		for (SparseDataChunk ck : pchunks) {
			byte[] exists = ck.getHashLoc();
			if (exists[0] == -1) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							" could not find "
									+ StringUtils.getHexString(ck.getHash()));
				corruptBlocks++;
			} else {
				byte[] currenthl = ck.getHashLoc();
				exists[0] = currenthl[0];
				try {
					int ncopies = 0;
					for (int i = 1; i < 8; i++) {
						if (exists[i] > (byte) 0) {
							ncopies++;
						}
					}
					if (ncopies < Main.volume.getClusterCopies()) {
						byte[] nb = HCServiceProxy.fetchChunk(ck.getHash(),
								exists);
						exists = HCServiceProxy.writeChunk(ck.getHash(), nb, 0,
								nb.length, true, exists);
						ncopies = 0;
						for (int i = 1; i < 8; i++) {
							if (exists[i] > (byte) 0) {
								ncopies++;
							}
						}
						if (ncopies >= Main.volume.getClusterCopies()) {
							this.newRendundantBlocks++;
						} else
							this.failedRendundantBlocks++;

					}
					exists[0] = currenthl[0];

					if (!brequals(currenthl, exists)) {
						ck.setHashLoc(exists);
					}
					mp.put(ck.getFpos(), ck.getBytes());
				} catch (IOException e) {
					this.failedRendundantBlocks++;
				}

			}
		}
		return corruptBlocks;
	}

	private void checkDedupFile(File mapFile) throws IOException {
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Cluster check " + mapFile.getPath());
		LongByteArrayMap mp = new LongByteArrayMap(mapFile.getPath());
		long prevpos = 0;
		try {
			ArrayList<SparseDataChunk> chunks = new ArrayList<SparseDataChunk>(
					MAX_BATCH_SIZE);
			byte[] val = new byte[0];
			mp.iterInit();
			long corruptBlocks = 0;
			while (val != null) {
				fEvt.curCt += (mp.getIterPos() - prevpos);
				prevpos = mp.getIterPos();
				val = mp.nextValue();
				if (val != null) {
					SparseDataChunk ck = new SparseDataChunk(val,mp.getVersion());
					ck.setFpos((prevpos / mp.getFree().length)
							* Main.CHUNK_LENGTH);
					if (!ck.isLocalData()) {
						if (Main.chunkStoreLocal) {
							byte[] exists = HCServiceProxy.hashExists(
									ck.getHash(), true);

							if (exists[0] == -1) {
								if (SDFSLogger.isDebug())
									SDFSLogger
											.getLog()
											.debug("file ["
													+ mapFile
													+ "] could not find "
													+ StringUtils
															.getHexString(ck
																	.getHash()));
								corruptBlocks++;
							} else {
								byte[] currenthl = ck.getHashLoc();
								exists[0] = currenthl[0];
								try {
									int ncopies = 0;
									for (int i = 1; i < 8; i++) {
										if (exists[i] > (byte) 0) {
											ncopies++;
										}
									}
									if (ncopies < Main.volume
											.getClusterCopies()
											&& ncopies < HCServiceProxy.cs
													.getStorageNodes().size()) {
										byte[] nb = HCServiceProxy.fetchChunk(
												ck.getHash(), exists);
										exists = HCServiceProxy.writeChunk(
												ck.getHash(), nb, 0, nb.length,
												true, exists);
										ncopies = 0;
										for (int i = 1; i < 8; i++) {
											if (exists[i] > (byte) 0) {
												ncopies++;
											}
										}
										if (ncopies >= Main.volume
												.getClusterCopies()) {
											this.newRendundantBlocks++;
										} else
											this.failedRendundantBlocks++;

									} else if (ncopies < Main.volume
											.getClusterCopies()
											&& ncopies >= HCServiceProxy.cs
													.getStorageNodes().size()) {
										this.failedRendundantBlocks++;
									}
									exists[0] = currenthl[0];

									if (!brequals(currenthl, exists)) {
										ck.setHashLoc(exists);
									}
									mp.put(ck.getFpos(), ck.getBytes());
								} catch (IOException e) {
									this.failedRendundantBlocks++;
								}

							}

						} else {
							chunks.add(ck);
							if (chunks.size() >= MAX_BATCH_SIZE) {
								corruptBlocks += batchCheck(chunks, mp);
								chunks = new ArrayList<SparseDataChunk>(
										MAX_BATCH_SIZE);
							}
						}
					}
				}
			}

			if (chunks.size() > 0) {
				corruptBlocks += batchCheck(chunks, mp);
			}
			if (corruptBlocks > 0) {
				this.corruptFiles++;
				SDFSLogger.getLog().info(
						"************** map file " + mapFile.getPath()
								+ " is suspect, [" + corruptBlocks
								+ "] missing blocks found.***************");
			}
		} catch (Exception e) {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog()
						.debug("error while checking file ["
								+ mapFile.getPath() + "]", e);
			throw new IOException(e);
		} finally {
			mp.close();
			mp = null;
		}
		this.files++;
	}

	static int count(byte[] nums, byte x) {
		int count = 0;
		for (byte num : nums) {
			if (num == x)
				count++;
		}
		return count;
	}

	static boolean brequals(byte[] arr1, byte[] arr2) {
		if (arr1.length != arr2.length)
			return false;
		for (byte x : arr1) {
			if (count(arr1, x) != count(arr2, x))
				return false;
		}
		return true;
	}

	public static void main(String[] args) {
		byte[] b1 = { (byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4,
				(byte) 5, (byte) 6 };
		byte[] b2 = { (byte) 0, (byte) 1, (byte) 6, (byte) 4, (byte) 3,
				(byte) 5, (byte) 2 };
		System.out.println("array equal =" + brequals(b1, b2));
	}

}
