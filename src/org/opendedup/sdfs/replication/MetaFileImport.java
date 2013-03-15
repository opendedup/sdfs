package org.opendedup.sdfs.replication;

import java.io.File;
import java.io.Serializable;


import java.io.IOException;
import java.util.ArrayList;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.notification.BlockImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;
import org.opendedup.util.StringUtils;

public class MetaFileImport implements Serializable{
	private static final long serialVersionUID = 2281680761909041919L;
	private long filesProcessed = 0;
	private transient ArrayList<String> hashes = null;
	private int MAX_SZ = ((30*1024*1024)/Main.CHUNK_LENGTH);
	boolean corruption = false;
	private long entries = 0;
	private long bytesTransmitted;
	private long virtualBytesTransmitted;
	private String server = null;
	private String path = null;
	private transient String password= null;
	private int port = 2222;
	private boolean useSSL;
	long startTime = 0;
	long endTime = 0;
	BlockImportEvent levt = null;
	private boolean closed = false;
	

	protected MetaFileImport(String path,String server,String password,int port,int maxSz,SDFSEvent evt,boolean useSSL) throws IOException {
		SDFSLogger.getLog().info("Starting MetaFile FDISK. Max entries per batch are " + MAX_SZ);
		levt = SDFSEvent.metaImportEvent("Starting MetaFile FDISK. Max entries per batch are " + MAX_SZ,evt);
		this.useSSL = useSSL;
		if(maxSz >0)
			MAX_SZ = (maxSz * 1024*1024)/Main.CHUNK_LENGTH;
		hashes = new ArrayList<String>();
		startTime = System.currentTimeMillis();
		File f = new File(path);
		levt.maxCt = FileCounts.getDBFileSize(f, false);
		this.server = server;
		this.password = password;
		this.port = port;
		this.path = path;
		
	}
	
	public void close() {
		this.closed = true;
	}
	
	public void runImport() throws IOException, ReplicationCanceledException {
		SDFSLogger.getLog().info("Running Import of " + path);
		this.traverse(new File(this.path));
		if(hashes.size() != 0) {
			try {
				HCServiceProxy.fetchChunks(hashes,server,password,port,useSSL);
				this.bytesTransmitted = this.bytesTransmitted + (hashes.size() * Main.CHUNK_LENGTH);
				levt.bytesImported = this.bytesTransmitted;
			} catch(Exception e) {
				SDFSLogger.getLog().error("Corruption Suspected on import",e);
				corruption = true;
			}
		}
		endTime = System.currentTimeMillis();
		levt.endEvent("took [" + (System.currentTimeMillis() - startTime) / 1000
						+ "] seconds to import [" + filesProcessed + "] files and [" + entries + "] blocks.");
		SDFSLogger.getLog().info(
				"took [" + (System.currentTimeMillis() - startTime) / 1000
						+ "] seconds to import [" + filesProcessed + "] files and [" + entries + "] blocks.");
	}

	

	private void traverse(File dir) throws IOException, ReplicationCanceledException {
		if(this.closed)
			throw new ReplicationCanceledException("MetaFile Import Canceled");
		if (dir.isDirectory()) {
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

	private void checkDedupFile(File metaFile) throws IOException, ReplicationCanceledException {
		if(this.closed)
			throw new ReplicationCanceledException("MetaFile Import Canceled");
		MetaDataDedupFile mf = MetaDataDedupFile.getFile(metaFile.getPath());
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
				long prevpos =  0;
				mp.iterInit();
				while (val != null) {
					if(this.closed)
						throw new ReplicationCanceledException("MetaFile Import Canceled");
					levt.curCt += (mp.getIterFPos()-prevpos);
					prevpos = mp.getIterFPos();
					val = mp.nextValue();
					if (val != null) {
						SparseDataChunk ck = new SparseDataChunk(val);
						if (!ck.isLocalData()) {
							boolean exists = HCServiceProxy.localHashExists(ck
									.getHash());
							mf.getIOMonitor().addVirtualBytesWritten(Main.CHUNK_LENGTH, true);
							if (!exists) {
								hashes.add(StringUtils.getHexString(ck.getHash()));
								entries++;
								levt.blocksImported = entries;
								mf.getIOMonitor().addActualBytesWritten(Main.CHUNK_LENGTH, true);
							} else {
								mf.getIOMonitor().addDulicateBlock(true);
							}
							if(hashes.size()>=MAX_SZ) {
								try {
									SDFSLogger.getLog().debug("fetching " + hashes.size() + " blocks");
									HCServiceProxy.fetchChunks(hashes,server,password,port,useSSL);
									SDFSLogger.getLog().debug("fetched " + hashes.size() + " blocks");
									this.bytesTransmitted = this.bytesTransmitted + (hashes.size() * Main.CHUNK_LENGTH);
									levt.bytesImported = this.bytesTransmitted;
									hashes = null;
									hashes = new ArrayList<String>();
								} catch(Exception e) {
									SDFSLogger.getLog().error("Corruption Suspected on import",e);
									corruption = true;
								}
							}
						}
					}
				}
				Main.volume.updateCurrentSize(mf.length(), true);
				if (corruption) {
					MetaFileStore.removeMetaFile(mf.getPath(), true);
						throw new IOException(
								"Unable to continue MetaFile Import because there are too many missing blocks");

				} 
			} catch (Exception e) {
				SDFSLogger.getLog()
						.warn("error while checking file [" + mapFile.getPath()
								+ "]", e);
				levt.endEvent("error while checking file [" + mapFile.getPath()
								+ "]",SDFSEvent.WARN, e);
				throw new IOException(e);
			} finally {
				mp.close();
				mp = null;
				this.virtualBytesTransmitted = this.virtualBytesTransmitted + mf.length();
				levt.virtualDataImported = this.virtualBytesTransmitted;
				
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
		return bytesTransmitted;
	}



	public long getVirtualBytesTransmitted() {
		return virtualBytesTransmitted;
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
