package org.opendedup.sdfs.servers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.util.ArrayList;



import org.opendedup.collections.HashtableFullException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.DSEClientSocket;
import org.opendedup.sdfs.cluster.cmds.ClaimHashesCmd;
import org.opendedup.sdfs.cluster.cmds.FetchChunkCmd;
import org.opendedup.sdfs.cluster.cmds.HashExistsCmd;
import org.opendedup.sdfs.cluster.cmds.RemoveChunksCmd;
import org.opendedup.sdfs.cluster.cmds.WriteHashCmd;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.notification.SDFSEvent;

public class HCServiceProxy {


	private static HashChunkServiceInterface hcService = null;
	private static DSEClientSocket socket = null;

	// private static boolean initialized = false;

	static {
		try {
			if(Main.chunkStoreLocal) {
				SDFSLogger.getLog().info("Starting local chunkstore");
				hcService = new HashChunkService();
				hcService.init();
				File file = new File(Main.hashDBStore + File.separator + ".lock");
				if (!Main.closedGracefully || file.exists()) {
					hcService.runConsistancyCheck();
				} 
				touchRunFile();
			}
			
			else {
				SDFSLogger.getLog().info("Starting clustered Volume with id=" + Main.DSEClusterID + " config="+ Main.DSEClusterConfig);
				socket = new DSEClientSocket(Main.DSEClusterConfig,Main.DSEClusterID);
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to initialize HashChunkService ",
					e);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void processHashClaims(SDFSEvent evt) throws IOException {
		if(Main.chunkStoreLocal)
			hcService.processHashClaims(evt);
		else {
			new ClaimHashesCmd(evt).executeCmd(socket);
			
		}
	}

	public static boolean hashExists(byte[] hash, short hops)
			throws IOException, HashtableFullException {
		return hcService.hashExists(hash, hops);
	}

	public static HashChunk fetchHashChunk(byte[] hash) throws IOException {
		return hcService.fetchChunk(hash);
	}

	public static long removeStailHashes(long ms, boolean forceRun,
			SDFSEvent evt) throws IOException {
		if(Main.chunkStoreLocal)
			return hcService.removeStailHashes(ms, forceRun, evt);
		else {
			RemoveChunksCmd cmd = new RemoveChunksCmd(ms,forceRun,evt);
			cmd.executeCmd(socket);
			return cmd.removedHashesCount();
		}
			
	}

	public static long getChunksFetched() {
		return -1;
	}

	public static synchronized void init() {
	}

	public static long getSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.hcService.getSize();
		} else {
			return socket.getCurrentSize();
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

	public static byte [] writeChunk(byte[] hash, byte[] aContents,
			int position, int len, boolean sendChunk) throws IOException,
			HashtableFullException {
		boolean doop = false;
		byte [] b = new byte[8];
		if (Main.chunkStoreLocal) {
			// doop = HCServiceProxy.hcService.hashExists(hash);
			if (!doop && sendChunk) {
				doop = HCServiceProxy.hcService.writeChunk(hash, aContents, 0,
						Main.CHUNK_LENGTH, false);
			}
		} else {
			try {
				WriteHashCmd cmd = new WriteHashCmd(hash, aContents, aContents.length,
						false, Main.volume.getClusterCopies());
				cmd.executeCmd(socket);
				return cmd.reponse();
			} catch (Exception e1) {
				SDFSLogger.getLog().fatal("Unable to write chunk " + hash, e1);
				throw new IOException("Unable to write chunk " + hash);
			} finally {
				
			}
		}if(doop)
			b[0] = 1;
			
		return b;
	}

	public static boolean localHashExists(byte[] hash) throws IOException,
			HashtableFullException {
		boolean exists = false;
		if (Main.chunkStoreLocal) {
			exists = HCServiceProxy.hcService.localHashExists(hash);

		}
		return exists;
	}

	public static void fetchChunks(ArrayList<String> hashes, String server,
			String password, int port, boolean useSSL) throws IOException,
			HashtableFullException {
		if (Main.chunkStoreLocal) {
			HCServiceProxy.hcService.remoteFetchChunks(hashes, server,
					password, port, useSSL);
		} else {
			throw new IllegalStateException(
					"not implemented for remote chunkstores");
		}
	}

	public static byte [] hashExists(byte[] hash) throws IOException,
			HashtableFullException {
		byte [] exists = new byte[8];
		if (Main.chunkStoreLocal) {
			if(HCServiceProxy.hcService.hashExists(hash, (short) 0))
				return exists;
			else {
				exists[0] = -1;
				return exists;
			}
				
		} else {
			HashExistsCmd cmd = new HashExistsCmd(hash,false);
			cmd.executeCmd(socket);
			return cmd.getResponse();
		}
	}

	

	public static byte[] fetchChunk(byte[] hash,byte [] hashloc) throws IOException {
		if (Main.chunkStoreLocal) {
			HashChunk hc = HCServiceProxy.hcService.fetchChunk(hash);
			return hc.getData();
		} else {
			FetchChunkCmd cmd = new FetchChunkCmd(hash,hashloc);
			cmd.executeCmd(socket);
			return cmd.getChunk();
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
	
	private static void touchRunFile()
	{
		File file = new File(Main.hashDBStore + File.separator + ".lock");
	    try
	    {
	    	
	        if (!file.exists())
	            new FileOutputStream(file).close();
	        file.setLastModified(System.currentTimeMillis());
	        SDFSLogger.getLog().warn("Write lock file " + file.getPath());
	    }
	    catch (IOException e)
	    {
	    	SDFSLogger.getLog().warn("unable to create lock file " + file.getPath(),e);
	    }
	}

}
