package org.opendedup.sdfs.servers;



import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.FileChunkStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.gc.SDFSGCScheduler;
import org.opendedup.sdfs.mgmt.MgmtWebServer;
import org.opendedup.sdfs.network.NetworkHCServer;
import org.opendedup.util.SDFSLogger;

public class SDFSService {
	String configFile;
	
	private SDFSGCScheduler gc = null;

	public SDFSService(String configFile) {

		this.configFile = configFile;
		SDFSLogger.getLog().info("Running SDFS Version " + Main.version);
		SDFSLogger.getLog().info("reading config file = " + this.configFile);
	}

	public void start() throws Exception {
		Config.parseSDFSConfigFile(this.configFile);
		if (Main.chunkStoreLocal) {
			if(Main.enableNetworkChunkStore) {
				NetworkHCServer.init();
			} else {
				HashChunkService.init();
			}
		}
			MgmtWebServer.start();
		gc = new SDFSGCScheduler();
	}

	public void stop() {
		System.out.println("Shutting Down SDFS");
		System.out.println("Stopping FDISK scheduler");
		gc.stopSchedules();
		System.out.println("Flushing and Closing Write Caches");
		DedupFileStore.close();
		System.out.println("Write Caches Flushed and Closed");
		System.out.println("Committing open Files");
		MetaFileStore.close();
		System.out.println("Open File Committed");
		System.out.println("Writing Config File");
		try {
			Config.writeSDFSConfigFile(configFile);
		} catch (Exception e) {

		}
		if (Main.chunkStoreLocal) {
			System.out.println("Shutting down ChunkStore");
			FileChunkStore.closeAll();
			System.out.println("Shutting down HashStore");
			HashChunkService.close();
		}
		MgmtWebServer.stop();
		System.out.println("SDFS is Shut Down");
		try {
			Process p = Runtime.getRuntime().exec("umount " +Main.volumeMountPoint);
			p.waitFor();
		} catch (Exception e) {
		}
	}

}
