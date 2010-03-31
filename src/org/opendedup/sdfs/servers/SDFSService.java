package org.opendedup.sdfs.servers;

import java.util.logging.Logger;

import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.FileChunkStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.gc.SDFSGCScheduler;

public class SDFSService {
	String configFile;
	String routingFile;
	private static Logger log = Logger.getLogger("sdfs");
	private SDFSGCScheduler gc = null;

	public SDFSService(String configFile, String routingFile) {

		this.configFile = configFile;
		this.routingFile = routingFile;
		log.info("Running SDFS Version " + Main.version);
		log.info("reading config file = " + this.configFile);
		log.info("reading routing file = " + this.routingFile);
	}

	public void start() throws Exception {
		Config.parseSDFSConfigFile(this.configFile);
		Config.parserRoutingFile(this.routingFile);
		if (Main.chunkStoreLocal)
			HashChunkService.init();
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
		System.out.println("SDFS is Shut Down");
	}

}
