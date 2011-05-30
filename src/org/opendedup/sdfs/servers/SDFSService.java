package org.opendedup.sdfs.servers;

import java.io.File;

import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.gc.SDFSGCScheduler;
import org.opendedup.sdfs.filestore.gc.StandAloneGCScheduler;
import org.opendedup.sdfs.mgmt.MgmtWebServer;
import org.opendedup.sdfs.network.NetworkHCServer;
import org.opendedup.util.OSValidator;
import org.opendedup.util.SDFSLogger;

public class SDFSService {
	String configFile;

	private SDFSGCScheduler gc = null;
	private StandAloneGCScheduler stGC = null;
	private String routingFile;

	public SDFSService(String configFile, String routingFile) {

		this.configFile = configFile;
		this.routingFile = routingFile;
		System.out.println("Running SDFS Version " + Main.version);
		if (routingFile != null)
			SDFSLogger.getLog().info(
					"reading routing config file = " + this.routingFile);
		System.out.println("reading config file = " + this.configFile);
	}

	public void start() throws Exception {
		Config.parseSDFSConfigFile(this.configFile);
		if (this.routingFile != null)
			Config.parserRoutingFile(routingFile);
		else if (!Main.chunkStoreLocal) {
			Config.parserRoutingFile(OSValidator.getConfigPath()
					+ File.separator + "routing-config.xml");
		}
		if (Main.chunkStoreLocal) {
			if (Main.enableNetworkChunkStore) {
				NetworkHCServer.init();
			} else {
				HashChunkService.init();
			}
			this.stGC = new StandAloneGCScheduler();
		}
		MgmtWebServer.start();
		if (!Main.chunkStoreLocal) {
			gc = new SDFSGCScheduler();
		}
	}

	public void stop() {
		SDFSLogger.getLog().info("Shutting Down SDFS");
		SDFSLogger.getLog().info("Stopping FDISK scheduler");
		if (!Main.chunkStoreLocal) {
			gc.stopSchedules();
		} else {
			this.stGC.close();
		}
		SDFSLogger.getLog().info("Flushing and Closing Write Caches");
		DedupFileStore.close();
		SDFSLogger.getLog().info("Write Caches Flushed and Closed");
		SDFSLogger.getLog().info("Committing open Files");
		MetaFileStore.close();
		SDFSLogger.getLog().info("Open File Committed");
		SDFSLogger.getLog().info("Writing Config File");
		try {
			Config.writeSDFSConfigFile(configFile);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to write volume config.", e);
		}
		
		/*
		 * try { MD5CudaHash.freeMem(); } catch (Exception e) { }
		 */
		MgmtWebServer.stop();
		SDFSLogger.getLog().info("SDFS is Shut Down");
		try {
			Process p = Runtime.getRuntime().exec(
					"umount " + Main.volumeMountPoint);
			p.waitFor();
		} catch (Exception e) {
		}
		if (Main.chunkStoreLocal) {
			SDFSLogger.getLog().info("######### Shutting down HashStore ###################");
			HashChunkService.close();
		}
	}

}
