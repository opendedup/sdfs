package org.opendedup.sdfs.servers;




import java.util.ArrayList;

import org.opendedup.logging.SDFSLogger;

import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.gc.StandAloneGCScheduler;
import org.opendedup.sdfs.mgmt.MgmtWebServer;
import org.opendedup.sdfs.network.NetworkDSEServer;
import org.opendedup.sdfs.notification.SDFSEvent;

public class SDFSService {
	String configFile;

	private NetworkDSEServer ndServer = null;
	private ArrayList<String> volumes;

	public SDFSService(String configFile,ArrayList<String> volumes) {

		this.configFile = configFile;
		this.volumes = volumes;
		System.out.println("Running SDFS Version " + Main.version);

		System.out.println("reading config file = " + this.configFile);
	}

	public void start(boolean useSSL) throws Exception {
		Config.parseSDFSConfigFile(this.configFile);
		SDFSLogger.getLog().debug("############# SDFSService Starting ##################");
		MgmtWebServer.start(useSSL);
		Main.mountEvent = SDFSEvent.mountEvent("SDFS Version [" + Main.version
				+ "] Mounting Volume from " + this.configFile);
		try {
			if (Main.volume.getName() == null)
				Main.volume.setName(configFile);
			Main.volume.setClosedGracefully(false);
			Config.writeSDFSConfigFile(configFile);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to write volume config.", e);
		}
		SDFSLogger.getLog().debug("HCServiceProxy Starting");
		HCServiceProxy.init(volumes);
		SDFSLogger.getLog().debug("HCServiceProxy Started");
		if (Main.chunkStoreLocal) {
			try {
				
				if (Main.enableNetworkChunkStore && !Main.runCompact) {
					ndServer = new NetworkDSEServer();
					new Thread(ndServer).start();
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("Unable to initialize volume ", e);
				System.err.println("Unable to initialize Hash Chunk Service");
				e.printStackTrace();
				System.exit(-1);
			}
			if (Main.runCompact) {
				this.stop();
				System.exit(0);
			}

			Main.pFullSched= new StandAloneGCScheduler();
		}

		
		Main.mountEvent.endEvent("Volume Mounted");
		SDFSLogger.getLog().debug("############### SDFSService Started ##################");
	}

	public void stop() {
		SDFSEvent evt = SDFSEvent.umountEvent("Unmounting Volume");
		SDFSLogger.getLog().info("Shutting Down SDFS");
		SDFSLogger.getLog().info("Stopping FDISK scheduler");
		
			try {
				Main.pFullSched.close();
				Main.pFullSched = null;
			}catch(Exception e) {}
		SDFSLogger.getLog().info("Flushing and Closing Write Caches");
		DedupFileStore.close();
		SDFSLogger.getLog().info("Write Caches Flushed and Closed");
		SDFSLogger.getLog().info("Committing open Files");
		MetaFileStore.close();
		SDFSLogger.getLog().info("Open File Committed");
		SDFSLogger.getLog().info("Writing Config File");

		/*
		 * try { MD5CudaHash.freeMem(); } catch (Exception e) { }
		 */
		MgmtWebServer.stop();
		try {
			Process p = Runtime.getRuntime().exec(
					"umount " + Main.volumeMountPoint);
			p.waitFor();
		} catch (Exception e) {
		}
		if (Main.chunkStoreLocal) {
			SDFSLogger.getLog().info(
					"######### Shutting down HashStore ###################");
			HCServiceProxy.close();
			if (Main.enableNetworkChunkStore && !Main.runCompact) {
				ndServer.close();
			} else {

			}
		}
		try {
			Main.volume.setClosedGracefully(true);
			Config.writeSDFSConfigFile(configFile);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to write volume config.", e);
		}
		evt.endEvent("Volume Unmounted");
		SDFSLogger.getLog().info("SDFS is Shut Down");
	}
}
