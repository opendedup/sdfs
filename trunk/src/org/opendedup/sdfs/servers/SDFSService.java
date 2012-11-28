package org.opendedup.sdfs.servers;

import java.io.File;

import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.gc.SDFSGCScheduler;
import org.opendedup.sdfs.filestore.gc.StandAloneGCScheduler;
import org.opendedup.sdfs.io.VolumeConfigWriterThread;
import org.opendedup.sdfs.mgmt.MgmtWebServer;
import org.opendedup.sdfs.network.NetworkDSEServer;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.OSValidator;
import org.opendedup.util.SDFSLogger;

public class SDFSService {
	String configFile;

	private SDFSGCScheduler gc = null;
	private StandAloneGCScheduler stGC = null;
	private String routingFile;
	private NetworkDSEServer ndServer = null;

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
		MgmtWebServer.start();
		Main.mountEvent = SDFSEvent.mountEvent("SDFS Version [" + Main.version
				+ "] Mounting Volume from " + this.configFile);
		if (this.routingFile != null)
			Config.parserRoutingFile(routingFile);
		else if (!Main.chunkStoreLocal) {
			Config.parserRoutingFile(OSValidator.getConfigPath()
					+ File.separator + "routing-config.xml");
		}
		try {
			if (Main.volume.getName() == null)
				Main.volume.setName(configFile);
			Main.volume.setClosedGracefully(false);
			Config.writeSDFSConfigFile(configFile);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to write volume config.", e);
		}
		Main.wth = new VolumeConfigWriterThread(configFile);
		if (Main.chunkStoreLocal) {
			try {
				HashChunkService.init();
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

			this.stGC = new StandAloneGCScheduler();
		}

		if (!Main.chunkStoreLocal) {
			gc = new SDFSGCScheduler();
		}
		Main.mountEvent.endEvent("Volume Mounted");
	}

	public void stop() {
		SDFSEvent evt = SDFSEvent.umountEvent("Unmounting Volume");
		SDFSLogger.getLog().info("Shutting Down SDFS");
		SDFSLogger.getLog().info("Stopping FDISK scheduler");
		if (!Main.chunkStoreLocal) {
			gc.stopSchedules();
		} else {
			try {
			this.stGC.close();
			}catch(Exception e) {}
		}
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
			Main.wth.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			Process p = Runtime.getRuntime().exec(
					"umount " + Main.volumeMountPoint);
			p.waitFor();
		} catch (Exception e) {
		}
		if (Main.chunkStoreLocal) {
			SDFSLogger.getLog().info(
					"######### Shutting down HashStore ###################");
			HashChunkService.close();
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
