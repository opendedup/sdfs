package org.opendedup.sdfs.io;

import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

public class VolumeConfigWriterThread implements Runnable {
	private String configFile = null;
	private Thread th = null;
	private long duration = 15 * 1000;
	boolean closed = false;

	public VolumeConfigWriterThread(String configFile) {
		this.configFile = configFile;
		th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		while (!closed) {

			try {
				Thread.sleep(duration);
				writeConfig();
			} catch (Exception e) {
				SDFSLogger.getLog().debug("Unable to write volume config.", e);
				this.closed = true;
			}
		}

	}
	
	public synchronized void writeConfig() throws Exception {
		Main.volume.setClosedGracefully(false);
		Config.writeSDFSConfigFile(configFile);
	}

	public void stop() {
		th.interrupt();
		this.closed = true;
	}
	
	public String getConfigFilePath() {
		return this.configFile;
	}

}
