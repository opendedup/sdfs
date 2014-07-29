package org.opendedup.sdfs.io;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.events.VolumeWritten;

import com.google.common.eventbus.EventBus;

public class VolumeConfigWriterThread implements Runnable {
	public  String configFile = null;
	private Thread th = null;
	private long duration = 60 * 1000;
	boolean closed = false;
	private static EventBus eventBus = new EventBus();
	
	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

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
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("Unable to write volume config.",
							e);
				this.closed = true;
			}
		}

	}

	public synchronized void writeConfig() throws Exception {
		File bkf = new File(configFile + ".back");
		if (bkf.exists())
			bkf.delete();
		Path bak = new File(configFile + ".back").toPath();
		Path src = new File(configFile).toPath();

		Files.copy(src, bak);
		Main.volume.setClosedGracefully(false);
		Config.writeSDFSConfigFile(configFile);
		eventBus.post(new VolumeWritten(Main.volume));
	}

	public void stop() {
		th.interrupt();
		this.closed = true;
	}

	public String getConfigFilePath() {
		return this.configFile;
	}

}
