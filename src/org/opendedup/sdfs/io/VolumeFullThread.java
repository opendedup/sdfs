package org.opendedup.sdfs.io;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class VolumeFullThread implements Runnable {
	private final Volume vol;
	private Thread th = null;
	private long duration = 15 * 1000;
	boolean closed = false;

	public VolumeFullThread(Volume vol) {
		this.vol = vol;
		th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		while (!closed) {

			try {
				Thread.sleep(duration);
				vol.setVolumeFull(this.isFull());
			} catch (Exception e) {
				SDFSLogger.getLog().error("Unable to check if full.", e);
				this.closed = true;
			}
		}

	}
	private long offset = Main.CHUNK_LENGTH*1024*10;
	public synchronized boolean isFull() throws Exception {
		long avail = vol.pathF.getUsableSpace();
		if (avail < (offset)) {
			SDFSLogger.getLog().warn(
					"Drive is almost full space left is [" + avail + "]");
			return true;
		}
		if (vol.fullPercentage < 0 || vol.currentSize.get() == 0)
			return false;
		else if ((vol.getCurrentSize()+offset) >= vol.getCapacity()) {
			SDFSLogger.getLog().warn(
					"Drive is almost full. Current Size [" + vol.getCurrentSize() + "] and capacity is [" + vol.getCapacity() + "]");
			return true;
		}
		else if((HCServiceProxy.getDSESize() + offset) >= HCServiceProxy.getDSEMaxSize()) {
			SDFSLogger.getLog().warn(
					"Drive is almost full. DSE Size [" + HCServiceProxy.getDSESize() + "] and DSE Max Size is [" + HCServiceProxy.getDSEMaxSize() + "]");
			return true;
		}
		else if((HCServiceProxy.getSize() + 10000) >= HCServiceProxy.getMaxSize()) {
			SDFSLogger.getLog().warn(
					"Drive is almost full. DSE HashMap Size [" + HCServiceProxy.getSize() + "] and DSE HashMap Max Size is [" + HCServiceProxy.getMaxSize() + "]");
			return true;
		}
		else
			return false;
	}

	public void stop() {
		th.interrupt();
		this.closed = true;
	}

}
