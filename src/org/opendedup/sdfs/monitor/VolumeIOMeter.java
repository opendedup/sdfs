package org.opendedup.sdfs.monitor;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.RollingFileAppender;
import org.opendedup.logging.JSONVolPerfLayout;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.io.Volume;
import org.slf4j.MDC;


public class VolumeIOMeter implements Runnable{
	
	private Volume vol;
	private long bytesRead,bytesWritten,virtualBytesWritten,RIOPS,WIOPS,duplicateBytes;
	private Logger log = Logger.getLogger("volperflog");
	private boolean closed = false;
	Thread th = null;
	
	public VolumeIOMeter(Volume vol) {
		RollingFileAppender app = null;
		try {
			app = new RollingFileAppender(new JSONVolPerfLayout(), vol.getPerfMonFile(), true);
			app.setMaxBackupIndex(2);
			app.setMaxFileSize("10MB");
		} catch (IOException e) {
			log.debug("unable to change appender", e);
		}
		log.addAppender(app);
		log.setLevel(Level.INFO);
		th = new Thread(this);
		th.start();
	}
	
	public void run() {
		while(!closed) {
			try {
				Thread.sleep(15*1000);
			
			this.calPerf();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("Exception in " + this.getClass().getName(),e);
				this.closed = true;
			}
		}
	}
	
	private void calPerf () {
		this.bytesRead =(long)(vol.getReadBytes() - this.bytesRead);
		MDC.put("bytesRead", Long.toString(bytesRead));
		this.bytesWritten = (long)(vol.getActualWriteBytes() - this.bytesWritten);
		MDC.put("bytesWritten",Long.toString(this.bytesWritten));
		this.duplicateBytes = (long)(vol.getDuplicateBytes() - this.duplicateBytes);
		MDC.put("duplicateBytes", Long.toString(this.duplicateBytes));
		this.virtualBytesWritten = (long)(vol.getVirtualBytesWritten() - this.virtualBytesWritten);
		MDC.put("virtualBytesWritten", Long.toString(this.virtualBytesWritten));
		this.RIOPS = (long)(vol.getReadOperations() - this.RIOPS);
		MDC.put("RIOPS", Long.toString(this.RIOPS));
		this.WIOPS = (long)(vol.getWriteOperations() - this.WIOPS);
		MDC.put("WIOPS", Long.toString(this.WIOPS));
		log.info(vol.getName());
		MDC.clear();
	}
	
	public void close() {
		this.closed = true;
		th.interrupt();
		
	}
	

}
