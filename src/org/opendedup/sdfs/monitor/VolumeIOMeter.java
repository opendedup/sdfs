package org.opendedup.sdfs.monitor;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.RollingFileAppender;
import org.opendedup.logging.JSONVolPerfLayout;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.slf4j.MDC;

import com.sun.management.UnixOperatingSystemMXBean;

public class VolumeIOMeter implements Runnable {

	private Volume vol;
	private long bytesRead = 0, bytesWritten = 0, virtualBytesWritten = 0,
			RIOPS = 0, WIOPS = 0, duplicateBytes = 0,dseSz=0,dseCompSz=0;
	private double pbytesRead = 0, pbytesWritten = 0, pvirtualBytesWritten = 0,
			pRIOPS = 0, pWIOPS = 0, pduplicateBytes = 0;
	private Logger log = Logger.getLogger("volperflog");
	private boolean closed = false;
	Thread th = null;
	UnixOperatingSystemMXBean perf = (UnixOperatingSystemMXBean) ManagementFactory
			.getOperatingSystemMXBean();

	public VolumeIOMeter(Volume vol) {
		RollingFileAppender app = null;
		try {
			app = new RollingFileAppender(new JSONVolPerfLayout(),
					vol.getPerfMonFile(), true);
			app.setMaxBackupIndex(2);
			app.setMaxFileSize("10MB");
		} catch (IOException e) {
			log.debug("unable to change appender", e);
		}
		this.vol = vol;
		log.addAppender(app);
		log.setLevel(Level.INFO);
		th = new Thread(this);
		th.start();
	}

	public void run() {
		while (!closed) {
			try {
				Thread.sleep(15 * 1000);

				this.calPerf();
			} catch (Exception e) {
				SDFSLogger.getLog().warn(
						"Exception in " + this.getClass().getName(), e);
				this.closed = true;
			}
		}
	}

	private void calPerf() {
		this.bytesRead = (long) (vol.getReadBytes() - this.pbytesRead);
		this.pbytesRead = vol.getReadBytes();
		MDC.put("bytesRead", Long.toString(bytesRead));
		this.bytesWritten = (long) (vol.getActualWriteBytes() - this.pbytesWritten);
		this.pbytesWritten = vol.getActualWriteBytes();
		MDC.put("bytesWritten", Long.toString(this.bytesWritten));
		this.duplicateBytes = (long) (vol.getDuplicateBytes() - this.pduplicateBytes);
		this.pduplicateBytes = vol.getDuplicateBytes();
		MDC.put("duplicateBytes", Long.toString(this.duplicateBytes));
		this.virtualBytesWritten = (long) (vol.getVirtualBytesWritten() - this.pvirtualBytesWritten);
		this.pvirtualBytesWritten = vol.getVirtualBytesWritten();
		MDC.put("virtualBytesWritten", Long.toString(this.virtualBytesWritten));
		this.RIOPS = (long) (vol.getReadOperations() - this.pRIOPS);
		this.pRIOPS = vol.getReadOperations();
		MDC.put("RIOPS", Long.toString(this.RIOPS));
		this.WIOPS = (long) (vol.getWriteOperations() - this.pWIOPS);
		this.pWIOPS = vol.getWriteOperations();
		this.dseSz = HCServiceProxy.getDSESize();
		this.dseCompSz = HCServiceProxy.getDSECompressedSize();
		MDC.put("dseSz", Long.toString(this.dseSz));
		MDC.put("dseCompSz", Long.toString(this.dseCompSz));
		MDC.put("WIOPS", Long.toString(this.WIOPS));
		MDC.put("sdfsCpuLoad", Double.toString(perf.getProcessCpuLoad()));
		MDC.put("sdfsCpuTime", Double.toString(perf.getProcessCpuTime()));
		MDC.put("systemCpuLoad", Double.toString(perf.getSystemCpuLoad()));
		MDC.put("systemCpuAverage", Double.toString(perf.getSystemLoadAverage()));
		MDC.put("freeMemory", Long.toString(perf.getFreePhysicalMemorySize()));
		MDC.put("totalMemory", Long.toString(perf.getTotalPhysicalMemorySize()));
		MDC.put("freeSwap", Long.toString(perf.getFreeSwapSpaceSize()));
		MDC.put("totalSwap", Long.toString(perf.getTotalSwapSpaceSize()));
		log.info(vol.getName());
		MDC.clear();
	}

	public void close() {
		this.closed = true;
		th.interrupt();

	}

}
