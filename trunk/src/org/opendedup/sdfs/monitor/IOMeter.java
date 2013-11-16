package org.opendedup.sdfs.monitor;

import java.io.BufferedOutputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import org.opendedup.sdfs.servers.HCServiceProxy;

public class IOMeter implements Runnable {

	private long sleeptime = 15;
	String fileName;
	private boolean stopped = false;
	DecimalFormat df = new DecimalFormat("###.##");
	DecimalFormat dfp = new DecimalFormat("##.##%");

	public IOMeter(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public void run() {
		BufferedOutputStream out = null;
		double lastMBRead = 0;
		double lastMBWrite = 0;
		try {
			out = new BufferedOutputStream(new FileOutputStream(fileName));
			String column = "reading (MB/s),writing (MB/s),duplicates found,chunks written,dedup rate\r\n";
			out.write(column.getBytes());
			out.flush();
			while (!stopped) {
				try {
					Thread.sleep(sleeptime * 1000);
					double difMBRead = (HCServiceProxy.getKBytesRead() - lastMBRead) / 1024;
					double difMBWrite = (HCServiceProxy.getKBytesWrite() - lastMBWrite) / 1024;
					double dedupRate = (HCServiceProxy.getDupsFound() / ((double) HCServiceProxy
							.getDupsFound() + (double) HCServiceProxy
							.getChunksWritten()));
					lastMBWrite = HCServiceProxy.getKBytesWrite();
					lastMBRead = HCServiceProxy.getKBytesRead();
					String csvStr = df.format((difMBRead) / sleeptime) + ","
							+ df.format((difMBWrite) / sleeptime) + ","
							+ HCServiceProxy.getDupsFound() + ","
							+ HCServiceProxy.getChunksWritten() + ","
							+ dfp.format(dedupRate) + "\r\n";
					out.write(csvStr.getBytes());
					out.flush();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
			} catch (IOException e) {
			}
		}

	}

	public void stop() {
		this.stopped = true;
	}

}
