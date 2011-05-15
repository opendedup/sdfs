package org.opendedup.sdfs.monitor;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import org.opendedup.sdfs.servers.HashChunkService;

public class IOMeter implements Runnable {

	private long sleeptime = 15;
	String fileName;
	private boolean stopped = false;
	DecimalFormat df = new DecimalFormat("###.##");
	DecimalFormat dfp = new DecimalFormat("##.##%");

	public IOMeter(String fileName) {
		this.fileName = fileName;
	}

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
					double difMBRead = (HashChunkService.getKBytesRead() - lastMBRead) / 1024;
					double difMBWrite = (HashChunkService.getKBytesWrite() - lastMBWrite) / 1024;
					double dedupRate = ((double) HashChunkService
							.getDupsFound() / ((double) HashChunkService
							.getDupsFound() + (double) HashChunkService
							.getChunksWritten()));
					lastMBWrite = HashChunkService.getKBytesWrite();
					lastMBRead = HashChunkService.getKBytesRead();
					String csvStr = df.format((difMBRead) / sleeptime) + ","
							+ df.format((difMBWrite) / sleeptime) + ","
							+ HashChunkService.getDupsFound() + ","
							+ HashChunkService.getChunksWritten() + ","
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void stop() {
		this.stopped = true;
	}

}
