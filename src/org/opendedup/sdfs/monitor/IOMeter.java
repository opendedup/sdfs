/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.monitor;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

import org.opendedup.sdfs.servers.HCServiceProxy;

public class IOMeter implements Runnable {

	private long sleeptime = 15;
	String fileName;
	private boolean stopped = false;

	private static DecimalFormat df = (DecimalFormat) NumberFormat
			.getNumberInstance(Locale.US);
	private static DecimalFormat dfp = (DecimalFormat) NumberFormat
			.getNumberInstance(Locale.US);

	static {
		df.applyPattern("###.##");
		dfp.applyPattern("##.##%");
	}

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
