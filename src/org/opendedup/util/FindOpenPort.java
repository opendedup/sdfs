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
package org.opendedup.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class FindOpenPort {

	static FileChannel fileChannel;
	static String portfilepath;

	@SuppressWarnings("resource")
	public void lock() throws IOException, InterruptedException {
		if (OSValidator.isUnix()) {
			if (Main.sdfsBasePath.equals("")) {
				portfilepath = "/var/log/sdfs/" + "port.lock";
			} else {
				portfilepath = OSValidator.getProgramBasePath() + "/var/log/sdfs/" + "port.lock";
			}
		}
		if (OSValidator.isWindows()) {
			String fn = "port.lock";
			portfilepath = OSValidator.getProgramBasePath() + File.separator + "logs" + File.separator + fn;
			File lf = new File(portfilepath);
			lf.getParentFile().mkdirs();
		}

		SDFSLogger.getLog().info("Lock FindOpenPort");
		File file = new File(portfilepath);
		fileChannel = new FileOutputStream(file, true).getChannel();

		while (fileChannel.lock() == null) { // the process checks if the channel is free to write to the file
			Thread.sleep(5000);
		}
	}

	public void unlock() throws IOException, InterruptedException {
		SDFSLogger.getLog().info("Unlock FindOpenPort");
		Thread.sleep(3000);
		fileChannel.close();
		File file = new File(portfilepath);
		file.delete();
		SDFSLogger.getLog().info("Unlock Done");
	}

	public static int pickFreePort(int start, int maxRange)

	{
		int range = 0;
		int port = -1;
		while (port == -1) {
			ServerSocket socket = null;

			try

			{

				socket = new ServerSocket(start, 50, InetAddress.getByAddress(new byte[] { 0x00, 0x00, 0x00, 0x00 }));

				port = socket.getLocalPort();
				if (maxRange > 0 && range == maxRange) {
					SDFSLogger.getLog().info("PickFreePort - Out of Range Specified");
					return 0;
				}

			}

			catch (IOException e)

			{

			}

			finally

			{

				if (socket != null)

				{

					try

					{

						socket.close();

					}

					catch (IOException e)

					{

					}

				}

			}
			start++;
			if (maxRange > 0) {
				range++;
			}
		}
		SDFSLogger.getLog().info("Free Port Identified: - " + port);

		return port;

	}

	public static void main(String[] args) {
		System.out.println(pickFreePort(6442, -1));
	}

}
