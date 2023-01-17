package org.opendedup.sdfs.mgmt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.mgmt.grpc.IOServer;
import org.opendedup.util.FindOpenPort;
import org.opendedup.util.KeyGenerator;

public class MgmtWebServer {
	private static IOServer grpcServer = null;

	public static void start(boolean useSSL) throws Exception {

		if (Main.sdfsCliEnabled) {

			if (useSSL && Main.authJarFilePath.equals("") && Main.authClassInfo.equals("")) {
				String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
				String key = keydir + File.separator + "tls_key.pem";
				if (!new File(key).exists() || !IOServer.keyFileExists()) {
					KeyGenerator.generateKey(new File(key));
				}
			}
			FindOpenPort port = new FindOpenPort();

			port.lock();
			try {

				int[] portRangeInfo = readPortRange();
				if (portRangeInfo != null && portRangeInfo.length == 2) {
					SDFSLogger.getLog().info("Port RangeSpecified");
					SDFSLogger.getLog()
							.info("Starting Port: " + portRangeInfo[0] + " Range Specified: " + portRangeInfo[1]);
					Main.sdfsCliPort = FindOpenPort.pickFreePort(portRangeInfo[0], portRangeInfo[1]);
				} else {
					SDFSLogger.getLog().info("Port Range not Specified");
					Main.sdfsCliPort = FindOpenPort.pickFreePort(Main.sdfsCliPort, 0);
				}

				if (portRangeInfo != null && Main.sdfsCliPort == 0) {
					SDFSLogger.getLog().info("No port Available in range Specified. Program Exiting with Code -2");
					throw new Exception("No port Available in range Specified");
				}

				grpcServer = new IOServer();
				grpcServer.start(useSSL, Main.sdfsCliRequireMutualTLSAuth, Main.sdfsCliListenAddr, Main.sdfsCliPort);
				Main.volume.startReplClients();
			} catch (InterruptedException | IOException e) {
				SDFSLogger.getLog().error("unable to lock port",e);
				throw new Exception(e);
			} finally {
				port.unlock();
			}

		}
	}

	public static void stop() {
		try {
			grpcServer.stop();
		} catch (Throwable e) {
			SDFSLogger.getLog().error("unable to stop Web Management Server", e);
		}
	}

	public static int[] readPortRange() throws IOException {
		if (Main.prodConfigFilePath.equals("")) {
			SDFSLogger.getLog().info("Port Range Not Specified");
			return null;
		}

		String strfilePath = Main.prodConfigFilePath;
		if ((new File(strfilePath).exists())) {
			BufferedReader bufRdr = new BufferedReader(
					new InputStreamReader(new FileInputStream(strfilePath), "ISO-8859-1"));
			try {
				// Read File Line By Line
				for (String line; (line = bufRdr.readLine()) != null;) {
					line = line.replaceAll("[^a-zA-Z0-9#=-]", "");
					if (!line.startsWith("#")) {
						String[] arrOfStr = line.split("=", 2);
						String strPort = arrOfStr[1].toString().strip();
						if (strPort != null && !strPort.isBlank() && !strPort.isEmpty()) {
							arrOfStr = strPort.split("-", 2);
							int minPort = Integer.parseInt(arrOfStr[0].toString());
							int maxPort = Integer.parseInt(arrOfStr[1].toString());
							if (minPort > maxPort)
								return null;
							else {
								int portRange = (maxPort - minPort) + 1;
								Main.sdfsCliPort = minPort;
								return new int[] { minPort, portRange };
							}
						}
					}
				}
				// Close the input stream
				bufRdr.close();
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally {
				bufRdr.close();
			}
			return null;
		}
		return null;
	}

	public static String executeLinuxCmd(String cmd) {
		ProcessBuilder processBuilder = new ProcessBuilder();
		// -- Linux --
		// Run a shell command
		processBuilder.command("bash", "-c", cmd);

		StringBuilder output = new StringBuilder();
		try {

			Process process = processBuilder.start();

			BufferedReader reader = new BufferedReader(
					new InputStreamReader(process.getInputStream()));
			String line;
			while ((line = reader.readLine()) != null) {
				output.append(line + " ");
			}
			process.waitFor();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return output.toString();
	}

}
