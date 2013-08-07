package org.opendedup.sdfs.network;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.DSEServerSocket;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class ClusteredHCServer {

	// Declaration section:
	// declare a server socket and a client socket for the server
	// declare an input and an output stream

	static DSEServerSocket socket = null;

	// This chat server can accept up to 10 clients' connections

	public static Options buildOptions() {
		Options options = new Options();
		options.addOption("d", false, "debug output");
		options.addOption(
				"c",
				true,
				"sdfs cluster configuration file to start this storage node \ne.g. /etc/sdfs/cluster-dse-cfg.xml");
		options.addOption(
				"rv",
				true,
				"comma separated list of remote volumes that should also be accounted for when doing garbage collection. "
						+ "If not entered the volume will attempt to identify other volumes in the cluster.");
		options.addOption("h", false, "displays available options");
		return options;
	}

	public static void main(String args[]) throws IOException, ParseException {
		checkJavaVersion();
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		ArrayList<String> volumes = new ArrayList<String>();
		if (cmd.hasOption("h")) {
			printHelp(options);
			System.exit(1);
		} else {
			if (!cmd.hasOption("c"))
				printHelp(options);
			else {
				ClusteredShutdownHook shutdownHook = new ClusteredShutdownHook();
				Runtime.getRuntime().addShutdownHook(shutdownHook);
				Main.standAloneDSE = true;
				Main.chunkStoreLocal = true;
				boolean debug = cmd.hasOption("d");
				if (debug) {
					SDFSLogger.setLevel(0);
				}
				if (cmd.hasOption("rv")) {
					StringTokenizer st = new StringTokenizer(
							cmd.getOptionValue("rv"), ",");
					while (st.hasMoreTokens()) {
						volumes.add(st.nextToken());
					}
				}
				try {
					Config.parseDSEConfigFile(cmd.getOptionValue("c"));
				} catch (IOException e1) {
					SDFSLogger.getLog().fatal(
							"exiting because of an error with the config file");
					System.exit(-1);
				}
				try {
					init(volumes);
				} catch (Exception e) {

					e.printStackTrace();
					SDFSLogger.getLog()
							.fatal("unable to start cluster node", e);
					System.exit(-1);
				}
			}
		}

	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter
				.printHelp("Usage: ClusteredHCServer -c <configFile>", options);
	}

	private static void checkJavaVersion() {
		Properties sProp = java.lang.System.getProperties();
		String sVersion = sProp.getProperty("java.version");
		sVersion = sVersion.substring(0, 3);
		Float f = Float.valueOf(sVersion);
		if (f.floatValue() < (float) 1.7) {
			System.out.println("Java version must be 1.7 or newer");
			System.out
					.println("To get Java 7 go to https://jdk7.dev.java.net/");
			System.exit(-1);
		}
	}

	public static void init(ArrayList<String> volumes) throws Exception {
		HCServiceProxy.init(volumes);
		// Initialization section:
		// Try to open a server socket on port port_number (default 2222)
		// Note that we can't choose a port less than 1023 if we are not
		// privileged users (root)

		socket = new DSEServerSocket(Main.DSEClusterConfig, Main.DSEClusterID,
				Main.DSEClusterMemberID,volumes);
		HCServiceProxy.cs = socket;
	}

	public static void close() {
		try {
			System.out.println("#### Shutting Down Network Service ####");
		} catch (Exception e) {
		}
		System.out.println("#### Shutting down HashStore ####");
		HCServiceProxy.close();
		System.out.println("#### Shut down completed ####");
	}
}

class ClusteredShutdownHook extends Thread {
	@Override
	public void run() {
		System.out.println("#### Shutting down StorageHub ####");

		ClusteredHCServer.close();
	}
}