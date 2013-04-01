package org.opendedup.sdfs.network;

import java.io.IOException;

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

	public static void main(String args[]) throws IOException {
		// The default port

		if (args.length < 1) {
			System.out.println("Usage: ClusteredHCServer <configFile> <debug true|false>");
		} else {
			
			ClusteredShutdownHook shutdownHook = new ClusteredShutdownHook();
			Runtime.getRuntime().addShutdownHook(shutdownHook);
			Main.standAloneDSE = true;
			Main.chunkStoreLocal = true;
			if(args.length >1) {
				boolean debug = Boolean.parseBoolean(args[1]);
				System.out.println("args length = " + debug);
				if(debug) {
					System.out.println("!!!!!!!!!!!!!!!! Debug is set");
					SDFSLogger.setLevel(0);
				}
			}
				
			try {
				Config.parseDSEConfigFile(args[0]);
			} catch (IOException e1) {
				SDFSLogger.getLog().fatal(
						"exiting because of an error with the config file");
				System.exit(-1);
			}
			try {
				init();
			}catch(Exception e) {
				
				e.printStackTrace();
				SDFSLogger.getLog().fatal("unable to start cluster node",e);
				System.exit(-1);
			}

		}

	}

	public static void init() throws Exception {
		HCServiceProxy.init();
		// Initialization section:
		// Try to open a server socket on port port_number (default 2222)
		// Note that we can't choose a port less than 1023 if we are not
		// privileged users (root)
		
		socket = new DSEServerSocket(Main.DSEClusterConfig,Main.DSEClusterID,Main.DSEClusterMemberID);
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