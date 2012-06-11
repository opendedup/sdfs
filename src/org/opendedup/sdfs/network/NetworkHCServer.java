package org.opendedup.sdfs.network;

import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.opendedup.util.SDFSLogger;

import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HashChunkService;

public class NetworkHCServer {

	// Declaration section:
	// declare a server socket and a client socket for the server
	// declare an input and an output stream

	static Socket clientSocket = null;
	static ServerSocket serverSocket = null;

	private static NioUDPServer udpServer = null;

	// This chat server can accept up to 10 clients' connections

	public static void main(String args[]) {
		// The default port

		if (args.length < 1) {
			System.out.println("Usage: NetworkHCServer <configFile>");
		} else {
			ShutdownHook shutdownHook = new ShutdownHook();
			Runtime.getRuntime().addShutdownHook(shutdownHook);

			try {
				Config.parseDSEConfigFile(args[0]);
			} catch (IOException e1) {
				SDFSLogger.getLog().fatal(
						"exiting because of an error with the config file");
				System.exit(-1);
			}
			init();

		}

	}

	public static void init() {

		HashChunkService.init();
		// Initialization section:
		// Try to open a server socket on port port_number (default 2222)
		// Note that we can't choose a port less than 1023 if we are not
		// privileged users (root)
		try {
			InetSocketAddress addr = new InetSocketAddress(Main.serverHostName,
					Main.serverPort);
			if (Main.useUDP) {
				udpServer = new NioUDPServer();
			}
			serverSocket = new ServerSocket();
			serverSocket.bind(addr);
			SDFSLogger.getLog().info("listening on " + addr.toString());
		} catch (Exception e) {
			e.printStackTrace();
			SDFSLogger.getLog().fatal("unable to open network ports", e);
			System.exit(-1);
		}

		// Create a socket object from the ServerSocket to listen and accept
		// connections.
		// Open input and output streams for this socket will be created in
		// client's thread since every client is served by the server in
		// an individual thread

		while (true) {
			try {
				clientSocket = serverSocket.accept();
				clientSocket.setKeepAlive(true);
				clientSocket.setTcpNoDelay(true);
				new ClientThread(clientSocket).start();
			} catch (IOException e) {
				if (!serverSocket.isClosed())
					SDFSLogger.getLog().error(
							"Unable to open port " + e.toString(), e);
			}
		}
	}

	public static void close() {
		try {
			System.out.println("#### Shutting Down Network Service ####");

			serverSocket.close();
		} catch (Exception e) {
		}
		try {

			udpServer.close();
		} catch (Exception e) {
		}
		System.out.println("#### Shutting down HashStore ####");
		HashChunkService.close();
		System.out.println("#### Shut down completed ####");
	}
}

class ShutdownHook extends Thread {
	public void run() {
		System.out.println("#### Shutting down StorageHub ####");

		NetworkHCServer.close();
	}
}