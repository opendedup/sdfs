package org.opendedup.sdfs.network;

import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.FileChunkStore;
import org.opendedup.sdfs.servers.HashChunkService;



public class NetworkHCServer {

	// Declaration section:
	// declare a server socket and a client socket for the server
	// declare an input and an output stream

	static Socket clientSocket = null;
	static ServerSocket serverSocket = null;
	private static Logger log = Logger.getLogger("sdfs");

	// This chat server can accept up to 10 clients' connections

	public static void main(String args[]) {

		// The default port

		if (args.length < 1) {
			System.out.println("Usage: NetworkHCServer <configFile>");
		} else {
			ShutdownHook shutdownHook = new ShutdownHook();
	        Runtime.getRuntime().addShutdownHook(shutdownHook);
			Config.parseHubStoreConfigFile(args[0]);
		// Initialization section:
		// Try to open a server socket on port port_number (default 2222)
		// Note that we can't choose a port less than 1023 if we are not
		// privileged users (root)

		try {
			InetSocketAddress addr = new InetSocketAddress(Main.serverHostName,Main.serverPort);
			if(Main.useUDP) {
				NioUDPServer udpServer = new NioUDPServer();
			}
			serverSocket = new ServerSocket();
			serverSocket.bind(addr);
			log.info("listening on " + addr.toString());
			//HashFunctions.insertRecorts(8000000);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
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
				System.out.println(e);
			}
		}
		}
	}
	
	
}

class ShutdownHook extends Thread {
    public void run() {
        System.out.println("###################### Shutting down StorageHub #################################");
        System.out.println("###################### Shutting down ChunkStore #################################");
        FileChunkStore.closeAll();
        System.out.println("###################### Shutting down HashStore #################################");
        HashChunkService.close();
    }
}
